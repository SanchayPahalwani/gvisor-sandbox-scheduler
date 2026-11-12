// Package nodeagent is the node agent: it runs sandboxes as runsc containers
// on one node (spec section 6).
package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bundle"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Config describes one node. Zero Prime, ReadyTimeout and TTLGrace take
// their defaults.
type Config struct {
	NodeID, Group string
	CPUMillis     int64 // declared capacity
	MemoryBytes   int64
	MaxSandboxes  int
	RootFS        string
	StateDir      string        // bundles live in StateDir/bundles/<runsc-id>
	CheckpointDir string        // "" disables checkpoint/restore; shared by the agents of a host
	WarmPoolSize  int           // 0 disables the pool
	Prime         string        // default bundle.DefaultPrime
	ReadyTimeout  time.Duration // default 30s
	TTLGrace      time.Duration // default 10s: the node reaps at ttl + grace
}

// NetPolicy is implemented by internal/netpolicy. A nil NetPolicy makes any
// request with egress_allow fail with FAILED_PRECONDITION.
type NetPolicy interface {
	Setup(ctx context.Context, id string, allow []string) (netnsPath string, hosts []bundle.HostEntry, err error)
	Teardown(ctx context.Context, id string) error
}

// Deps are the manager's connections to the host.
type Deps struct {
	Runtime runsc.Runtime
	Cgroups Cgroups
	Net     NetPolicy
	Now     func() time.Time // default time.Now
}

const (
	defaultReadyTimeout = 30 * time.Second
	defaultTTLGrace     = 10 * time.Second

	reapInterval     = time.Second
	livenessInterval = 2 * time.Second

	// cleanupTimeout bounds one teardown (runsc delete, network, files, cgroup).
	cleanupTimeout = 30 * time.Second
	// loopLockWait bounds how long a background destroy waits for the
	// lifecycle lock of a sandbox.
	loopLockWait = time.Minute
)

// nodeIDRE is what the bundle accepts as a node ID; it also keeps the
// cgroup path /gss/<node> one level deep.
var nodeIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$`)

// Manager owns the sandboxes of one node: the table of entries, admission
// against the node's capacity, the start and teardown steps, the warm pool
// (pool.go), the TTL reaper and the liveness check.
//
// Locking: mu guards the table, the pool, the image directory, and each
// entry's state, pid and start mode. Each entry also has a lifecycle lock
// (sandbox.op) held for the whole of a Create or a destroy, so those never
// run at once on one entry; mu is never held while waiting for that lock or
// while calling runsc.
type Manager struct {
	cfg Config
	rt  runsc.Runtime // every call is traced as runsc.<method>
	cg  Cgroups
	net NetPolicy
	now func() time.Time

	// Test hooks, set before Start.
	processAlive         func(pid int) bool
	reapEvery, liveEvery time.Duration
	destroyLookedUp      func(e *sandbox)        // called by Destroy before it destroys e
	fsync                func(path string) error // fsyncPath: syncs a file or directory
	restoreReadyTimeout  time.Duration           // the readiness bound after a restore
	poolRetry            time.Duration           // the refill loop's wait after a failed start

	mu            sync.Mutex
	sandboxes     map[string]*sandbox // by sandbox ID
	imageDir      string              // the checkpoint image Creates restore from; "" if none
	imageKey      string              // the key EnsureImage computed; "" before
	imageBad      bool                // the image was dropped after failed restores: none until restart
	readyFailures int                 // restored sandboxes in a row that did not get ready
	imageLooked   time.Time           // the last look for another agent's image (Deps.Now)
	serving       bool                // Start finished; Create is open
	started       bool
	closed        bool

	// The warm pool. Pool sandboxes are not in the table until claimed.
	warm     []*sandbox        // ready to be claimed, oldest first
	fill     *poolFill         // the pool sandbox being started, or nil
	dropping map[*sandbox]bool // pool sandboxes being torn down
	refill   chan struct{}     // wakes the refill loop (buffer of one)

	stop     chan struct{}
	stopOnce sync.Once
	loops    sync.WaitGroup
}

// NewManager checks cfg and returns a manager that serves nothing until Start.
func NewManager(cfg Config, deps Deps) (*Manager, error) {
	switch {
	case deps.Runtime == nil:
		return nil, errors.New("nodeagent: Deps.Runtime is required")
	case deps.Cgroups == nil:
		return nil, errors.New("nodeagent: Deps.Cgroups is required")
	case !nodeIDRE.MatchString(cfg.NodeID):
		return nil, fmt.Errorf("nodeagent: node ID %q must match %s", cfg.NodeID, nodeIDRE)
	case cfg.CPUMillis <= 0 || cfg.MemoryBytes <= 0 || cfg.MaxSandboxes <= 0:
		return nil, fmt.Errorf("nodeagent: capacity must be positive, got %dm, %d bytes, %d sandboxes",
			cfg.CPUMillis, cfg.MemoryBytes, cfg.MaxSandboxes)
	case cfg.WarmPoolSize < 0:
		return nil, fmt.Errorf("nodeagent: WarmPoolSize must not be negative, got %d", cfg.WarmPoolSize)
	case !filepath.IsAbs(cfg.RootFS) || !filepath.IsAbs(cfg.StateDir):
		return nil, fmt.Errorf("nodeagent: RootFS %q and StateDir %q must be absolute paths", cfg.RootFS, cfg.StateDir)
	}
	if cfg.Prime == "" {
		cfg.Prime = bundle.DefaultPrime
	}
	if cfg.ReadyTimeout <= 0 {
		cfg.ReadyTimeout = defaultReadyTimeout
	}
	if cfg.TTLGrace <= 0 {
		cfg.TTLGrace = defaultTTLGrace
	}
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	return &Manager{
		cfg: cfg, rt: tracedRuntime{deps.Runtime}, cg: deps.Cgroups, net: deps.Net, now: now,
		processAlive: pidAlive, fsync: fsyncPath, restoreReadyTimeout: defaultRestoreReadyTimeout,
		reapEvery: reapInterval, liveEvery: livenessInterval, poolRetry: defaultPoolRetry,
		sandboxes: map[string]*sandbox{},
		dropping:  map[*sandbox]bool{},
		refill:    make(chan struct{}, 1),
		stop:      make(chan struct{}),
	}, nil
}

// Start removes what an earlier agent with this node ID left behind, makes
// sure the checkpoint image exists (when CheckpointDir is set), then starts
// the reaper, the liveness loop and (with a WarmPoolSize) the refill loop
// of the warm pool, which run until Close. Create is refused
// until Start has succeeded: the leftover sweep deletes every container under
// the runsc root. A failed image build is logged and does not fail Start:
// the node then serves cold starts.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.started || m.closed {
		m.mu.Unlock()
		return errors.New("nodeagent: Start called twice or after Close")
	}
	m.started = true
	m.mu.Unlock()

	if err := m.removeLeftovers(ctx); err != nil {
		return fmt.Errorf("nodeagent: remove leftovers: %w", err)
	}
	if m.cfg.CheckpointDir != "" {
		if dir, err := m.EnsureImage(ctx); err != nil {
			slog.Warn("no checkpoint image; sandboxes start cold", "node", m.cfg.NodeID, "err", err)
		} else {
			slog.Info("checkpoint image ready", "node", m.cfg.NodeID, "dir", dir)
		}
	}
	m.loops.Add(2)
	go m.loop(m.reapEvery, m.reapExpired)
	go m.loop(m.liveEvery, m.checkLiveness)
	if m.cfg.WarmPoolSize > 0 {
		m.loops.Add(1)
		go m.refillLoop()
	}
	m.mu.Lock()
	m.serving = !m.closed
	m.mu.Unlock()
	return nil
}

// Create starts a sandbox and returns when it is ready, on the path
// chooseMode picks (spec 7.3). The warm path claims a pool sandbox; when
// the pool has emptied by then or the claim fails, AUTO takes the restore
// or cold path instead, and a forced WARM fails. When a restore fails in
// its own steps, in AUTO mode the restore is undone and the cold path runs
// once instead, and a forced RESTORE fails with Internal. A failed runsc
// restore also turns restore off on this node until it restarts, and so do
// three restored sandboxes in a row that do not get ready (restoreFailed).
func (m *Manager) Create(ctx context.Context, req *gssv1.NodeCreateRequest) (resp *gssv1.NodeCreateResponse, err error) {
	begin := time.Now() // ready_latency is a measurement: real time, not Deps.Now
	ctx, span := startSpan(ctx, "node.create",
		attribute.String("sandbox.id", req.GetSandboxId()),
		attribute.Int("sandbox.incarnation", int(req.GetIncarnation())))
	defer func() { endSpan(span, err) }()

	id, inc := req.GetSandboxId(), req.GetIncarnation()
	if err := validateSandboxID(id); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if inc < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "incarnation %d is negative", inc)
	}
	spec := &gssv1.SandboxSpec{}
	if req.GetSpec() != nil {
		spec = proto.Clone(req.GetSpec()).(*gssv1.SandboxSpec)
	}
	gssv1.ApplyDefaults(spec)
	if err := gssv1.ValidateSpec(spec); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	egress := len(spec.GetEgressAllow()) > 0
	imageDir := m.restoreImage()
	mode, err := chooseMode(spec.GetStartMode(), egress, m.WarmReady(), imageDir != "")
	if err != nil {
		return nil, err
	}
	if egress && m.net == nil {
		return nil, status.Error(codes.FailedPrecondition, "egress_allow needs a network policy, and this node has none")
	}
	span.SetAttributes(attribute.String("start.mode", modeName(mode)))

	if mode == gssv1.StartMode_START_MODE_WARM {
		miss, err := m.claim(ctx, id, inc, spec)
		if err == nil {
			return &gssv1.NodeCreateResponse{StartedAs: mode, ReadyLatency: durationpb.New(time.Since(begin))}, nil
		}
		if !miss || spec.GetStartMode() != gssv1.StartMode_START_MODE_AUTO {
			return nil, err
		}
		mode = gssv1.StartMode_START_MODE_COLD
		if imageDir != "" {
			mode = gssv1.StartMode_START_MODE_RESTORE
		}
		span.SetAttributes(attribute.String("start.mode", modeName(mode)))
	}

	e, err := m.reserve(ctx, id, inc, spec, mode)
	if err != nil {
		return nil, err
	}
	defer e.unlock()
	var step string
	if mode == gssv1.StartMode_START_MODE_RESTORE {
		step, err = m.restoreSandbox(ctx, e, imageDir)
		if err != nil && restoreStep(step) && ctx.Err() == nil && spec.GetStartMode() == gssv1.StartMode_START_MODE_AUTO {
			mode = gssv1.StartMode_START_MODE_COLD
			span.SetAttributes(attribute.String("start.mode", modeName(mode)), attribute.Bool("start.fallback", true))
			step, err = m.coldAfterRestore(ctx, e, step, err)
		}
	} else {
		step, err = m.startSandbox(ctx, e, "")
	}
	latency := time.Since(begin)
	if err == nil {
		step, err = "runsc.state", m.recordPID(ctx, e)
	}
	if err != nil {
		m.mu.Lock()
		e.state = stateDestroying
		m.mu.Unlock()
		if rerr := m.remove(ctx, e); rerr != nil {
			return nil, status.Errorf(codes.Internal, "create %s: %s: %v (cleanup failed: %v)", e.ref.ID, step, err, rerr)
		}
		return nil, status.Errorf(codes.Internal, "create %s: %s: %v", e.ref.ID, step, err)
	}
	m.mu.Lock()
	e.state = stateReady
	m.mu.Unlock()
	return &gssv1.NodeCreateResponse{StartedAs: mode, ReadyLatency: durationpb.New(latency)}, nil
}

// chooseMode is the whole start-path policy (spec 7.3). AUTO takes the warm
// pool for a no-network sandbox when it has a ready sandbox, else restores
// when there is a checkpoint image, else starts cold. A forced mode that
// cannot be served fails with FailedPrecondition rather than measuring a
// different path.
func chooseMode(requested gssv1.StartMode, hasEgress bool, poolReady int, imageReady bool) (gssv1.StartMode, error) {
	warm := poolReady > 0 && !hasEgress
	switch requested {
	case gssv1.StartMode_START_MODE_AUTO:
		switch {
		case warm:
			return gssv1.StartMode_START_MODE_WARM, nil
		case imageReady:
			return gssv1.StartMode_START_MODE_RESTORE, nil
		}
		return gssv1.StartMode_START_MODE_COLD, nil
	case gssv1.StartMode_START_MODE_COLD:
		return gssv1.StartMode_START_MODE_COLD, nil
	case gssv1.StartMode_START_MODE_RESTORE:
		if !imageReady {
			return 0, status.Error(codes.FailedPrecondition, "no checkpoint image")
		}
		return gssv1.StartMode_START_MODE_RESTORE, nil
	case gssv1.StartMode_START_MODE_WARM:
		switch {
		case hasEgress:
			return 0, status.Error(codes.FailedPrecondition, "warm pool cannot serve egress")
		case !warm:
			return 0, status.Error(codes.FailedPrecondition, "warm pool empty")
		}
		return gssv1.StartMode_START_MODE_WARM, nil
	}
	return 0, status.Errorf(codes.InvalidArgument, "unknown start_mode %d", requested)
}

// restoreStep reports whether a failure in step of the restore path is the
// restore's own (the image did not give a ready sandbox), as opposed to a
// step the cold path shares and would fail the same way.
func restoreStep(step string) bool { return step == "runsc.restore" || step == "sandbox.ready" }

// restoreSandbox runs the restore path for e from imageDir (startSandbox)
// and counts its outcome for the image's drop rule: restoreSucceeded, or
// restoreFailed for a failure in a step of the restore's own. A restore
// whose ctx ended does not count. Creates and pool starts both use it.
func (m *Manager) restoreSandbox(ctx context.Context, e *sandbox, imageDir string) (string, error) {
	step, err := m.startSandbox(ctx, e, imageDir)
	if err == nil {
		m.restoreSucceeded()
	} else if restoreStep(step) && ctx.Err() == nil {
		m.restoreFailed(step, err)
	}
	return step, err
}

// coldAfterRestore is the AUTO fallback after the restore path failed with
// err in step: it removes everything the restore made (container, bundle,
// cgroup, network), then runs the cold path for the same entry once.
func (m *Manager) coldAfterRestore(ctx context.Context, e *sandbox, step string, err error) (string, error) {
	slog.Warn("restore failed; starting cold", "sandbox", e.id, "runsc_id", e.ref.ID, "step", step, "err", err)
	if terr := m.teardown(ctx, e); terr != nil {
		return step, fmt.Errorf("%w (cleanup before the cold start failed: %v)", err, terr)
	}
	m.mu.Lock()
	e.startedAs = gssv1.StartMode_START_MODE_COLD
	m.mu.Unlock()
	return m.startSandbox(ctx, e, "")
}

func modeName(mode gssv1.StartMode) string {
	switch mode {
	case gssv1.StartMode_START_MODE_COLD:
		return "cold"
	case gssv1.StartMode_START_MODE_RESTORE:
		return "restore"
	case gssv1.StartMode_START_MODE_WARM:
		return "warm"
	}
	return "auto"
}

// reserve puts a new starting entry for (id, inc) in the table, with its
// lifecycle lock held by the caller: for mode WARM a claimed pool sandbox
// (errPoolEmpty if there is none), else a new entry, which on a node at
// MaxSandboxes takes the place of a pool sandbox. A live entry at the same
// or a higher incarnation is AlreadyExists. Any other entry with that ID (a
// live older incarnation the scheduler has moved on from, or one being
// destroyed) is destroyed first.
func (m *Manager) reserve(ctx context.Context, id string, inc int32, spec *gssv1.SandboxSpec, mode gssv1.StartMode) (*sandbox, error) {
	for {
		m.mu.Lock()
		if !m.serving {
			m.mu.Unlock()
			return nil, status.Error(codes.Unavailable, "node agent is not serving (starting or shutting down)")
		}
		old := m.sandboxes[id]
		if old == nil {
			if err := m.admitLocked(spec); err != nil {
				m.mu.Unlock()
				return nil, err
			}
			if mode == gssv1.StartMode_START_MODE_WARM {
				// A claim moves a sandbox from the pool to the table: the
				// node holds no more sandboxes than before.
				e := m.claimLocked(id, inc, spec)
				m.mu.Unlock()
				if e == nil {
					return nil, errPoolEmpty
				}
				return e, nil
			}
			wait, err := m.roomLocked()
			if err != nil {
				m.mu.Unlock()
				return nil, err
			}
			e := m.newSandbox(id, inc, spec, mode)
			m.sandboxes[id] = e
			m.mu.Unlock()
			if wait != nil {
				if err := wait(ctx); err != nil {
					return nil, m.unreserve(ctx, e, err)
				}
			}
			return e, nil
		}
		live := old.state == stateStarting || old.state == stateReady
		if live && old.incarnation >= inc {
			m.mu.Unlock()
			return nil, status.Errorf(codes.AlreadyExists, "sandbox %s is live at incarnation %d", id, old.incarnation)
		}
		m.mu.Unlock()
		if err := m.destroyFor(ctx, old, "replaced"); err != nil {
			if ctx.Err() != nil {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			return nil, status.Errorf(codes.Internal, "create %s: remove incarnation %d: %v", id, old.incarnation, err)
		}
	}
}

// unreserve takes e, a reserved entry that has started nothing, out of the
// table again, after making room for it failed with err, and releases its
// lifecycle lock.
func (m *Manager) unreserve(ctx context.Context, e *sandbox, err error) error {
	m.mu.Lock()
	e.state = stateDestroying
	m.mu.Unlock()
	rerr := m.remove(ctx, e)
	e.unlock()
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	if rerr != nil {
		err = fmt.Errorf("%w (cleanup failed: %v)", err, rerr)
	}
	return status.Errorf(codes.Internal, "create %s: remove a pool sandbox to make room: %v", e.ref.ID, err)
}

// admitLocked checks that spec's CPU and memory fit next to every entry in
// the table (including ones still being torn down: they hold their
// resources until they are gone). Pool sandboxes count only once claimed;
// the number of sandboxes is roomLocked's check. Call with mu held.
func (m *Manager) admitLocked(spec *gssv1.SandboxSpec) error {
	var cpu, mem int64
	for _, e := range m.sandboxes {
		cpu += e.spec.GetResources().GetCpuMillis()
		mem += e.spec.GetResources().GetMemoryBytes()
	}
	// Used never exceeds capacity, so capacity - used cannot overflow.
	if c := spec.GetResources().GetCpuMillis(); c > m.cfg.CPUMillis-cpu {
		return status.Errorf(codes.ResourceExhausted, "node %s: %dm CPU requested, %dm of %dm free", m.cfg.NodeID, c, m.cfg.CPUMillis-cpu, m.cfg.CPUMillis)
	}
	if b := spec.GetResources().GetMemoryBytes(); b > m.cfg.MemoryBytes-mem {
		return status.Errorf(codes.ResourceExhausted, "node %s: %d bytes of memory requested, %d of %d free", m.cfg.NodeID, b, m.cfg.MemoryBytes-mem, m.cfg.MemoryBytes)
	}
	return nil
}

// newSandbox returns a starting entry whose lifecycle lock is held.
func (m *Manager) newSandbox(id string, inc int32, spec *gssv1.SandboxSpec, mode gssv1.StartMode) *sandbox {
	rid := runscID(id, inc)
	network := runsc.NetworkNone
	if len(spec.GetEgressAllow()) > 0 {
		network = runsc.NetworkSandbox
	}
	e := &sandbox{
		id: id, incarnation: inc, spec: spec, startedAs: mode,
		ref:       runsc.Ref{ID: rid, Network: network},
		bundleDir: filepath.Join(m.bundlesDir(), rid),
		cgroup:    bundle.CgroupPath(m.cfg.NodeID, rid),
		expires:   m.now().Add(spec.GetTtl().AsDuration() + m.cfg.TTLGrace),
		op:        make(chan struct{}, 1),
		state:     stateStarting,
		execs:     map[*execRun]struct{}{},
	}
	e.op <- struct{}{}
	return e
}

func (m *Manager) bundlesDir() string { return filepath.Join(m.cfg.StateDir, "bundles") }

// Destroy removes a sandbox. incarnation 0 means whatever is live; otherwise
// only a live incarnation <= incarnation is destroyed, so an instruction about
// an old copy cannot remove a newer one. An unknown ID is not an error. A
// Destroy during a Create of the same sandbox waits for the Create to finish;
// Execs in flight end with Unavailable and are not waited for.
//
// A Create of a newer incarnation may replace the entry Destroy found before
// Destroy gets to it, so Destroy looks again after each entry and goes on
// while it finds a different one that the request covers.
func (m *Manager) Destroy(ctx context.Context, sandboxID string, incarnation int32) (err error) {
	ctx, span := startSpan(ctx, "node.destroy",
		attribute.String("sandbox.id", sandboxID),
		attribute.Int("sandbox.incarnation", int(incarnation)),
		attribute.String("destroy.reason", "request"))
	defer func() { endSpan(span, err) }()

	var last *sandbox
	for {
		m.mu.Lock()
		e := m.sandboxes[sandboxID]
		m.mu.Unlock()
		if e == nil || e == last || (incarnation != 0 && e.incarnation > incarnation) {
			return nil
		}
		if m.destroyLookedUp != nil {
			m.destroyLookedUp(e)
		}
		if err := m.destroyEntry(ctx, e); err != nil {
			if ctx.Err() != nil {
				return status.FromContextError(ctx.Err()).Err()
			}
			return status.Errorf(codes.Internal, "destroy %s: %v", e.ref.ID, err)
		}
		last = e
	}
}

// destroyFor is a destroy the manager starts itself, traced as node.destroy
// with the reason (ttl, exited, retry, replaced, close).
func (m *Manager) destroyFor(ctx context.Context, e *sandbox, reason string) (err error) {
	ctx, span := startSpan(ctx, "node.destroy",
		attribute.String("sandbox.id", e.id),
		attribute.Int("sandbox.incarnation", int(e.incarnation)),
		attribute.String("destroy.reason", reason))
	defer func() { endSpan(span, err) }()
	return m.destroyEntry(ctx, e)
}

// List returns the live sandboxes (starting or ready), sorted by ID.
func (m *Manager) List() []*gssv1.NodeSandbox {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*gssv1.NodeSandbox, 0, len(m.sandboxes))
	for _, e := range m.sandboxes {
		if e.state != stateStarting && e.state != stateReady {
			continue
		}
		out = append(out, &gssv1.NodeSandbox{
			Id:          e.id,
			Spec:        proto.Clone(e.spec).(*gssv1.SandboxSpec),
			Incarnation: e.incarnation,
			StartedAs:   e.startedAs,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Id < out[j].Id })
	return out
}

// Close refuses new Creates, ends a pool start in flight, stops the loops
// and destroys every sandbox, pool sandboxes too, waiting (bounded by ctx)
// for Creates in flight. A second Close returns nil.
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed, m.serving = true, false
	if m.fill != nil {
		m.fill.cancel() // the refill loop tears it down before it stops
	}
	pooled := m.warm
	m.warm = nil
	for _, p := range pooled {
		m.dropping[p] = true
	}
	m.mu.Unlock()

	m.stopLoops()
	// Taken after the loops stopped, so that it has what they left broken.
	m.mu.Lock()
	entries := make([]*sandbox, 0, len(m.sandboxes))
	for _, e := range m.sandboxes {
		entries = append(entries, e)
	}
	m.mu.Unlock()
	errs := make([]error, len(entries)+len(pooled))
	var wg sync.WaitGroup
	for i, e := range entries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.destroyFor(ctx, e, "close"); err != nil {
				errs[i] = fmt.Errorf("destroy %s: %w", e.ref.ID, err)
			}
		}()
	}
	for i, p := range pooled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.dropPooled(ctx, p, "close"); err != nil {
				errs[len(entries)+i] = fmt.Errorf("destroy %s: %w", p.ref.ID, err)
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// stopLoops stops the reaper, the liveness loop and the refill loop and
// waits for them.
func (m *Manager) stopLoops() {
	m.stopOnce.Do(func() { close(m.stop) })
	m.loops.Wait()
}

func (m *Manager) loop(every time.Duration, fn func()) {
	defer m.loops.Done()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			fn()
		}
	}
}

// reapExpired destroys ready sandboxes whose ttl + TTLGrace has passed. The
// scheduler expires sandboxes at their TTL; this is the backstop for a node
// that lost contact with it (spec 6.6).
func (m *Manager) reapExpired() {
	now := m.now()
	m.mu.Lock()
	var expired []*sandbox
	for _, e := range m.sandboxes {
		if e.state == stateReady && now.After(e.expires) {
			expired = append(expired, e)
		}
	}
	m.mu.Unlock()
	for _, e := range expired {
		m.backgroundDestroy(e, "ttl")
	}
}

// checkLiveness removes ready sandboxes whose runsc sandbox process is gone
// (the scheduler notices them missing from heartbeats), and retries the
// teardown of broken ones. It also drops dead pool sandboxes (checkPool).
func (m *Manager) checkLiveness() {
	type check struct {
		e   *sandbox
		pid int
	}
	var checks []check
	var broken []*sandbox
	m.mu.Lock()
	for _, e := range m.sandboxes {
		switch e.state {
		case stateReady:
			checks = append(checks, check{e, e.pid})
		case stateBroken:
			broken = append(broken, e)
		}
	}
	m.mu.Unlock()
	for _, c := range checks {
		if !m.processAlive(c.pid) {
			m.backgroundDestroy(c.e, "exited")
		}
	}
	for _, e := range broken {
		m.backgroundDestroy(e, "retry")
	}
	m.checkPool()
}

func (m *Manager) backgroundDestroy(e *sandbox, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), loopLockWait)
	defer cancel()
	if err := m.destroyFor(ctx, e, reason); err != nil {
		slog.Warn("destroy failed", "sandbox", e.id, "runsc_id", e.ref.ID, "reason", reason, "err", err)
	}
}
