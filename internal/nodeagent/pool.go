package nodeagent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bundle"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The warm pool (spec 7.1, call D16) holds WarmPoolSize ready sandboxes of
// the default size with no network, started by the refill loop: restored
// from the checkpoint image when the node has one, else cold. A claim takes
// one, rewrites its cgroup limits to the request and puts it in the table
// under the requested sandbox ID; it keeps its runsc ID ("warm_<8 hex>_"),
// bundle and cgroup. Pool sandboxes are not in the table until claimed, so
// they are not listed and do not count against the node's CPU and memory,
// but each one counts against MaxSandboxes (heldLocked), and they give way
// to real work: the pool does not grow on a full node, and a Create that
// finds the node full takes the place of one (roomLocked).

// defaultPoolRetry is how long the refill loop waits after a failed start.
const defaultPoolRetry = 2 * time.Second

// errPoolEmpty is a WARM Create's error when no pool sandbox was ready:
// chooseMode's for a forced WARM, and the claim's when other Creates took
// the pool since.
var errPoolEmpty = status.Error(codes.FailedPrecondition, "warm pool empty")

// poolFill is the start of one pool sandbox by the refill loop.
type poolFill struct {
	cancel context.CancelFunc // ends the start: Close, or a Create taking its place
	done   chan struct{}      // closed once the sandbox is in the pool or torn down

	// Guarded by Manager.mu; err is set before done is closed, so a reader
	// that saw done closed may read it without mu.
	yielded bool  // a Create took this sandbox's place: it is torn down
	err     error // the teardown failed (the sandbox is then broken in the table)
}

// WarmReady is the number of pool sandboxes ready to be claimed.
func (m *Manager) WarmReady() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.warm)
}

// heldLocked is the number of sandboxes that count against MaxSandboxes:
// every table entry (live, being destroyed or broken) and every pool
// sandbox (ready, being started or being torn down). Call with mu held.
func (m *Manager) heldLocked() int {
	n := len(m.sandboxes) + len(m.warm) + len(m.dropping)
	if m.fill != nil {
		n++
	}
	return n
}

// kickRefill makes the refill loop look at the pool again.
func (m *Manager) kickRefill() {
	select {
	case m.refill <- struct{}{}:
	default:
	}
}

// refillLoop keeps WarmPoolSize sandboxes in the pool, starting one at a
// time, until Close. It runs after Start and whenever kicked (a claim or a
// destroy), and waits poolRetry after a failed start.
func (m *Manager) refillLoop() {
	defer m.loops.Done()
	for {
		for {
			ctx, p, f := m.nextFill()
			if p == nil {
				break
			}
			if err := m.fillOne(ctx, p, f); err != nil {
				slog.Warn("pool sandbox did not start; retrying", "node", m.cfg.NodeID, "runsc_id", p.ref.ID,
					"in", m.poolRetry, "err", err)
				t := time.NewTimer(m.poolRetry)
				select {
				case <-m.stop:
					t.Stop()
					return
				case <-t.C:
				}
			}
		}
		select {
		case <-m.stop:
			return
		case <-m.refill:
		}
	}
}

// nextFill returns a new pool sandbox to start, with its fill recorded and
// the ctx of its start, when the pool is short of WarmPoolSize, the node
// holds fewer than MaxSandboxes and Close has not begun; else nil.
func (m *Manager) nextFill() (context.Context, *sandbox, *poolFill) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || len(m.warm) >= m.cfg.WarmPoolSize || m.heldLocked() >= m.cfg.MaxSandboxes {
		return nil, nil, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.fill = &poolFill{cancel: cancel, done: make(chan struct{})}
	return ctx, m.poolSandboxLocked(), m.fill
}

// fillOne starts p, traced as pool.fill, and puts it in the pool. When the
// start failed, Close began or a Create took p's place, p is torn down
// instead. It returns an error only for a failed start, after which the
// loop waits.
func (m *Manager) fillOne(ctx context.Context, p *sandbox, f *poolFill) error {
	defer f.cancel()
	ctx, span := startSpan(ctx, "pool.fill", attribute.String("runsc.id", p.ref.ID))
	imageDir := m.restoreImage()
	mode := gssv1.StartMode_START_MODE_COLD
	if imageDir != "" {
		mode = gssv1.StartMode_START_MODE_RESTORE
	}
	span.SetAttributes(attribute.String("start.mode", modeName(mode)))
	step, err := m.startPooled(ctx, p, imageDir)
	m.mu.Lock()
	gaveWay := f.yielded || m.closed
	if err == nil && !gaveWay {
		p.state = stateReady
		m.warm = append(m.warm, p)
		m.fill = nil
		m.mu.Unlock()
		close(f.done)
		endSpan(span, nil)
		return nil
	}
	m.mu.Unlock()

	terr := m.teardown(ctx, p)
	m.mu.Lock()
	m.fill = nil
	f.err = terr
	m.settleLocked(p, terr)
	m.mu.Unlock()
	close(f.done)
	if terr != nil {
		terr = fmt.Errorf("cleanup failed: %w", terr)
	}
	if gaveWay {
		// The start ended because it was asked to; only the cleanup can fail.
		span.SetAttributes(attribute.Bool("pool.gave_way", true))
		endSpan(span, terr)
		return nil
	}
	err = errors.Join(fmt.Errorf("%s: %w", step, err), terr)
	endSpan(span, err)
	return err
}

// startPooled runs a start path for the pool sandbox p up to readiness and
// records its pid: the restore path from imageDir (its outcome counts for
// the image's drop rule as a Create's does), or the cold path when imageDir
// is "".
func (m *Manager) startPooled(ctx context.Context, p *sandbox, imageDir string) (step string, err error) {
	if imageDir != "" {
		step, err = m.restoreSandbox(ctx, p, imageDir)
	} else {
		step, err = m.startSandbox(ctx, p, "")
	}
	if err != nil {
		return step, err
	}
	return "runsc.state", m.recordPID(ctx, p)
}

// poolSandboxLocked returns a new pool sandbox, not in the table: no
// network, the default size, and a runsc ID "warm_<8 hex>_". "warm" is a
// reserved sandbox ID and sandbox IDs contain no '_', so no other runsc ID
// of the node is a prefix of it or has it as a prefix. Call with mu held.
func (m *Manager) poolSandboxLocked() *sandbox {
	rid := m.poolIDLocked()
	spec := &gssv1.SandboxSpec{}
	gssv1.ApplyDefaults(spec)
	return &sandbox{
		id: rid, spec: spec,
		ref:       runsc.Ref{ID: rid, Network: runsc.NetworkNone},
		bundleDir: filepath.Join(m.bundlesDir(), rid),
		cgroup:    bundle.CgroupPath(m.cfg.NodeID, rid),
		op:        make(chan struct{}, 1),
		state:     stateStarting,
		execs:     map[*execRun]struct{}{},
	}
}

// poolIDLocked returns a random pool runsc ID that no sandbox of the node
// has: a second container under one ID would make the teardown of the one
// that failed to start delete the other. Call with mu held.
func (m *Manager) poolIDLocked() string {
	for {
		var b [4]byte
		rand.Read(b[:]) // crypto/rand.Read never returns an error
		if rid := "warm_" + hex.EncodeToString(b[:]) + "_"; !m.runscIDUsedLocked(rid) {
			return rid
		}
	}
}

// runscIDUsedLocked reports whether a sandbox of the node has runsc ID rid:
// a table entry or a pool sandbox (ready or being torn down; the refill
// loop starts one at a time, so none is being started). Call with mu held.
func (m *Manager) runscIDUsedLocked(rid string) bool {
	for _, e := range m.sandboxes {
		if e.ref.ID == rid {
			return true
		}
	}
	for p := range m.dropping {
		if p.ref.ID == rid {
			return true
		}
	}
	return slices.ContainsFunc(m.warm, func(p *sandbox) bool { return p.ref.ID == rid })
}

// claimLocked takes a ready pool sandbox for sandbox id at incarnation inc
// and puts it in the table as starting, with spec and its lifecycle lock
// held by the caller; nil if the pool is empty. It keeps its runsc ID,
// bundle, cgroup and pid. Call with mu held, after admitLocked.
func (m *Manager) claimLocked(id string, inc int32, spec *gssv1.SandboxSpec) *sandbox {
	p := m.popWarmLocked()
	if p == nil {
		return nil
	}
	p.op <- struct{}{} // free: no one else holds a pool sandbox
	p.id, p.incarnation, p.spec = id, inc, spec
	p.startedAs = gssv1.StartMode_START_MODE_WARM
	p.expires = m.now().Add(spec.GetTtl().AsDuration() + m.cfg.TTLGrace)
	m.sandboxes[id] = p
	return p
}

// popWarmLocked takes the oldest ready pool sandbox out of the pool; nil if
// the pool is empty. Call with mu held.
func (m *Manager) popWarmLocked() *sandbox {
	if len(m.warm) == 0 {
		return nil
	}
	p := m.warm[0]
	m.warm = slices.Delete(m.warm, 0, 1)
	return p
}

// claim serves a Create from the pool: it takes a ready pool sandbox
// (reserve) and rewrites its limits to the request. There is no runsc call
// on this path. miss reports that the pool could not serve the Create:
// it was empty by now (errPoolEmpty), or the rewrite failed and the
// sandbox was removed (Internal); in AUTO the Create then takes the next
// start path. Other errors are the ones every path would get.
func (m *Manager) claim(ctx context.Context, id string, inc int32, spec *gssv1.SandboxSpec) (miss bool, err error) {
	e, err := m.reserve(ctx, id, inc, spec, gssv1.StartMode_START_MODE_WARM)
	if err != nil {
		return errors.Is(err, errPoolEmpty), err
	}
	defer e.unlock()
	defer m.kickRefill()
	if err := m.setClaimLimits(ctx, e); err != nil {
		m.mu.Lock()
		e.state = stateDestroying
		m.mu.Unlock()
		err = fmt.Errorf("pool.claim: %w", err)
		if rerr := m.remove(ctx, e); rerr != nil {
			err = fmt.Errorf("%w (cleanup failed: %v)", err, rerr)
		}
		slog.Warn("claim failed; pool sandbox removed", "sandbox", id, "runsc_id", e.ref.ID, "err", err)
		return true, status.Errorf(codes.Internal, "create %s: %v", e.ref.ID, err)
	}
	m.mu.Lock()
	e.state = stateReady
	m.mu.Unlock()
	return false, nil
}

// setClaimLimits rewrites cpu.max and memory.max of a claimed pool sandbox
// to its request (spec 6.4), traced as pool.claim. The sentry keeps the
// memory total it read at boot (call D16). A memory limit below what the
// sandbox uses would make the kernel reclaim or OOM-kill in it; an idle
// primed sandbox uses about 60 MiB and the smallest valid request is
// 192 MiB, so a valid request never does.
func (m *Manager) setClaimLimits(ctx context.Context, e *sandbox) (err error) {
	_, span := startSpan(ctx, "pool.claim", attribute.String("runsc.id", e.ref.ID))
	defer func() { endSpan(span, err) }()
	r := e.spec.GetResources()
	return m.cg.SetLimits(e.cgroup, r.GetCpuMillis(), r.GetMemoryBytes())
}

// roomLocked makes room for one more sandbox on a node that holds
// MaxSandboxes by taking the place of a pool sandbox: a ready one, else the
// one being started, whose start it ends. The caller puts its entry in the
// table in the same critical section, so the place cannot go to anyone
// else, and then calls wait, outside mu and before it starts anything:
// wait returns once the pool sandbox is gone. With room to spare it returns
// nil, nil; with nothing to give way, ResourceExhausted. Call with mu held.
func (m *Manager) roomLocked() (wait func(context.Context) error, err error) {
	if m.heldLocked() < m.cfg.MaxSandboxes {
		return nil, nil
	}
	if p := m.popWarmLocked(); p != nil {
		m.dropping[p] = true
		return func(ctx context.Context) error { return m.dropPooled(ctx, p, "yield") }, nil
	}
	if f := m.fill; f != nil && !f.yielded {
		f.yielded = true
		f.cancel()
		return func(ctx context.Context) error {
			select {
			case <-f.done:
				return f.err
			case <-ctx.Done():
				return ctx.Err()
			}
		}, nil
	}
	return nil, status.Errorf(codes.ResourceExhausted, "node %s is at its maximum of %d sandboxes", m.cfg.NodeID, m.cfg.MaxSandboxes)
}

// dropPooled tears down p, a pool sandbox the caller took out of the pool
// and put in m.dropping, traced as node.destroy with reason.
func (m *Manager) dropPooled(ctx context.Context, p *sandbox, reason string) (err error) {
	ctx, span := startSpan(ctx, "node.destroy",
		attribute.String("runsc.id", p.ref.ID),
		attribute.String("destroy.reason", reason))
	defer func() { endSpan(span, err) }()
	err = m.teardown(ctx, p)
	m.mu.Lock()
	delete(m.dropping, p)
	m.settleLocked(p, err)
	m.mu.Unlock()
	m.kickRefill()
	return err
}

// settleLocked records the end of a pool sandbox's teardown. When err is
// not nil, what is left of p goes into the table as broken, under its
// runsc ID (no sandbox ID contains '_'), so that it keeps counting against
// the node and Destroy, the liveness loop and Close retry it like any
// broken entry. Call with mu held.
func (m *Manager) settleLocked(p *sandbox, err error) {
	if err != nil {
		p.state = stateBroken
		m.sandboxes[p.id] = p
		return
	}
	p.state = stateGone
}

// checkPool drops the ready pool sandboxes whose sandbox process is gone,
// so that no Create claims a dead one.
func (m *Manager) checkPool() {
	type check struct {
		p   *sandbox
		pid int
	}
	m.mu.Lock()
	checks := make([]check, 0, len(m.warm))
	for _, p := range m.warm {
		checks = append(checks, check{p, p.pid})
	}
	m.mu.Unlock()
	for _, c := range checks {
		if m.processAlive(c.pid) || !m.takePooled(c.p) {
			continue
		}
		if err := m.dropPooled(context.Background(), c.p, "exited"); err != nil {
			slog.Warn("destroy failed", "runsc_id", c.p.ref.ID, "reason", "exited", "err", err)
		}
	}
}

// takePooled takes p out of the pool for a teardown, if it is still there.
func (m *Manager) takePooled(p *sandbox) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	i := slices.Index(m.warm, p)
	if i < 0 {
		return false
	}
	m.warm = slices.Delete(m.warm, i, i+1)
	m.dropping[p] = true
	return true
}
