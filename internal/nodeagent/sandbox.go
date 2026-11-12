package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bundle"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
	"go.opentelemetry.io/otel/attribute"
)

// sandboxIDRE allows no '_' or '.': runsc IDs are "<sandboxID>_<incarnation>_".
var sandboxIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,39}$`)

// reservedIDs start the runsc IDs of the node's own sandboxes: the warm pool
// ("warm_<hex>_") and the checkpoint image builder ("img_<key>_<pid>_").
var reservedIDs = map[string]bool{"warm": true, "img": true}

func validateSandboxID(id string) error {
	if !sandboxIDRE.MatchString(id) {
		return fmt.Errorf("sandbox_id %q must be 1-40 characters of [A-Za-z0-9-] starting with a letter or digit", id)
	}
	if reservedIDs[id] {
		return fmt.Errorf("sandbox_id %q is reserved", id)
	}
	return nil
}

// runscID is the runsc container ID of one incarnation of a sandbox. runsc
// matches container IDs by prefix, so no ID under one root may be a prefix of
// another. Sandbox IDs contain no '_', so the first '_' ends the sandbox ID,
// and the trailing '_' ends the incarnation: two IDs share a prefix only if
// they are equal.
func runscID(sandboxID string, incarnation int32) string {
	return fmt.Sprintf("%s_%d_", sandboxID, incarnation)
}

// sandboxState is where an entry is in its life. Only starting and ready
// entries are live: listed, and (when ready) open to Exec.
type sandboxState int

const (
	stateStarting   sandboxState = iota // Create is running
	stateReady                          // serving Execs
	stateDestroying                     // teardown is running
	stateBroken                         // teardown failed; Destroy and the liveness loop retry it
	stateGone                           // torn down and out of the table
)

// sandbox is one entry of the manager's table, keyed by sandbox ID. Every
// entry uses its share of the node's capacity until it is gone.
type sandbox struct {
	// Set before the entry is in the table, then never changed. A pool
	// sandbox gets its id, incarnation, spec and expiry when a Create claims
	// it, as it enters the table.
	id          string
	incarnation int32
	spec        *gssv1.SandboxSpec // defaulted and validated
	ref         runsc.Ref
	bundleDir   string
	cgroup      string
	expires     time.Time // created + ttl + TTLGrace

	// op is a lock held by the one lifecycle operation (Create, destroy) on
	// this entry; a Destroy during a Create waits for it. Execs do not take it.
	op chan struct{}

	// Guarded by Manager.mu; changed only by the holder of op.
	state     sandboxState
	pid       int             // the runsc sandbox process, once ready
	startedAs gssv1.StartMode // COLD after a restore that fell back

	// The Execs in flight, guarded by Manager.mu. An Exec adds itself while
	// the entry is ready; a destroy ends them all when it starts.
	execs map[*execRun]struct{}

	// The steps done so far, so a teardown undoes exactly those. Only the
	// holder of op touches them.
	netUp         bool // Net.Setup succeeded
	bundleWritten bool // the bundle directory may exist
	containerMade bool // Runtime.Create ran: the container and its cgroup may exist
}

// lock takes the entry's lifecycle lock, or fails when ctx ends first.
func (s *sandbox) lock(ctx context.Context) error {
	select {
	case s.op <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *sandbox) unlock() { <-s.op }

// readyProbe is pyfork's stdin for a no-op python_code Exec: the header line,
// then the 4 bytes of code.
const readyProbe = `{"code_len":4,"timeout_s":0,"env":{}}` + "\n" + "pass"

// containerLog is the file in the bundle directory that gets the container's
// own stdout and stderr (the zygote's, not an Exec's).
const containerLog = "container.log"

// startSandbox runs a start path up to readiness (spec 7.3): network,
// bundle, runsc create, then runsc start (the cold path, imageDir "") or
// runsc restore from the checkpoint image in imageDir, then the readiness
// probe, bounded by ReadyTimeout (and after a restore also by
// restoreReadyTimeout). On failure it returns the name of the failing step;
// e records what was done, for teardown.
func (m *Manager) startSandbox(ctx context.Context, e *sandbox, imageDir string) (step string, err error) {
	var netns string
	var hosts []bundle.HostEntry
	if len(e.spec.GetEgressAllow()) > 0 {
		sctx, span := startSpan(ctx, "netpolicy.setup", attribute.String("runsc.id", e.ref.ID))
		netns, hosts, err = m.net.Setup(sctx, e.ref.ID, e.spec.GetEgressAllow())
		endSpan(span, err)
		if err != nil {
			return "netpolicy.setup", err
		}
		e.netUp = true
	}
	e.bundleWritten = true
	err = bundle.Write(bundle.Options{
		ID: e.ref.ID, Dir: e.bundleDir, RootFS: m.cfg.RootFS, NodeID: m.cfg.NodeID,
		CPUMillis: e.spec.GetResources().GetCpuMillis(), MemoryBytes: e.spec.GetResources().GetMemoryBytes(),
		NetNSPath: netns, Hosts: hosts, Prime: m.cfg.Prime,
	})
	if err != nil {
		return "bundle", err
	}
	e.containerMade = true
	if err := m.rt.Create(ctx, e.ref, e.bundleDir, filepath.Join(e.bundleDir, containerLog)); err != nil {
		return "runsc.create", err
	}
	if imageDir == "" {
		if err := m.rt.Start(ctx, e.ref); err != nil {
			return "runsc.start", err
		}
	} else if err := m.rt.Restore(ctx, e.ref, e.bundleDir, imageDir); err != nil {
		return "runsc.restore", err
	}
	ready := m.cfg.ReadyTimeout
	if imageDir != "" && m.restoreReadyTimeout < ready {
		ready = m.restoreReadyTimeout
	}
	if err := m.waitReady(ctx, e, ready); err != nil {
		return "sandbox.ready", err
	}
	return "", nil
}

// waitReady runs the readiness probe (spec 6.3): a no-op python_code Exec,
// bounded by timeout. pyfork retries its connection to the zygote, so on a
// cold start this one call returns when the interpreter is ready; a restored
// zygote is already listening.
func (m *Manager) waitReady(ctx context.Context, e *sandbox, timeout time.Duration) (err error) {
	ctx, span := startSpan(ctx, "sandbox.ready", attribute.String("runsc.id", e.ref.ID))
	defer func() { endSpan(span, err) }()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out := &tailBuffer{max: 1024}
	code, err := m.rt.Exec(ctx, e.ref, runsc.ExecSpec{
		Args:   []string{"/gss/pyfork"},
		Stdin:  strings.NewReader(readyProbe),
		Stdout: out, Stderr: out,
	})
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("probe exited %d: %q", code, out.String())
	}
	return nil
}

// recordPID looks up the sandbox process for the liveness check.
func (m *Manager) recordPID(ctx context.Context, e *sandbox) error {
	st, err := m.rt.State(ctx, e.ref)
	if err != nil {
		return err
	}
	if st.Status != "running" || st.PID <= 0 {
		return fmt.Errorf("container is %q with pid %d", st.Status, st.PID)
	}
	m.mu.Lock()
	e.pid = st.PID
	m.mu.Unlock()
	return nil
}

// destroyEntry tears e down and removes it from the table. It first takes
// e's lifecycle lock, so it waits for an in-flight Create (or another
// destroy) of e; ctx bounds only that wait. It does not wait for Execs: it
// marks each one destroyed and ends its runsc exec client, and each returns
// Unavailable on its own. A nil error means e is gone.
func (m *Manager) destroyEntry(ctx context.Context, e *sandbox) error {
	if err := e.lock(ctx); err != nil {
		return err
	}
	defer e.unlock()
	m.mu.Lock()
	if e.state == stateGone {
		m.mu.Unlock()
		return nil
	}
	e.state = stateDestroying // new Execs refuse from here on
	for run := range e.execs {
		run.destroyed = true
		run.cancel()
	}
	m.mu.Unlock()
	return m.remove(ctx, e)
}

// remove runs the teardown of e, whose lock the caller holds, and takes e
// out of the table. If the teardown fails, e stays in the table as broken:
// what is left of it still uses the node, and a later attempt finishes it.
func (m *Manager) remove(ctx context.Context, e *sandbox) error {
	err := m.teardown(ctx, e)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		e.state = stateBroken
		return err
	}
	e.state = stateGone
	if m.sandboxes[e.id] == e {
		delete(m.sandboxes, e.id)
	}
	m.kickRefill() // the pool may have room again
	return nil
}

// teardown undoes the steps recorded in e: runsc delete, network teardown,
// bundle directory, exec pid directory, cgroup. Each step is idempotent and
// is forgotten once it succeeded. If the delete fails the container may
// still be running, so the host-side pieces it uses are left for the next
// attempt. After the delete, every process left in the sandbox's cgroup is
// killed: one runsc no longer tracks would otherwise keep the cgroup busy for
// good.
//
// It never runs with a cancelled ctx: the caller's may have ended (a
// cancelled Create), and runsc fails at once on a done context.
func (m *Manager) teardown(ctx context.Context, e *sandbox) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	var killErr error
	if e.containerMade {
		if err := m.rt.Delete(ctx, e.ref); err != nil {
			return fmt.Errorf("runsc.delete: %w", err)
		}
		killErr = m.cg.KillTree(e.cgroup)
	}
	var errs []error
	if e.netUp {
		if err := m.net.Teardown(ctx, e.ref.ID); err != nil {
			errs = append(errs, fmt.Errorf("netpolicy.teardown: %w", err))
		} else {
			e.netUp = false
		}
	}
	if e.bundleWritten {
		if err := os.RemoveAll(e.bundleDir); err != nil {
			errs = append(errs, fmt.Errorf("remove bundle: %w", err))
		} else {
			e.bundleWritten = false
		}
	}
	// The Execs' pid files; the directory exists only if an Exec ran.
	if err := os.RemoveAll(m.execDir(e)); err != nil {
		errs = append(errs, fmt.Errorf("remove exec pid files: %w", err))
	}
	if e.containerMade {
		// A failed kill matters only if the cgroup then cannot be removed.
		if err := m.cg.Remove(e.cgroup); err != nil {
			errs = append(errs, fmt.Errorf("remove cgroup: %w", errors.Join(err, killErr)))
		} else {
			e.containerMade = false
		}
	}
	return errors.Join(errs...)
}

// pidAlive reports whether a host process exists, by sending it signal 0.
// EPERM means it exists. A pid that is not positive cannot be checked
// (kill(0) and kill(-1) address groups), so it counts as alive.
func pidAlive(pid int) bool {
	if pid <= 0 {
		return true
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// tailBuffer keeps the last max bytes written to it.
type tailBuffer struct {
	mu  sync.Mutex
	max int
	b   []byte
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.b = append(t.b, p...)
	if len(t.b) > t.max {
		t.b = append(t.b[:0], t.b[len(t.b)-t.max:]...)
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.TrimSpace(string(t.b))
}
