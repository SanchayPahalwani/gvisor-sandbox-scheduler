package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// MaxChunk is the most stdout or stderr bytes one ExecEvent carries.
const MaxChunk = 32 << 10

// The Exec timeout (ExecRequest.timeout): default and maximum.
const (
	DefaultExecTimeout = 30 * time.Second
	MaxExecTimeout     = 10 * time.Minute
)

const (
	pyforkPath = "/gss/pyfork"
	execCwd    = "/work"
	// maxPythonCode is pyfork's limit on code_len (16 MiB).
	maxPythonCode = 16 << 20
	// zygoteGrace: the zygote enforces a python_code timeout inside the
	// sandbox; the agent's own timer fires this much later, as the backstop.
	zygoteGrace = 2 * time.Second
	// killTimeout bounds the runsc kill of an Exec's process.
	killTimeout = 5 * time.Second
	// stateTimeout bounds the runsc state that checks a sandbox after an Exec.
	stateTimeout = 5 * time.Second
	// pidWait and pidPoll: how long and how often a kill that is due waits
	// for runsc to write the process's pid.
	pidWait = time.Second
	pidPoll = 10 * time.Millisecond
	// goneWait and goneInterval: how long and how often the check of a
	// sandbox that seems to have died is repeated.
	goneWait     = 2 * time.Second
	goneInterval = 50 * time.Millisecond
	// exitKilled is the exit code of a process killed by SIGKILL (128 + 9).
	exitKilled = 137
	// exitRunscFailed is what runsc exec exits with when it fails itself.
	exitRunscFailed = 128
)

// execRun is one Exec in flight, registered on its entry so that a destroy
// can end it without waiting for it.
type execRun struct {
	cancel    context.CancelFunc // ends the runsc exec client
	destroyed bool               // guarded by Manager.mu: a destroy of the sandbox ended the Exec
}

// Exec runs a command in a ready sandbox and streams its output to send
// (spec 6.2). send is never called concurrently; each event carries at most
// MaxChunk bytes, and the bytes of each stream keep their order. When Exec
// returns nil, the last event sent was the one exit event. When it returns an
// error, no exit event was sent and the error is the outcome: NotFound,
// InvalidArgument, Canceled, Unavailable (the sandbox was destroyed or died
// during the Exec), Internal, or send's own error.
//
// A timeout kills the process inside the sandbox and gives exit 137 with
// timed_out. For python_code the zygote enforces the timeout itself, and the
// agent's timer, 2 s later, is the backstop. A cancelled ctx or a failed send
// also kills the process. A destroy of the sandbox ends the Exec and does not
// wait for it.
func (m *Manager) Exec(ctx context.Context, req *gssv1.ExecRequest, send func(*gssv1.ExecEvent) error) (err error) {
	ctx, span := startSpan(ctx, "node.exec", attribute.String("sandbox.id", req.GetSandboxId()))
	defer func() { endSpan(span, err) }()

	p, err := planExec(req)
	if err != nil {
		return err
	}
	span.SetAttributes(attribute.String("exec.kind", p.kind))
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}

	// The runsc exec client has a ctx of its own: when the caller goes away
	// the process inside the sandbox is killed first, then the client.
	rctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	run := &execRun{cancel: cancel}
	e, err := m.registerExec(req.GetSandboxId(), run)
	if err != nil {
		return err
	}
	out, err := m.execIn(ctx, rctx, cancel, e, p, send)
	if m.unregisterExec(e, run) {
		// This Exec may have made its pid directory after the teardown
		// removed it; the last Exec out removes it (rmdir fails while
		// another Exec still has a file there).
		os.Remove(m.execDir(e))
		// A destroy may also surface as exit 137 with no error; either way
		// the sandbox is going away.
		return status.Errorf(codes.Unavailable, "sandbox %s destroyed", e.id)
	}
	if err != nil {
		return err
	}
	exit, err := m.exitStatus(ctx, e, p, out)
	if err != nil {
		return err
	}
	span.SetAttributes(
		attribute.Int("exec.exit_code", int(exit.GetCode())),
		attribute.Bool("exec.timed_out", exit.GetTimedOut()),
		attribute.Bool("exec.oom_killed", exit.GetOomKilled()))
	return send(&gssv1.ExecEvent{Event: &gssv1.ExecEvent_Exit{Exit: exit}})
}

// execPlan is a validated ExecRequest in the form Runtime.Exec takes.
type execPlan struct {
	kind    string // "argv" or "python": the span's exec.kind
	args    []string
	env     []string // argv only; python_code passes env in pyfork's header
	stdin   io.Reader
	timeout time.Duration // the request's
	kill    time.Duration // when the agent's timer kills the process
}

// pyforkHeader is the first line of pyfork's stdin (cmd/pyfork).
type pyforkHeader struct {
	CodeLen  int               `json:"code_len"`
	TimeoutS float64           `json:"timeout_s"`
	Env      map[string]string `json:"env"`
}

func invalid(format string, args ...any) error {
	return status.Errorf(codes.InvalidArgument, format, args...)
}

func planExec(req *gssv1.ExecRequest) (*execPlan, error) {
	timeout, err := execTimeout(req.GetTimeout())
	if err != nil {
		return nil, err
	}
	if err := validateEnv(req.GetEnv()); err != nil {
		return nil, err
	}
	switch cmd := req.GetCommand().(type) {
	case *gssv1.ExecRequest_Argv:
		args := cmd.Argv.GetArgs()
		if len(args) == 0 {
			return nil, invalid("argv is empty")
		}
		for _, a := range args {
			if strings.ContainsRune(a, 0) {
				return nil, invalid("argv element %q contains NUL", a)
			}
		}
		return &execPlan{
			kind: "argv", args: args, env: sortedEnv(req.GetEnv()),
			stdin:   bytes.NewReader(req.GetStdin()),
			timeout: timeout, kill: timeout,
		}, nil
	case *gssv1.ExecRequest_PythonCode:
		code := cmd.PythonCode
		if len(code) > maxPythonCode {
			return nil, invalid("python_code is %d bytes, more than the 16 MiB limit", len(code))
		}
		env := req.GetEnv()
		if env == nil {
			env = map[string]string{} // "env":{} rather than null
		}
		header, err := json.Marshal(pyforkHeader{CodeLen: len(code), TimeoutS: timeout.Seconds(), Env: env})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode pyfork header: %v", err)
		}
		stdin := io.MultiReader(bytes.NewReader(append(header, '\n')), strings.NewReader(code), bytes.NewReader(req.GetStdin()))
		return &execPlan{
			kind: "python", args: []string{pyforkPath}, stdin: stdin,
			timeout: timeout, kill: timeout + zygoteGrace,
		}, nil
	}
	return nil, invalid("neither argv nor python_code is set")
}

// execTimeout applies the default (nil or zero) and the limits.
func execTimeout(d *durationpb.Duration) (time.Duration, error) {
	if d == nil {
		return DefaultExecTimeout, nil
	}
	if err := d.CheckValid(); err != nil {
		return 0, invalid("timeout: %v", err)
	}
	t := d.AsDuration()
	switch {
	case t < 0:
		return 0, invalid("timeout %v is negative", t)
	case t == 0:
		return DefaultExecTimeout, nil
	case t > MaxExecTimeout:
		return 0, invalid("timeout %v is above the maximum of %v", t, MaxExecTimeout)
	}
	return t, nil
}

// validateEnv rejects what pyfork rejects (with exit 125) and what cannot be
// a runsc argument: an empty name, '=' or NUL in a name, NUL in a value.
func validateEnv(env map[string]string) error {
	for k, v := range env {
		if k == "" || strings.ContainsAny(k, "=\x00") {
			return invalid("env name %q is empty or contains '=' or NUL", k)
		}
		if strings.ContainsRune(v, 0) {
			return invalid("env value of %q contains NUL", k)
		}
	}
	return nil
}

func sortedEnv(env map[string]string) []string {
	out := make([]string, 0, len(env))
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// registerExec finds the ready entry for id and registers run on it; a
// destroy that starts after this ends run.
func (m *Manager) registerExec(id string, run *execRun) (*sandbox, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.sandboxes[id]
	if e == nil {
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", id)
	}
	switch e.state {
	case stateReady:
		e.execs[run] = struct{}{}
		return e, nil
	case stateStarting:
		return nil, status.Errorf(codes.Unavailable, "sandbox %s is still starting", id)
	}
	return nil, status.Errorf(codes.NotFound, "sandbox %s not found (being destroyed)", id)
}

// unregisterExec removes run from e and reports whether a destroy ended it.
func (m *Manager) unregisterExec(e *sandbox, run *execRun) (destroyed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(e.execs, run)
	return run.destroyed
}

// execOutcome is what happened to one Exec, before it becomes an exit status
// or an error.
type execOutcome struct {
	code      int   // from Runtime.Exec
	err       error // from Runtime.Exec
	sendErr   error // the first error from send
	cancelled bool  // the caller's ctx ended first
	timedOut  bool  // the agent's timer fired first
	elapsed   time.Duration
	oomBefore uint64
	oomErr    error // reading oom_kill before the Exec failed
}

// execIn runs the Exec. Its loop is the one goroutine that calls send: the
// runtime's stdout and stderr writers hand it their chunks over an
// unbuffered channel, so output is held one chunk at a time and each stream
// keeps its order. The timeout runs on its own timer, not in the loop, so a
// send that blocks (a client that stopped reading) cannot hold it off.
// Stopping the Exec (timeout, cancel, failed send) kills the process inside
// the sandbox and then calls cancel, which ends the runsc exec client
// (rctx). execIn returns after Runtime.Exec has returned and any stop has
// finished.
func (m *Manager) execIn(ctx, rctx context.Context, cancel context.CancelFunc, e *sandbox, p *execPlan, send func(*gssv1.ExecEvent) error) (out execOutcome, err error) {
	pidFile, err := newPIDFile(m.execDir(e))
	if err != nil {
		return out, status.Errorf(codes.Internal, "exec in %s: %v", e.ref.ID, err)
	}
	defer os.Remove(pidFile)
	out.oomBefore, out.oomErr = m.cg.OOMKills(e.cgroup)

	events := make(chan *gssv1.ExecEvent)
	ended := make(chan struct{}) // the loop is done: a late Write must not block
	defer close(ended)
	spec := runsc.ExecSpec{
		Args: p.args, Env: p.env, Cwd: execCwd, Stdin: p.stdin,
		Stdout:          &streamWriter{events: events, ended: ended},
		Stderr:          &streamWriter{events: events, ended: ended, stderr: true},
		InternalPIDFile: pidFile,
	}
	var res struct {
		code int
		err  error
	}
	runDone := make(chan struct{}) // closed once Runtime.Exec has returned and res is set
	began := time.Now()
	go func() {
		defer close(runDone)
		res.code, res.err = m.rt.Exec(rctx, e.ref, spec)
	}()

	// The first stop wins; it runs in its own goroutine, so neither the
	// timer nor the loop waits for runsc kill.
	var (
		mu                 sync.Mutex
		stopped, over      bool // a stop started; the runtime call has returned
		timedOut, killSent bool
		killed             = make(chan struct{}) // the stop's kill and cancel are done
	)
	stop := func(timeout bool) {
		mu.Lock()
		defer mu.Unlock()
		if stopped || over {
			return
		}
		stopped, timedOut = true, timeout
		go func() {
			defer close(killed)
			sent := m.killExec(ctx, e, pidFile, runDone)
			mu.Lock()
			killSent = sent
			mu.Unlock()
			cancel()
		}()
	}
	timer := time.AfterFunc(p.kill, func() { stop(true) })
	defer timer.Stop()

	done := ctx.Done()
	for {
		select {
		case ev := <-events:
			// Output that arrives after a timeout is still the program's;
			// after a cancel or a failed send nobody reads it.
			if out.sendErr == nil && !out.cancelled {
				if err := send(ev); err != nil {
					out.sendErr = err
					stop(false)
				}
			}
		case <-done:
			done = nil
			out.cancelled = true
			stop(false)
		case <-runDone:
			// Every Write returned before Runtime.Exec did, and each one
			// waited for this loop to take its chunk: nothing is pending.
			out.code, out.err, out.elapsed = res.code, res.err, time.Since(began)
			timer.Stop()
			mu.Lock()
			over = true
			wasStopped := stopped
			mu.Unlock()
			if wasStopped {
				<-killed
			}
			mu.Lock()
			out.timedOut = timedOut
			retry := wasStopped && !killSent
			mu.Unlock()
			if retry {
				// runsc wrote the pid only after the kill gave up waiting.
				m.killExec(ctx, e, pidFile, runDone)
			}
			return out, nil
		}
	}
}

// exitStatus turns the outcome of a finished Exec into its exit status, or
// into the error Exec returns instead.
func (m *Manager) exitStatus(ctx context.Context, e *sandbox, p *execPlan, out execOutcome) (*gssv1.ExitStatus, error) {
	switch {
	case out.sendErr != nil:
		return nil, out.sendErr
	case out.cancelled:
		return nil, status.FromContextError(ctx.Err()).Err()
	case out.timedOut:
		return &gssv1.ExitStatus{Code: exitKilled, TimedOut: true}, nil
	case out.err != nil:
		// The sandbox may have died under the Exec. When the error says so,
		// or the OOM killer struck (it can pick the sentry, spec 6.4), keep
		// checking for a moment: right after the sentry is killed, runsc
		// state can still say running (seen in TestRealExecMemoryLimit).
		notRunning := errors.Is(out.err, runsc.ErrNotRunning)
		if m.removeIfGone(ctx, e, notRunning || m.oomRose(e, out)) || notRunning {
			return nil, status.Errorf(codes.Unavailable, "sandbox %s stopped during the exec: %v", e.id, out.err)
		}
		return nil, status.Errorf(codes.Internal, "exec in %s: %v", e.ref.ID, out.err)
	}
	exit := &gssv1.ExitStatus{Code: int32(out.code)}
	if out.code == exitRunscFailed && m.oomRose(e, out) {
		// runsc exec's own failure code, with the OOM killer active: it can
		// be the sentry dying, which the wrapper did not recognise.
		if m.removeIfGone(ctx, e, true) {
			return nil, status.Errorf(codes.Unavailable, "sandbox %s stopped during the exec (exit 128 after an OOM kill)", e.id)
		}
		return exit, nil
	}
	if out.code != exitKilled {
		return exit, nil
	}
	if m.oomRose(e, out) {
		// The OOM killer took the process. The sandbox reported its exit,
		// so it was alive then; check it once anyway.
		exit.OomKilled = true
		m.removeIfGone(ctx, e, false)
	} else if p.kind == "python" && out.elapsed >= p.timeout {
		// The zygote killed the worker at the timeout.
		exit.TimedOut = true
	}
	return exit, nil
}

// oomRose reports whether the sandbox cgroup's oom_kill counter went up
// during the Exec. An Exec running at the same time in the same sandbox can
// raise it too; the counter is per sandbox.
func (m *Manager) oomRose(e *sandbox, out execOutcome) bool {
	if out.oomErr != nil {
		return false
	}
	after, err := m.cg.OOMKills(e.cgroup)
	return err == nil && after > out.oomBefore
}

// removeIfGone checks the sandbox after an Exec that suggests it died, and
// destroys the entry if the sandbox process is gone, so later Execs get
// NotFound instead of reaching a dead sandbox. With wait it checks again
// every goneInterval for up to goneWait. It waits for the teardown and
// reports whether the sandbox was gone.
func (m *Manager) removeIfGone(ctx context.Context, e *sandbox, wait bool) bool {
	ctx = context.WithoutCancel(ctx)
	deadline := time.Now().Add(goneWait)
	for !m.sandboxGone(ctx, e) {
		if !wait || time.Now().After(deadline) {
			return false
		}
		time.Sleep(goneInterval)
	}
	dctx, cancel := context.WithTimeout(ctx, loopLockWait)
	defer cancel()
	if err := m.destroyFor(dctx, e, "exited"); err != nil {
		slog.Warn("destroy failed", "sandbox", e.id, "runsc_id", e.ref.ID, "reason", "exited", "err", err)
	}
	return true
}

// sandboxGone reports whether e's sandbox process has gone: runsc state does
// not know the container or says it is not running, or the process is dead.
func (m *Manager) sandboxGone(ctx context.Context, e *sandbox) bool {
	sctx, cancel := context.WithTimeout(ctx, stateTimeout)
	st, err := m.rt.State(sctx, e.ref)
	cancel()
	m.mu.Lock()
	pid := e.pid
	m.mu.Unlock()
	return errors.Is(err, runsc.ErrNotFound) || (err == nil && st.Status != "running") || !m.processAlive(pid)
}

// killExec sends SIGKILL to the Exec's process inside the sandbox, by the
// pid runsc writes to pidFile. If the pid is not there yet it waits for it,
// up to pidWait or until the runtime call has returned (runDone). It
// reports false if there was no pid to kill.
func (m *Manager) killExec(ctx context.Context, e *sandbox, pidFile string, runDone <-chan struct{}) bool {
	pid, ok := waitPID(pidFile, runDone, pidWait)
	if !ok {
		return false
	}
	kctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), killTimeout)
	defer cancel()
	// It fails if the process has just exited; nothing is left to do then.
	if err := m.rt.KillProcess(kctx, e.ref, pid); err != nil {
		slog.Warn("exec kill failed", "runsc_id", e.ref.ID, "pid", pid, "err", err)
	}
	return true
}

// waitPID returns the pid in pidFile, checking every pidPoll for up to max
// while runDone is open. Once runDone is closed it reads the file once more
// and stops.
func waitPID(pidFile string, runDone <-chan struct{}, max time.Duration) (int, bool) {
	deadline := time.Now().Add(max)
	for {
		if pid, ok := readPID(pidFile); ok {
			return pid, true
		}
		if !time.Now().Before(deadline) {
			return 0, false
		}
		select {
		case <-runDone:
			return readPID(pidFile)
		case <-time.After(pidPoll):
		}
	}
}

// readPID reads a positive pid from pidFile; an empty file has none yet.
func readPID(pidFile string) (int, bool) {
	b, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	return pid, err == nil && pid > 0
}

// execDir holds the pid files of e's Execs. It is apart from the bundle
// directory so that creating one never races the bundle's removal; teardown
// removes it after the bundle.
func (m *Manager) execDir(e *sandbox) string { return filepath.Join(m.execsDir(), e.ref.ID) }

func (m *Manager) execsDir() string { return filepath.Join(m.cfg.StateDir, "execs") }

// newPIDFile creates dir if needed and an empty file in it for runsc's
// --internal-pid-file.
func newPIDFile(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, "exec-*.pid")
	if err != nil {
		return "", err
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		os.Remove(name)
		return "", err
	}
	return name, nil
}

// streamWriter is an Exec's Stdout or Stderr. Write hands p to the Exec's
// loop in events of at most MaxChunk bytes and returns once the loop has
// taken the last one.
type streamWriter struct {
	events chan<- *gssv1.ExecEvent
	ended  <-chan struct{}
	stderr bool
}

func (w *streamWriter) Write(p []byte) (int, error) {
	for n := 0; n < len(p); {
		chunk := bytes.Clone(p[n:min(n+MaxChunk, len(p))])
		ev := &gssv1.ExecEvent{Event: &gssv1.ExecEvent_Stdout{Stdout: chunk}}
		if w.stderr {
			ev = &gssv1.ExecEvent{Event: &gssv1.ExecEvent_Stderr{Stderr: chunk}}
		}
		select {
		case w.events <- ev:
		case <-w.ended:
			return n, io.ErrClosedPipe
		}
		n += len(chunk)
	}
	return len(p), nil
}
