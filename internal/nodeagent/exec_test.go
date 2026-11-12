package nodeagent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
)

// collector is the send of one Exec. Exec calls it from one goroutine; the
// test reads it after Exec returned.
type collector struct {
	events []*gssv1.ExecEvent
}

func (c *collector) send(ev *gssv1.ExecEvent) error {
	c.events = append(c.events, ev)
	return nil
}

func (c *collector) stream(stderr bool) string {
	var b bytes.Buffer
	for _, ev := range c.events {
		if stderr {
			b.Write(ev.GetStderr())
		} else {
			b.Write(ev.GetStdout())
		}
	}
	return b.String()
}

// exit checks that exactly one exit event was sent, last, and returns it.
func (c *collector) exit(t *testing.T) *gssv1.ExitStatus {
	t.Helper()
	var exits int
	for _, ev := range c.events {
		if ev.GetExit() != nil {
			exits++
		}
	}
	if exits != 1 || len(c.events) == 0 || c.events[len(c.events)-1].GetExit() == nil {
		t.Fatalf("%d exit events in %d events; want exactly one, last", exits, len(c.events))
	}
	return c.events[len(c.events)-1].GetExit()
}

func argvReq(id string, args ...string) *gssv1.ExecRequest {
	return &gssv1.ExecRequest{SandboxId: id, Command: &gssv1.ExecRequest_Argv{Argv: &gssv1.Argv{Args: args}}}
}

func pyReq(id, code string) *gssv1.ExecRequest {
	return &gssv1.ExecRequest{SandboxId: id, Command: &gssv1.ExecRequest_PythonCode{PythonCode: code}}
}

// readyEnv returns a started env with sandbox sb-1 ready. Tests set
// e.rt.ExecFunc afterwards: the readiness probe runs the default (exit 0).
func readyEnv(t *testing.T, mod func(*Config, *Deps)) *env {
	t.Helper()
	e := startEnv(t, mod)
	e.mustCreate(t, "sb-1", 1, nil)
	return e
}

// killRecorder records "pid err" for every KillProcess.
type killRecorder struct {
	*runsc.Fake
	mu    sync.Mutex
	kills []string
}

func (k *killRecorder) KillProcess(ctx context.Context, ref runsc.Ref, pid int) error {
	err := k.Fake.KillProcess(ctx, ref, pid)
	k.mu.Lock()
	defer k.mu.Unlock()
	k.kills = append(k.kills, fmt.Sprintf("%d %v", pid, err))
	return err
}

func (k *killRecorder) Kills() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.Clone(k.kills)
}

// withKills wraps the env's fake runtime in a killRecorder.
func withKills(rec **killRecorder) func(*Config, *Deps) {
	return func(_ *Config, d *Deps) {
		*rec = &killRecorder{Fake: d.Runtime.(*runsc.Fake)}
		d.Runtime = *rec
	}
}

// oomCgroups returns the counts in turn from OOMKills; the last one repeats.
type oomCgroups struct {
	*fakeCgroups
	mu     sync.Mutex
	counts []uint64
}

func (c *oomCgroups) OOMKills(path string) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.counts[0]
	if len(c.counts) > 1 {
		c.counts = c.counts[1:]
	}
	return n, nil
}

// readPIDFile returns what the fake wrote to the Exec's pid file.
func readPIDFile(t *testing.T, s runsc.ExecSpec) string {
	t.Helper()
	b, err := os.ReadFile(s.InternalPIDFile)
	if err != nil || len(b) == 0 {
		t.Errorf("pid file %q: %q, %v", s.InternalPIDFile, b, err)
	}
	return string(b)
}

// recv receives from ch, or fails the test after 5 s.
func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

// waitCtx blocks until ctx ends, or fails the test after 5 s.
func waitCtx(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Error("the Exec's ctx was never cancelled")
	}
}

func TestExecArgv(t *testing.T) {
	e := readyEnv(t, nil)
	var got runsc.ExecSpec
	var stdin string
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		got = s
		b, err := io.ReadAll(s.Stdin)
		stdin = string(b)
		fmt.Fprint(s.Stdout, "out")
		fmt.Fprint(s.Stderr, "err")
		return 0, err
	}
	req := argvReq("sb-1", "sh", "-c", "echo hi")
	req.Env = map[string]string{"B": "2", "A": "1"}
	req.Stdin = []byte("IN")
	var c collector
	if err := e.m.Exec(context.Background(), req, c.send); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.Args, []string{"sh", "-c", "echo hi"}) || got.Cwd != "/work" || !slices.Equal(got.Env, []string{"A=1", "B=2"}) {
		t.Errorf("spec = args %q, cwd %q, env %q", got.Args, got.Cwd, got.Env)
	}
	if stdin != "IN" {
		t.Errorf("stdin = %q", stdin)
	}
	if dir := filepath.Dir(got.InternalPIDFile); dir != e.execDir("sb-1_1_") {
		t.Errorf("pid file %q is not in %s", got.InternalPIDFile, e.execDir("sb-1_1_"))
	}
	if exists(got.InternalPIDFile) {
		t.Error("pid file left after the Exec")
	}
	want := []string{"stdout out", "stderr err", "exit 0 false false"}
	var evs []string
	for _, ev := range c.events {
		switch {
		case ev.GetExit() != nil:
			x := ev.GetExit()
			evs = append(evs, fmt.Sprintf("exit %d %v %v", x.GetCode(), x.GetTimedOut(), x.GetOomKilled()))
		case ev.GetStdout() != nil:
			evs = append(evs, "stdout "+string(ev.GetStdout()))
		default:
			evs = append(evs, "stderr "+string(ev.GetStderr()))
		}
	}
	if !slices.Equal(evs, want) {
		t.Errorf("events = %q, want %q", evs, want)
	}
}

func TestExecPythonHeader(t *testing.T) {
	e := readyEnv(t, nil)
	var got runsc.ExecSpec
	var stdin string
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		got = s
		b, err := io.ReadAll(s.Stdin)
		stdin = string(b)
		return 0, err
	}
	req := pyReq("sb-1", "print(1)\n")
	req.Env = map[string]string{"K": "V"}
	req.Stdin = []byte("IN")
	var c collector
	if err := e.m.Exec(context.Background(), req, c.send); err != nil {
		t.Fatal(err)
	}
	if want := `{"code_len":9,"timeout_s":30,"env":{"K":"V"}}` + "\n" + "print(1)\n" + "IN"; stdin != want {
		t.Errorf("stdin = %q, want %q", stdin, want)
	}
	if !slices.Equal(got.Args, []string{"/gss/pyfork"}) || len(got.Env) != 0 || got.Cwd != "/work" {
		t.Errorf("spec = args %q, env %q, cwd %q", got.Args, got.Env, got.Cwd)
	}
	if exit := c.exit(t); exit.GetCode() != 0 {
		t.Errorf("exit = %v", exit)
	}
}

func TestExecChunking(t *testing.T) {
	e := readyEnv(t, nil)
	data := make([]byte, 100<<10)
	for i := range data {
		data[i] = byte(i * 7)
	}
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		_, err := s.Stdout.Write(data)
		return 0, err
	}
	var c collector
	if err := e.m.Exec(context.Background(), argvReq("sb-1", "cat"), c.send); err != nil {
		t.Fatal(err)
	}
	for i, ev := range c.events {
		if n := len(ev.GetStdout()); n > MaxChunk {
			t.Errorf("event %d carries %d bytes, more than %d", i, n, MaxChunk)
		}
	}
	if got := c.stream(false); got != string(data) {
		t.Errorf("stdout: %d bytes, not the %d written", len(got), len(data))
	}
	c.exit(t)
}

func TestExecExitCode(t *testing.T) {
	e := readyEnv(t, nil)
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) { return 7, nil }
	var c collector
	if err := e.m.Exec(context.Background(), argvReq("sb-1", "false"), c.send); err != nil {
		t.Fatal(err)
	}
	if exit := c.exit(t); exit.GetCode() != 7 || exit.GetTimedOut() || exit.GetOomKilled() {
		t.Errorf("exit = %v, want code 7", exit)
	}
}

func TestExecUnknownSandbox(t *testing.T) {
	e := readyEnv(t, nil)
	sent := 0
	send := func(*gssv1.ExecEvent) error { sent++; return nil }
	err := e.m.Exec(context.Background(), argvReq("nope", "true"), send)
	wantCode(t, err, codes.NotFound, "unknown sandbox")
	if sent != 0 {
		t.Errorf("send called %d times", sent)
	}
	if err := e.m.Destroy(context.Background(), "sb-1", 0); err != nil {
		t.Fatal(err)
	}
	err = e.m.Exec(context.Background(), argvReq("sb-1", "true"), send)
	wantCode(t, err, codes.NotFound, "destroyed sandbox")
	if sent != 0 {
		t.Errorf("send called %d times", sent)
	}
}

func TestExecNoCommand(t *testing.T) {
	e := readyEnv(t, nil)
	err := e.m.Exec(context.Background(), &gssv1.ExecRequest{SandboxId: "sb-1"}, (&collector{}).send)
	wantCode(t, err, codes.InvalidArgument, "no command")
}

func TestExecInvalidRequest(t *testing.T) {
	e := readyEnv(t, nil)
	before := len(e.rt.Calls())
	withEnv := func(env map[string]string) *gssv1.ExecRequest {
		r := argvReq("sb-1", "true")
		r.Env = env
		return r
	}
	withTimeout := func(d time.Duration) *gssv1.ExecRequest {
		r := pyReq("sb-1", "pass")
		r.Timeout = durationpb.New(d)
		return r
	}
	bad := map[string]*gssv1.ExecRequest{
		"empty argv":       argvReq("sb-1"),
		"NUL in argv":      argvReq("sb-1", "echo", "a\x00b"),
		"empty env name":   withEnv(map[string]string{"": "x"}),
		"= in env name":    withEnv(map[string]string{"A=B": "x"}),
		"NUL in env name":  withEnv(map[string]string{"A\x00": "x"}),
		"NUL in env value": withEnv(map[string]string{"A": "x\x00"}),
		"timeout 11m":      withTimeout(11 * time.Minute),
		"negative timeout": withTimeout(-time.Second),
		"code over 16 MiB": pyReq("sb-1", strings.Repeat("#", 16<<20+1)),
	}
	for name, req := range bad {
		sent := 0
		err := e.m.Exec(context.Background(), req, func(*gssv1.ExecEvent) error { sent++; return nil })
		wantCode(t, err, codes.InvalidArgument, name)
		if sent != 0 {
			t.Errorf("%s: send called %d times", name, sent)
		}
	}
	if got := e.rt.Calls()[before:]; len(got) != 0 {
		t.Errorf("runtime calls = %q", got)
	}
	// The limits themselves are accepted.
	for _, req := range []*gssv1.ExecRequest{withTimeout(10 * time.Minute), pyReq("sb-1", strings.Repeat("#", 16<<20))} {
		if err := e.m.Exec(context.Background(), req, (&collector{}).send); err != nil {
			t.Errorf("Exec at the limit: %v", err)
		}
	}
}

func TestExecTimeout(t *testing.T) {
	var rec *killRecorder
	e := readyEnv(t, withKills(&rec))
	pids := make(chan string, 1)
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		pids <- readPIDFile(t, s)
		waitCtx(t, ctx)
		return -1, ctx.Err()
	}
	req := argvReq("sb-1", "sleep", "60")
	req.Timeout = durationpb.New(50 * time.Millisecond)
	var c collector
	start := time.Now()
	if err := e.m.Exec(context.Background(), req, c.send); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("Exec took %v with a 50ms timeout", d)
	}
	if exit := c.exit(t); exit.GetCode() != 137 || !exit.GetTimedOut() || exit.GetOomKilled() {
		t.Errorf("exit = %v, want 137 timed_out", exit)
	}
	if got, want := rec.Kills(), []string{recv(t, pids, "the Exec") + " <nil>"}; !slices.Equal(got, want) {
		t.Errorf("kills = %q, want %q", got, want)
	}
}

// A python_code timeout is enforced by the zygote: pyfork exits 137 after
// the timeout and the agent marks it timed out without killing anything.
func TestExecPythonTimeout(t *testing.T) {
	var rec *killRecorder
	e := readyEnv(t, withKills(&rec))
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		time.Sleep(60 * time.Millisecond)
		return 137, nil
	}
	req := pyReq("sb-1", "while True: pass")
	req.Timeout = durationpb.New(50 * time.Millisecond)
	var c collector
	if err := e.m.Exec(context.Background(), req, c.send); err != nil {
		t.Fatal(err)
	}
	if exit := c.exit(t); exit.GetCode() != 137 || !exit.GetTimedOut() {
		t.Errorf("exit = %v, want 137 timed_out", exit)
	}
	if got := rec.Kills(); len(got) != 0 {
		t.Errorf("kills = %q", got)
	}

	// A 137 well before the timeout is not a timeout.
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) { return 137, nil }
	c = collector{}
	if err := e.m.Exec(context.Background(), pyReq("sb-1", "import os; os.kill(os.getpid(), 9)"), c.send); err != nil {
		t.Fatal(err)
	}
	if exit := c.exit(t); exit.GetCode() != 137 || exit.GetTimedOut() || exit.GetOomKilled() {
		t.Errorf("exit = %v, want a plain 137", exit)
	}
}

func TestExecCancel(t *testing.T) {
	var rec *killRecorder
	e := readyEnv(t, withKills(&rec))
	pids := make(chan string, 1)
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		pids <- readPIDFile(t, s)
		waitCtx(t, ctx)
		return -1, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var c collector
	errc := make(chan error, 1)
	go func() { errc <- e.m.Exec(ctx, argvReq("sb-1", "sleep", "60"), c.send) }()
	pid := recv(t, pids, "the Exec to start")
	cancel()
	select {
	case err := <-errc:
		wantCode(t, err, codes.Canceled, "cancelled Exec")
	case <-time.After(5 * time.Second):
		t.Fatal("Exec did not return after its ctx was cancelled")
	}
	if got, want := rec.Kills(), []string{pid + " <nil>"}; !slices.Equal(got, want) {
		t.Errorf("kills = %q, want %q", got, want)
	}
	if len(c.events) != 0 {
		t.Errorf("events sent to a cancelled caller: %v", c.events)
	}
}

func TestExecOOM(t *testing.T) {
	e := readyEnv(t, func(_ *Config, d *Deps) {
		d.Cgroups = &oomCgroups{fakeCgroups: d.Cgroups.(*fakeCgroups), counts: []uint64{0, 1}}
	})
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) { return 137, nil }
	var c collector
	if err := e.m.Exec(context.Background(), argvReq("sb-1", "python3", "-c", "x = bytearray(1 << 40)"), c.send); err != nil {
		t.Fatal(err)
	}
	if exit := c.exit(t); exit.GetCode() != 137 || !exit.GetOomKilled() || exit.GetTimedOut() {
		t.Errorf("exit = %v, want 137 oom_killed", exit)
	}
	// The sandbox process is still there, so the sandbox stays.
	if got := e.listed(); !slices.Equal(got, []string{"sb-1/1"}) {
		t.Errorf("List = %q", got)
	}
	// The counter did not rise this time.
	c = collector{}
	if err := e.m.Exec(context.Background(), argvReq("sb-1", "true"), c.send); err != nil {
		t.Fatal(err)
	}
	if exit := c.exit(t); exit.GetOomKilled() {
		t.Errorf("exit = %v, want no oom_killed", exit)
	}
}

func TestExecSendError(t *testing.T) {
	var rec *killRecorder
	e := readyEnv(t, withKills(&rec))
	cancelled := make(chan error, 1)
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		fmt.Fprint(s.Stdout, "first")
		waitCtx(t, ctx)
		cancelled <- ctx.Err()
		return -1, ctx.Err()
	}
	sendErr := errors.New("client gone")
	sent := 0
	err := e.m.Exec(context.Background(), argvReq("sb-1", "yes"), func(*gssv1.ExecEvent) error {
		sent++
		return sendErr
	})
	if !errors.Is(err, sendErr) {
		t.Errorf("Exec = %v, want the send error", err)
	}
	if sent != 1 {
		t.Errorf("send called %d times, want 1", sent)
	}
	if err := recv(t, cancelled, "the Exec to end"); err == nil {
		t.Error("runtime ctx not cancelled")
	}
	if got := rec.Kills(); len(got) != 1 || !strings.HasSuffix(got[0], " <nil>") {
		t.Errorf("kills = %q, want one successful kill", got)
	}
}

func TestExecDuringDestroy(t *testing.T) {
	e := readyEnv(t, nil)
	started := make(chan struct{})
	release := make(chan struct{})
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		close(started)
		waitCtx(t, ctx)
		<-release // the Exec stays in flight until the test lets it go
		return -1, ctx.Err()
	}
	errc := make(chan error, 1)
	go func() {
		errc <- e.m.Exec(context.Background(), pyReq("sb-1", "import time; time.sleep(60)"), (&collector{}).send)
	}()
	recv(t, started, "the Exec to start")

	destroyed := make(chan error, 1)
	go func() { destroyed <- e.m.Destroy(context.Background(), "sb-1", 0) }()
	select {
	case err := <-destroyed:
		if err != nil {
			t.Fatalf("Destroy: %v", err)
		}
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("Destroy waited for the Exec")
	}
	if e.inTable("sb-1") || len(e.rt.Containers()) != 0 || exists(e.bundleDir("sb-1_1_")) || exists(e.execDir("sb-1_1_")) {
		t.Errorf("not torn down: containers %v", e.rt.Containers())
	}
	close(release)
	select {
	case err := <-errc:
		wantCode(t, err, codes.Unavailable, "Exec during Destroy")
		if !strings.Contains(err.Error(), "destroyed") {
			t.Errorf("error %v does not say the sandbox was destroyed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Exec did not return after Destroy")
	}
	if exists(e.execDir("sb-1_1_")) {
		t.Error("the Exec left its pid directory behind")
	}
}

// An Exec that arrives once a destroy has started gets NotFound (spec D34).
func TestExecWhileDestroying(t *testing.T) {
	e := readyEnv(t, nil)
	e.rt.Delay["Delete"] = 300 * time.Millisecond
	destroyed := make(chan error, 1)
	go func() { destroyed <- e.m.Destroy(context.Background(), "sb-1", 0) }()
	eventually(t, "the destroy to reach runsc delete", func() bool {
		return slices.Contains(e.rt.Calls(), "Delete id=sb-1_1_ net=none")
	})
	sent := 0
	err := e.m.Exec(context.Background(), argvReq("sb-1", "true"), func(*gssv1.ExecEvent) error { sent++; return nil })
	wantCode(t, err, codes.NotFound, "Exec during the teardown")
	if sent != 0 {
		t.Errorf("send called %d times", sent)
	}
	if err := recv(t, destroyed, "Destroy"); err != nil {
		t.Fatal(err)
	}
}

// The timeout does not depend on the output stream: while send is blocked
// (a client that stopped reading but stays connected) the timer still
// kills the process and ends the runsc client.
func TestExecTimeoutWhileSendBlocked(t *testing.T) {
	var rec *killRecorder
	e := readyEnv(t, withKills(&rec))
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		fmt.Fprint(s.Stdout, "first")
		waitCtx(t, ctx)
		return -1, ctx.Err()
	}
	release := make(chan struct{})
	var c collector
	send := func(ev *gssv1.ExecEvent) error {
		if ev.GetStdout() != nil {
			select {
			case <-release:
			case <-time.After(10 * time.Second):
				return errors.New("never released")
			}
		}
		return c.send(ev)
	}
	req := argvReq("sb-1", "yes")
	req.Timeout = durationpb.New(50 * time.Millisecond)
	errc := make(chan error, 1)
	go func() { errc <- e.m.Exec(context.Background(), req, send) }()
	eventually(t, "the kill while send is blocked", func() bool { return len(rec.Kills()) == 1 })
	select {
	case err := <-errc:
		t.Fatalf("Exec returned while send was blocked: %v", err)
	default:
	}
	close(release)
	if err := recv(t, errc, "the Exec to return once send was released"); err != nil {
		t.Fatal(err)
	}
	if exit := c.exit(t); exit.GetCode() != 137 || !exit.GetTimedOut() {
		t.Errorf("exit = %v, want 137 timed_out", exit)
	}
	if got := rec.Kills(); len(got) != 1 || !strings.HasSuffix(got[0], " <nil>") {
		t.Errorf("kills = %q, want one successful kill", got)
	}
}

func TestWaitPID(t *testing.T) {
	dir := t.TempDir()
	never := make(chan struct{})
	empty := func(name string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}

	late := empty("late.pid")
	time.AfterFunc(100*time.Millisecond, func() { os.WriteFile(late, []byte("42"), 0o644) })
	start := time.Now()
	if pid, ok := waitPID(late, never, time.Second); !ok || pid != 42 {
		t.Errorf("late pid: %d, %v; want 42", pid, ok)
	}
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Errorf("late pid returned after %v, before it was written", d)
	}

	// The runtime call returned without a pid: stop waiting at once.
	finished := make(chan struct{})
	time.AfterFunc(50*time.Millisecond, func() { close(finished) })
	start = time.Now()
	if pid, ok := waitPID(empty("finished.pid"), finished, 5*time.Second); ok {
		t.Errorf("no pid, call finished: got %d", pid)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("waited %v after the call finished", d)
	}

	// Still running and no pid: give up after the wait.
	start = time.Now()
	if pid, ok := waitPID(empty("never.pid"), never, 100*time.Millisecond); ok {
		t.Errorf("no pid: got %d", pid)
	}
	if d := time.Since(start); d < 100*time.Millisecond || d > time.Second {
		t.Errorf("gave up after %v, want about 100ms", d)
	}
}

// An exit 128 with oom_kill risen can be the sentry dying (runsc exec's own
// failure code): the manager checks the sandbox and reports Unavailable
// when it is gone.
func TestExecOOMExit128SandboxGone(t *testing.T) {
	e := readyEnv(t, func(_ *Config, d *Deps) {
		d.Cgroups = &oomCgroups{fakeCgroups: d.Cgroups.(*fakeCgroups), counts: []uint64{0, 1}}
	})
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		time.AfterFunc(150*time.Millisecond, func() { e.rt.Delete(context.Background(), ref) })
		return 128, nil
	}
	err := e.m.Exec(context.Background(), pyReq("sb-1", "x = bytearray(1 << 30)"), (&collector{}).send)
	wantCode(t, err, codes.Unavailable, "exit 128 with the sandbox OOM-killed")
	if e.inTable("sb-1") {
		t.Error("the dead sandbox is still in the table")
	}
	err = e.m.Exec(context.Background(), argvReq("sb-1", "true"), (&collector{}).send)
	wantCode(t, err, codes.NotFound, "Exec after the sandbox died")
}

// ErrNotRunning from the runtime means the sandbox died under the Exec: the
// Exec is Unavailable and the manager removes the dead sandbox.
func TestExecSandboxDied(t *testing.T) {
	e := readyEnv(t, nil)
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		if err := e.rt.Delete(ctx, ref); err != nil {
			t.Error(err)
		}
		return -1, fmt.Errorf("exit not seen: %w", runsc.ErrNotRunning)
	}
	err := e.m.Exec(context.Background(), argvReq("sb-1", "true"), (&collector{}).send)
	wantCode(t, err, codes.Unavailable, "Exec in a dying sandbox")
	if e.inTable("sb-1") {
		t.Error("the dead sandbox is still in the table")
	}
	if e.cg.count("Remove /gss/n1/sb-1_1_") != 1 || exists(e.bundleDir("sb-1_1_")) || exists(e.execDir("sb-1_1_")) {
		t.Errorf("dead sandbox not cleaned up: cgroups %q", e.cg.Calls())
	}
	err = e.m.Exec(context.Background(), argvReq("sb-1", "true"), (&collector{}).send)
	wantCode(t, err, codes.NotFound, "Exec after the sandbox died")
}

// When the OOM killer takes the sentry, runsc exec can fail with a plain
// error while runsc state still says running. The manager keeps checking
// and removes the sandbox once it is gone.
func TestExecSentryOOMKilled(t *testing.T) {
	e := readyEnv(t, func(_ *Config, d *Deps) {
		d.Cgroups = &oomCgroups{fakeCgroups: d.Cgroups.(*fakeCgroups), counts: []uint64{0, 1}}
	})
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		time.AfterFunc(150*time.Millisecond, func() { e.rt.Delete(context.Background(), ref) })
		return -1, errors.New(`runsc exec: exit status 128: urpc method "containerManager.WaitPID" failed: EOF`)
	}
	start := time.Now()
	err := e.m.Exec(context.Background(), pyReq("sb-1", "x = bytearray(1 << 30)"), (&collector{}).send)
	wantCode(t, err, codes.Unavailable, "Exec whose sandbox was OOM-killed")
	if d := time.Since(start); d < 150*time.Millisecond || d > time.Second {
		t.Errorf("Exec returned after %v; want it to wait for the sandbox to go (150ms)", d)
	}
	if e.inTable("sb-1") {
		t.Error("the dead sandbox is still in the table")
	}
	err = e.m.Exec(context.Background(), argvReq("sb-1", "true"), (&collector{}).send)
	wantCode(t, err, codes.NotFound, "Exec after the sandbox died")
}

// A runtime error in a sandbox that is fine (a missing binary, say) is
// Internal and leaves the sandbox alone.
func TestExecRuntimeError(t *testing.T) {
	e := readyEnv(t, nil)
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		return -1, errors.New("runsc exec: executing processes for container: no such file or directory")
	}
	start := time.Now()
	err := e.m.Exec(context.Background(), argvReq("sb-1", "/no/such/binary"), (&collector{}).send)
	wantCode(t, err, codes.Internal, "missing binary")
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("Exec took %v", d)
	}
	if got := e.listed(); !slices.Equal(got, []string{"sb-1/1"}) {
		t.Errorf("List = %q", got)
	}
}

func TestExecStartingSandbox(t *testing.T) {
	e := startEnv(t, nil)
	e.rt.Delay["Start"] = 200 * time.Millisecond
	created := make(chan error, 1)
	go func() {
		_, err := e.create("sb-1", 1, nil)
		created <- err
	}()
	eventually(t, "Create to reach Start", func() bool { return slices.Contains(e.rt.Calls(), "Start id=sb-1_1_ net=none") })
	err := e.m.Exec(context.Background(), argvReq("sb-1", "true"), (&collector{}).send)
	wantCode(t, err, codes.Unavailable, "Exec during Create")
	if err := <-created; err != nil {
		t.Fatal(err)
	}
}

func TestExecConcurrent(t *testing.T) {
	e := readyEnv(t, nil)
	const n = 8
	var mu sync.Mutex
	inFlight := 0
	all := make(chan struct{})
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		mu.Lock()
		inFlight++
		if inFlight == n {
			close(all)
		}
		mu.Unlock()
		select { // every Exec runs at once: none waits for another
		case <-all:
		case <-time.After(5 * time.Second):
			return 1, errors.New("the Execs did not run in parallel")
		}
		fmt.Fprint(s.Stdout, s.Args[1])
		fmt.Fprint(s.Stderr, s.Args[1])
		return 0, nil
	}
	var wg sync.WaitGroup
	results := make([]*collector, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = &collector{}
			errs[i] = e.m.Exec(context.Background(), argvReq("sb-1", "echo", fmt.Sprint(i)), results[i].send)
		}()
	}
	wg.Wait()
	for i := range n {
		if errs[i] != nil {
			t.Errorf("Exec %d: %v", i, errs[i])
			continue
		}
		if out, errOut := results[i].stream(false), results[i].stream(true); out != fmt.Sprint(i) || errOut != fmt.Sprint(i) {
			t.Errorf("Exec %d: stdout %q, stderr %q", i, out, errOut)
		}
		if exit := results[i].exit(t); exit.GetCode() != 0 {
			t.Errorf("Exec %d: exit %v", i, exit)
		}
	}
	e.m.mu.Lock()
	left := len(e.m.sandboxes["sb-1"].execs)
	e.m.mu.Unlock()
	if left != 0 {
		t.Errorf("%d Execs still registered", left)
	}
}

func TestExecSpans(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	e := readyEnv(t, nil)
	for _, req := range []*gssv1.ExecRequest{argvReq("sb-1", "true"), pyReq("sb-1", "pass")} {
		sr.Reset()
		if err := e.m.Exec(context.Background(), req, (&collector{}).send); err != nil {
			t.Fatal(err)
		}
		var exec, child sdktrace.ReadOnlySpan
		for _, s := range sr.Ended() {
			switch s.Name() {
			case "node.exec":
				exec = s
			case "runsc.exec":
				child = s
			}
		}
		if exec == nil || child == nil {
			t.Fatalf("spans node.exec %v, runsc.exec %v", exec != nil, child != nil)
		}
		if child.Parent().SpanID() != exec.SpanContext().SpanID() {
			t.Error("runsc.exec is not a child of node.exec")
		}
		kind := "argv"
		if req.GetPythonCode() != "" {
			kind = "python"
		}
		if !slices.Contains(exec.Attributes(), attribute.String("exec.kind", kind)) {
			t.Errorf("node.exec attributes = %v, want exec.kind %s", exec.Attributes(), kind)
		}
	}
}
