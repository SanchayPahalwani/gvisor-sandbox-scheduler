package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
)

func TestParseCPU(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{"500m", 500}, {"0.5", 500}, {"2", 2000}, {"1.25", 1250}, {"100m", 100}, {"0.001", 1},
	} {
		got, err := parseCPU(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("parseCPU(%q) = %d, %v; want %d", tc.in, got, err, tc.want)
		}
	}
	for _, in := range []string{"abc", "", "0", "0m", "-1", "1.5m", "0.0005", "m", "1e3", "2 ", "99999999999999999999"} {
		if got, err := parseCPU(in); err == nil {
			t.Errorf("parseCPU(%q) = %d, want an error", in, got)
		}
	}
}

func TestParseMem(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{"256Mi", 256 << 20}, {"1Gi", 1 << 30}, {"1048576", 1048576}, {"512Ki", 512 << 10}, {"1Ti", 1 << 40},
	} {
		got, err := parseMem(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("parseMem(%q) = %d, %v; want %d", tc.in, got, err, tc.want)
		}
	}
	for _, in := range []string{"bad", "", "0", "0Mi", "-5Mi", "256M", "1.5Gi", "Mi", "256mi", "9999999999Ti"} {
		if got, err := parseMem(in); err == nil {
			t.Errorf("parseMem(%q) = %d, want an error", in, got)
		}
	}
}

// fake is a SandboxService that records what it is asked and answers from its fields.
type fake struct {
	gssv1.UnimplementedSandboxServiceServer

	mu       sync.Mutex
	creates  []*gssv1.SandboxSpec
	execs    []*gssv1.ExecRequest
	destroys []string
	gets     []string
	// exitCode is the code of the exit event each Exec ends with.
	exitCode int32
	timedOut bool
	// execEnd, when set, ends each Exec with this status after the output and
	// sends no exit event.
	execEnd error
	// destroyErr fails every DestroySandbox.
	destroyErr error
	nodes      []*gssv1.Node
	sandboxes  []*gssv1.Sandbox
	// createGate, when set, holds each CreateSandbox until it is closed;
	// createStarted is closed when the first one arrives.
	createGate, createStarted chan struct{}
	// execBlock holds each Exec after its output until the call's context ends;
	// execStarted is closed when the first one has sent its output.
	execBlock   bool
	execStarted chan struct{}
}

func (f *fake) CreateSandbox(_ context.Context, req *gssv1.CreateSandboxRequest) (*gssv1.Sandbox, error) {
	f.mu.Lock()
	gate, started := f.createGate, f.createStarted
	f.mu.Unlock()
	if gate != nil {
		close(started)
		<-gate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, proto.Clone(req.GetSpec()).(*gssv1.SandboxSpec))
	return &gssv1.Sandbox{
		Id: fmt.Sprintf("sb-%d", len(f.creates)), Spec: req.GetSpec(), State: gssv1.SandboxState_SANDBOX_STATE_READY,
		NodeId: "a1", NodeGroup: "a", StartedAs: gssv1.StartMode_START_MODE_WARM, Incarnation: 1,
		StartLatency: durationpb.New(2 * time.Millisecond),
	}, nil
}

func (f *fake) Exec(req *gssv1.ExecRequest, stream grpc.ServerStreamingServer[gssv1.ExecEvent]) error {
	f.mu.Lock()
	f.execs = append(f.execs, proto.Clone(req).(*gssv1.ExecRequest))
	code, timedOut, end := f.exitCode, f.timedOut, f.execEnd
	block, started := f.execBlock, f.execStarted
	f.mu.Unlock()
	out := "1\n"
	if req.GetPythonCode() != "print(1)" {
		out = "other\n"
	}
	for _, ev := range []*gssv1.ExecEvent{
		{Event: &gssv1.ExecEvent_Stdout{Stdout: []byte(out)}},
		{Event: &gssv1.ExecEvent_Stderr{Stderr: []byte("warning\n")}},
	} {
		if err := stream.Send(ev); err != nil {
			return err
		}
	}
	if block {
		close(started)
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	if end != nil {
		return end
	}
	return stream.Send(&gssv1.ExecEvent{Event: &gssv1.ExecEvent_Exit{Exit: &gssv1.ExitStatus{Code: code, TimedOut: timedOut}}})
}

func (f *fake) DestroySandbox(_ context.Context, req *gssv1.DestroySandboxRequest) (*gssv1.DestroySandboxResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.destroys = append(f.destroys, req.GetSandboxId())
	if f.destroyErr != nil {
		return nil, f.destroyErr
	}
	return &gssv1.DestroySandboxResponse{}, nil
}

func (f *fake) GetSandbox(_ context.Context, req *gssv1.GetSandboxRequest) (*gssv1.Sandbox, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets = append(f.gets, req.GetSandboxId())
	for _, sb := range f.sandboxes {
		if sb.GetId() == req.GetSandboxId() {
			return sb, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "sandbox %s not found", req.GetSandboxId())
}

func (f *fake) ListSandboxes(context.Context, *gssv1.ListSandboxesRequest) (*gssv1.ListSandboxesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &gssv1.ListSandboxesResponse{Sandboxes: f.sandboxes}, nil
}

func (f *fake) ListNodes(context.Context, *gssv1.ListNodesRequest) (*gssv1.ListNodesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &gssv1.ListNodesResponse{Nodes: f.nodes}, nil
}

// now is the fixed clock of every test.
var now = time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)

// harness runs gssctl command lines against a fake served over bufconn.
type harness struct {
	f              *fake
	c              *cli
	stdout, stderr *bytes.Buffer
	env            map[string]string
	dialed         []string // the addresses gssctl dialled
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	f := &fake{}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	gssv1.RegisterSandboxServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	h := &harness{f: f, stdout: &bytes.Buffer{}, stderr: &bytes.Buffer{}, env: map[string]string{}}
	h.c = &cli{
		stdout: h.stdout, stderr: h.stderr,
		getenv: func(k string) string { return h.env[k] },
		now:    func() time.Time { return now },
		dial: func(addr string) (*grpc.ClientConn, error) {
			h.dialed = append(h.dialed, addr)
			return grpc.NewClient("passthrough:///bufnet",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
				grpc.WithTransportCredentials(insecure.NewCredentials()))
		},
	}
	return h
}

func (h *harness) run(args ...string) int { return h.runCtx(context.Background(), args...) }

func (h *harness) runCtx(ctx context.Context, args ...string) int {
	h.stdout.Reset()
	h.stderr.Reset()
	return h.c.main(ctx, args)
}

// brokenPipe is a stdout whose reader has gone, as with `gssctl run ... | head -1`.
type brokenPipe struct{}

func (brokenPipe) Write([]byte) (int, error) { return 0, syscall.EPIPE }

// A write to a closed stdout ends the run, and the sandbox is still destroyed.
func TestRunStdoutBroken(t *testing.T) {
	h := newHarness(t)
	h.c.stdout = brokenPipe{}
	if code := h.run("run", "-c", "print(1)"); code != 1 {
		t.Errorf("exit code %d, want 1", code)
	}
	if !strings.Contains(h.stderr.String(), "broken pipe") {
		t.Errorf("stderr = %q, want the write error", h.stderr)
	}
	if strings.Join(h.f.destroys, " ") != "sb-1" {
		t.Errorf("destroys = %q, want [sb-1]", h.f.destroys)
	}
}

// gssctl catches SIGPIPE and SIGHUP as well as SIGINT and SIGTERM, so that a
// closed pipe or a hung-up terminal ends the run through the destroy.
func TestSignals(t *testing.T) {
	for _, want := range []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGPIPE} {
		if !slices.Contains(signals, want) {
			t.Errorf("signals %v lack %v", signals, want)
		}
	}
}

// TestHelperProcess is gssctl itself when GSSCTL_HELPER_PROCESS is set: the test
// below runs the test binary again as a gssctl process, with the arguments after "--".
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GSSCTL_HELPER_PROCESS") != "1" {
		t.Skip("only as the child of a *Process test")
	}
	args := os.Args
	for i, a := range args {
		if a == "--" {
			args = args[i+1:]
			break
		}
	}
	os.Exit(gssctl(args))
}

// serveTCP serves f on a loopback port, for tests that run gssctl as a process.
func serveTCP(t *testing.T, f *fake) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	gssv1.RegisterSandboxServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// gssctlProcess is the command that runs gssctl with args as a child process, with
// OTEL_EXPORTER_OTLP_ENDPOINT set to otel (empty: telemetry off).
func gssctlProcess(otel string, args ...string) *exec.Cmd {
	cmd := exec.Command(os.Args[0], append([]string{"-test.run=^TestHelperProcess$", "--"}, args...)...)
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "OTEL_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "GSSCTL_HELPER_PROCESS=1", "OTEL_EXPORTER_OTLP_ENDPOINT="+otel)
	return cmd
}

// A gssctl process whose stdout reader is gone (gssctl run ... | head -0) is not
// killed by SIGPIPE: the write fails, the sandbox is destroyed, gssctl exits 1.
func TestRunBrokenPipeProcess(t *testing.T) {
	f := &fake{}
	addr := serveTCP(t, f)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	r.Close() // no reader: the first write to stdout raises SIGPIPE
	cmd := gssctlProcess("", "--scheduler", addr, "run", "-c", "print(1)")
	var stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = w, &stderr
	err = cmd.Run()
	w.Close()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 1 {
		t.Errorf("gssctl ended with %v, want exit status 1; stderr %q", err, stderr.String())
	}
	if !strings.Contains(stderr.String(), "broken pipe") {
		t.Errorf("stderr = %q, want the write error", stderr.String())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if strings.Join(f.destroys, " ") != "sb-1" {
		t.Errorf("destroys = %q, want [sb-1]", f.destroys)
	}
}

// With OTLP export on and no collector listening, gssctl still runs the program and
// exits with its code; the flush at exit gives up after flushTimeout and says so.
func TestRunTelemetryOnProcess(t *testing.T) {
	f := &fake{}
	f.exitCode = 3
	addr := serveTCP(t, f)
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	collector := "http://" + dead.Addr().String()
	dead.Close() // nothing listens there now
	cmd := gssctlProcess(collector, "--scheduler", addr, "run", "-c", "print(1)")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	start := time.Now()
	err = cmd.Run()
	took := time.Since(start)
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 3 {
		t.Errorf("gssctl ended with %v, want exit status 3; stderr %q", err, stderr.String())
	}
	if stdout.String() != "1\n" {
		t.Errorf("stdout = %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "telemetry flush failed") {
		t.Errorf("stderr = %q, want the failed flush reported", stderr.String())
	}
	if took > flushTimeout+5*time.Second {
		t.Errorf("gssctl took %v; the flush must give up after %v", took, flushTimeout)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if strings.Join(f.destroys, " ") != "sb-1" {
		t.Errorf("destroys = %q, want [sb-1]", f.destroys)
	}
}

// SIGPIPE is caught but does not end the context (a broken gRPC socket raises it
// too); SIGHUP ends it.
func TestNotifyContext(t *testing.T) {
	ctx, stop := notifyContext(context.Background())
	defer stop()
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGPIPE); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if ctx.Err() != nil {
		t.Fatalf("SIGPIPE ended the context: %v", context.Cause(ctx))
	}
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
		if got := context.Cause(ctx).Error(); got != "signal: hangup" {
			t.Errorf("cause %q, want signal: hangup", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SIGHUP did not end the context")
	}
}

// Ctrl-C while the program runs: the Exec is cancelled, the sandbox destroyed,
// and gssctl exits 130.
func TestRunInterruptedDuringExec(t *testing.T) {
	h := newHarness(t)
	h.f.execBlock, h.f.execStarted = true, make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-h.f.execStarted
		cancel()
	}()
	if code := h.runCtx(ctx, "run", "-c", "print(1)"); code != 130 {
		t.Errorf("exit code %d, want 130; stderr %q", code, h.stderr)
	}
	if strings.Join(h.f.destroys, " ") != "sb-1" {
		t.Errorf("destroys = %q, want [sb-1]", h.f.destroys)
	}
	if !strings.Contains(h.stderr.String(), "interrupted") {
		t.Errorf("stderr = %q", h.stderr)
	}
}

// Ctrl-C while the sandbox is being created: the create is waited for, the new
// sandbox destroyed, nothing run in it, and gssctl exits 130.
func TestRunInterruptedDuringCreate(t *testing.T) {
	h := newHarness(t)
	h.f.createGate, h.f.createStarted = make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-h.f.createStarted
		cancel()
		time.Sleep(50 * time.Millisecond) // the create is still in flight after the cancel
		close(h.f.createGate)
	}()
	if code := h.runCtx(ctx, "run", "-c", "print(1)"); code != 130 {
		t.Errorf("exit code %d, want 130; stderr %q", code, h.stderr)
	}
	if strings.Join(h.f.destroys, " ") != "sb-1" || len(h.f.execs) != 0 {
		t.Errorf("destroys = %q, execs %d; want [sb-1] and none", h.f.destroys, len(h.f.execs))
	}
}

// summaryRE is the line run prints after the program's output.
var summaryRE = regexp.MustCompile(`(?m)^sandbox sb-1 node=a1 group=a start=warm ready=2ms total=\d+ms\n\z`)

func TestRunAgainstFake(t *testing.T) {
	h := newHarness(t)
	if code := h.run("run", "-c", "print(1)"); code != 0 {
		t.Fatalf("exit code %d, stderr %q", code, h.stderr)
	}
	if got := h.stdout.String(); got != "1\n" {
		t.Errorf("stdout = %q, want %q", got, "1\n")
	}
	if !strings.HasPrefix(h.stderr.String(), "warning\n") || !summaryRE.MatchString(h.stderr.String()) {
		t.Errorf("stderr = %q, want the program's stderr, then a line matching %s", h.stderr, summaryRE)
	}
	if len(h.f.execs) != 1 || h.f.execs[0].GetSandboxId() != "sb-1" || h.f.execs[0].GetPythonCode() != "print(1)" {
		t.Errorf("execs = %v", h.f.execs)
	}
	if got := h.f.execs[0].GetTimeout().AsDuration(); got != 30*time.Second {
		t.Errorf("exec timeout = %v, want the default 30s", got)
	}
	if len(h.f.destroys) != 1 || h.f.destroys[0] != "sb-1" {
		t.Errorf("destroys = %q, want [sb-1]", h.f.destroys)
	}
	if len(h.dialed) != 1 || h.dialed[0] != "127.0.0.1:7000" {
		t.Errorf("dialled %q, want the default 127.0.0.1:7000", h.dialed)
	}

	// The exit code is the one in the exit event.
	h.f.exitCode = 3
	if code := h.run("run", "-c", "import sys; sys.exit(3)"); code != 3 {
		t.Errorf("exit code %d, want 3; stderr %q", code, h.stderr)
	}
	if len(h.f.destroys) != 2 {
		t.Errorf("destroys = %q, want the second sandbox destroyed too", h.f.destroys)
	}
}

func TestRunKeep(t *testing.T) {
	h := newHarness(t)
	if code := h.run("run", "--keep", "-c", "print(1)"); code != 0 {
		t.Fatalf("exit code %d, stderr %q", code, h.stderr)
	}
	if h.stdout.String() != "1\n" || !summaryRE.MatchString(h.stderr.String()) {
		t.Errorf("stdout %q, stderr %q", h.stdout, h.stderr)
	}
	if len(h.f.destroys) != 0 {
		t.Errorf("destroys = %q, want none with --keep", h.f.destroys)
	}
}

// Spec D34: a stream that ends with a status and no exit event did not see the
// program exit; gssctl prints the status and exits 1, whatever the program did.
func TestRunStreamEndsWithoutExit(t *testing.T) {
	h := newHarness(t)
	h.f.exitCode = 7
	h.f.execEnd = status.Error(codes.Unavailable, "sandbox sb-1 lost: node a1 is dead")
	if code := h.run("run", "-c", "print(1)"); code != 1 {
		t.Errorf("exit code %d, want 1", code)
	}
	if !strings.Contains(h.stderr.String(), "Unavailable: sandbox sb-1 lost: node a1 is dead") {
		t.Errorf("stderr = %q, want the status", h.stderr)
	}
	if strings.Contains(h.stderr.String(), "total=") {
		t.Errorf("stderr = %q: no summary line without an exit", h.stderr)
	}
	if len(h.f.destroys) != 1 {
		t.Errorf("destroys = %q, want the sandbox destroyed after the failure", h.f.destroys)
	}
}

func TestRunTimedOut(t *testing.T) {
	h := newHarness(t)
	h.f.exitCode, h.f.timedOut = 137, true
	if code := h.run("run", "--timeout", "1s", "-c", "while True: pass"); code != 137 {
		t.Errorf("exit code %d, want 137", code)
	}
	if !strings.Contains(h.stderr.String(), "timed out after 1s") {
		t.Errorf("stderr = %q, want a timeout note", h.stderr)
	}
}

// A failed destroy is reported, but the exit code stays the program's.
func TestRunDestroyFails(t *testing.T) {
	h := newHarness(t)
	h.f.exitCode = 4
	h.f.destroyErr = status.Error(codes.Unavailable, "scheduler busy")
	if code := h.run("run", "-c", "print(1)"); code != 4 {
		t.Errorf("exit code %d, want 4", code)
	}
	if !strings.Contains(h.stderr.String(), "destroy sb-1: Unavailable: scheduler busy") {
		t.Errorf("stderr = %q", h.stderr)
	}
}

func TestRunSpecFlags(t *testing.T) {
	h := newHarness(t)
	code := h.run("run", "--cpu", "0.5", "--mem", "1Gi", "--allow", "1.1.1.1:443", "--allow", "example.com:80",
		"--group", "b", "--affinity", "k1", "--mode", "restore", "--ttl", "5m", "--timeout", "45s",
		"--", "python3", "-c", "print(2)")
	if code != 0 {
		t.Fatalf("exit code %d, stderr %q", code, h.stderr)
	}
	want := &gssv1.SandboxSpec{
		Resources:   &gssv1.Resources{CpuMillis: 500, MemoryBytes: 1 << 30},
		EgressAllow: []string{"1.1.1.1:443", "example.com:80"},
		NodeGroup:   "b", AffinityKey: "k1", Ttl: durationpb.New(5 * time.Minute),
		StartMode: gssv1.StartMode_START_MODE_RESTORE,
	}
	if len(h.f.creates) != 1 || !proto.Equal(h.f.creates[0], want) {
		t.Errorf("spec = %v\nwant   %v", h.f.creates, want)
	}
	ex := h.f.execs[0]
	if got := ex.GetArgv().GetArgs(); strings.Join(got, " ") != "python3 -c print(2)" || ex.GetTimeout().AsDuration() != 45*time.Second {
		t.Errorf("exec = %v", ex)
	}

	// The defaults: 500m, 256 MiB, 10 minutes, auto.
	if code := h.run("create"); code != 0 {
		t.Fatalf("create: exit code %d, stderr %q", code, h.stderr)
	}
	def := &gssv1.SandboxSpec{
		Resources: &gssv1.Resources{CpuMillis: 500, MemoryBytes: 256 << 20}, Ttl: durationpb.New(10 * time.Minute),
	}
	if !proto.Equal(h.f.creates[1], def) {
		t.Errorf("default spec = %v\nwant           %v", h.f.creates[1], def)
	}
	if h.stdout.String() != "sb-2\n" {
		t.Errorf("create printed %q, want the ID", h.stdout)
	}
}

func TestRunFile(t *testing.T) {
	h := newHarness(t)
	path := filepath.Join(t.TempDir(), "p.py")
	if err := os.WriteFile(path, []byte("print(1)"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := h.run("run", "-f", path); code != 0 || h.stdout.String() != "1\n" {
		t.Errorf("exit code %d, stdout %q, stderr %q", code, h.stdout, h.stderr)
	}
}

func TestUsageErrors(t *testing.T) {
	h := newHarness(t)
	big := filepath.Join(t.TempDir(), "big.py")
	if err := os.WriteFile(big, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(big, 16<<20+1); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "usage: gssctl"},
		{[]string{"frob"}, `unknown command "frob"`},
		{[]string{"run"}, "-c CODE, -f FILE or -- ARGV"},
		{[]string{"run", "-c", "x", "-f", "y"}, "only one of"},
		{[]string{"run", "-c", "x", "--", "ls"}, "only one of"},
		{[]string{"run", "-c", "x", "extra"}, `unexpected argument "extra"`},
		{[]string{"run", "--"}, "-c CODE, -f FILE or -- ARGV"},
		{[]string{"run", "--cpu", "abc", "-c", "x"}, "--cpu"},
		{[]string{"run", "--mem", "100Mi", "-c", "x"}, "192 MiB"},
		{[]string{"run", "--mode", "hot", "-c", "x"}, "--mode"},
		{[]string{"run", "--allow", "1.1.1.1", "-c", "x"}, "egress_allow"},
		{[]string{"run", "--timeout", "0s", "-c", "x"}, "--timeout"},
		{[]string{"run", "--timeout", "10m1s", "-c", "x"}, "at most 10m0s"},
		{[]string{"exec", "sb-1", "--timeout", "1h", "-c", "x"}, "at most 10m0s"},
		{[]string{"run", "-f", big}, "more than 16 MiB"},
		{[]string{"run", "--bogus"}, "flag provided but not defined"},
		{[]string{"exec"}, "sandbox ID"},
		{[]string{"exec", "-c", "x"}, "sandbox ID"},
		{[]string{"destroy"}, "sandbox ID"},
		{[]string{"destroy", "a", "b"}, "unexpected argument"},
		{[]string{"explain"}, "sandbox ID"},
		{[]string{"nodes", "x"}, "unexpected argument"},
		{[]string{"ps", "x"}, "unexpected argument"},
	} {
		if code := h.run(tc.args...); code != 2 {
			t.Errorf("%q: exit code %d, want 2", tc.args, code)
		}
		if !strings.Contains(h.stderr.String(), tc.want) {
			t.Errorf("%q: stderr = %q, want it to contain %q", tc.args, h.stderr, tc.want)
		}
	}
	if len(h.f.creates)+len(h.f.execs)+len(h.f.destroys) != 0 {
		t.Errorf("a bad command line reached the scheduler")
	}
	if code := h.run("-h"); code != 0 || !strings.Contains(h.stdout.String(), "usage: gssctl") || h.stderr.Len() != 0 {
		t.Errorf("-h: exit code %d, stdout %q, stderr %q", code, h.stdout, h.stderr)
	}
	if code := h.run("--nope", "ps"); code != 2 || !strings.Contains(h.stderr.String(), "usage: gssctl") {
		t.Errorf("an unknown global flag: exit code %d, stderr %q", code, h.stderr)
	}
}

func TestSchedulerAddress(t *testing.T) {
	h := newHarness(t)
	h.env["GSS_SCHEDULER"] = "10.0.0.1:7000"
	h.run("nodes")
	h.run("--scheduler", "10.0.0.2:7000", "nodes")
	if strings.Join(h.dialed, " ") != "10.0.0.1:7000 10.0.0.2:7000" {
		t.Errorf("dialled %q, want $GSS_SCHEDULER, then the flag over it", h.dialed)
	}
}

func TestExecDestroy(t *testing.T) {
	h := newHarness(t)
	h.f.exitCode = 5
	if code := h.run("exec", "sb-9", "-c", "print(1)"); code != 5 {
		t.Errorf("exec: exit code %d, want 5", code)
	}
	if h.stdout.String() != "1\n" || h.stderr.String() != "warning\n" {
		t.Errorf("exec: stdout %q, stderr %q (no summary line)", h.stdout, h.stderr)
	}
	if h.f.execs[0].GetSandboxId() != "sb-9" {
		t.Errorf("exec went to %q", h.f.execs[0].GetSandboxId())
	}
	if code := h.run("destroy", "sb-9"); code != 0 || len(h.f.destroys) != 1 || h.f.destroys[0] != "sb-9" {
		t.Errorf("destroy: exit code %d, destroys %q, stderr %q", code, h.f.destroys, h.stderr)
	}
	h.f.destroyErr = status.Error(codes.NotFound, "sandbox sb-8 not found")
	if code := h.run("destroy", "sb-8"); code != 1 || !strings.Contains(h.stderr.String(), "NotFound: sandbox sb-8 not found") {
		t.Errorf("destroy of a missing sandbox: exit code %d, stderr %q", code, h.stderr)
	}
}

func TestNodesTable(t *testing.T) {
	h := newHarness(t)
	h.f.nodes = []*gssv1.Node{
		{
			Info:  &gssv1.NodeInfo{Id: "b1", Group: "b", Address: "127.0.0.1:7103", Capacity: &gssv1.Resources{CpuMillis: 4000, MemoryBytes: 2048 << 20}, MaxSandboxes: 16},
			State: gssv1.NodeState_NODE_STATE_READY, Allocated: &gssv1.Resources{CpuMillis: 500, MemoryBytes: 256 << 20},
			Sandboxes: 1, WarmReady: 2, Health: 1, LastHeartbeat: timestamppb.New(now.Add(-400 * time.Millisecond)),
		},
		{
			Info:  &gssv1.NodeInfo{Id: "a1", Group: "a", Address: "127.0.0.1:7101", Capacity: &gssv1.Resources{CpuMillis: 4000, MemoryBytes: 2048 << 20}, MaxSandboxes: 16},
			State: gssv1.NodeState_NODE_STATE_DEAD, Health: 0.5, LastHeartbeat: timestamppb.New(now.Add(-5 * time.Second)),
		},
	}
	if code := h.run("nodes"); code != 0 {
		t.Fatalf("exit code %d, stderr %q", code, h.stderr)
	}
	want := "" +
		"NODE  GROUP  STATE  ADDRESS         CPU         MEM           SANDBOXES  WARM  HEALTH  HEARTBEAT\n" +
		"a1    a      DEAD   127.0.0.1:7101  0/4000m     0/2048Mi      0/16       0     0.50    5s ago\n" +
		"b1    b      READY  127.0.0.1:7103  500m/4000m  256Mi/2048Mi  1/16       2     1.00    400ms ago\n"
	if got := h.stdout.String(); got != want {
		t.Errorf("nodes printed\n%s\nwant\n%s", got, want)
	}
}

func TestPs(t *testing.T) {
	h := newHarness(t)
	h.f.sandboxes = []*gssv1.Sandbox{
		{
			Id: "sb-2", State: gssv1.SandboxState_SANDBOX_STATE_PENDING,
			Spec:      &gssv1.SandboxSpec{Resources: &gssv1.Resources{CpuMillis: 1000, MemoryBytes: 1 << 30}},
			CreatedAt: timestamppb.New(now.Add(-time.Second)),
		},
		{
			Id: "sb-1", State: gssv1.SandboxState_SANDBOX_STATE_READY, NodeId: "a1", NodeGroup: "a",
			StartedAs: gssv1.StartMode_START_MODE_RESTORE, StartLatency: durationpb.New(85 * time.Millisecond),
			Spec:      &gssv1.SandboxSpec{Resources: &gssv1.Resources{CpuMillis: 500, MemoryBytes: 256 << 20}},
			CreatedAt: timestamppb.New(now.Add(-time.Minute)), ExpiresAt: timestamppb.New(now.Add(9*time.Minute + 30*time.Second)),
		},
	}
	if code := h.run("ps"); code != 0 {
		t.Fatalf("exit code %d, stderr %q", code, h.stderr)
	}
	// Sorted by creation time; checked field by field.
	lines := strings.Split(strings.TrimSuffix(h.stdout.String(), "\n"), "\n")
	wantFields := [][]string{
		{"ID", "STATE", "NODE", "GROUP", "START", "READY", "CPU", "MEM", "EXPIRES"},
		{"sb-1", "READY", "a1", "a", "restore", "85ms", "500m", "256Mi", "in", "9m30s"},
		{"sb-2", "PENDING", "-", "-", "-", "-", "1000m", "1024Mi", "-"},
	}
	if len(lines) != len(wantFields) {
		t.Fatalf("ps printed %q", h.stdout)
	}
	for i, l := range lines {
		if got := strings.Fields(l); strings.Join(got, " ") != strings.Join(wantFields[i], " ") {
			t.Errorf("ps line %d = %q, want fields %q", i, l, wantFields[i])
		}
	}
}

func TestExplain(t *testing.T) {
	h := newHarness(t)
	h.f.sandboxes = []*gssv1.Sandbox{{
		Id: "sb-1", State: gssv1.SandboxState_SANDBOX_STATE_READY, NodeId: "a1", NodeGroup: "a",
		StartedAs: gssv1.StartMode_START_MODE_WARM, Incarnation: 1,
		Placement: &gssv1.Placement{NodesTotal: 4, NodesFeasible: 3, Top: []*gssv1.NodeScore{
			{NodeId: "a1", Total: 2.5, Plugins: map[string]float64{"BinPack": 1, "WarmPool": 0.5, "NodeHealth": 1, "GroupAffinity": 0}},
			{NodeId: "b1", Total: 1.75, Plugins: map[string]float64{"BinPack": 0.75, "WarmPool": 0, "NodeHealth": 1}},
		}},
	}, {Id: "sb-2", State: gssv1.SandboxState_SANDBOX_STATE_PENDING}}
	if code := h.run("explain", "sb-1"); code != 0 {
		t.Fatalf("exit code %d, stderr %q", code, h.stderr)
	}
	want := "" +
		"sandbox sb-1: READY on node a1 (group a), started as warm, incarnation 1\n" +
		"nodes considered: 4, feasible: 3\n" +
		"top candidates (* = the node it runs on):\n" +
		"   NODE  TOTAL  BinPack  GroupAffinity  NodeHealth  WarmPool\n" +
		"*  a1    2.500  1.000    0.000          1.000       0.500\n" +
		"   b1    1.750  0.750    -              1.000       0.000\n"
	if got := h.stdout.String(); got != want {
		t.Errorf("explain printed\n%s\nwant\n%s", got, want)
	}

	if code := h.run("explain", "sb-2"); code != 0 || !strings.Contains(h.stdout.String(), "no placement recorded") {
		t.Errorf("explain without a placement: exit code %d, stdout %q", code, h.stdout)
	}
	if code := h.run("explain", "sb-3"); code != 1 || !strings.Contains(h.stderr.String(), "NotFound") {
		t.Errorf("explain of a missing sandbox: exit code %d, stderr %q", code, h.stderr)
	}
}
