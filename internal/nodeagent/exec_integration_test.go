//go:build integration && linux

package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// execSandbox starts a manager on a new test node with one ready sandbox,
// sb-1 (incarnation 1), with spec s. The manager is closed at cleanup.
func execSandbox(t *testing.T, s *gssv1.SandboxSpec) (*realNode, *Manager) {
	t.Helper()
	n := newRealNode(t)
	m := n.manager(t)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := m.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := m.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: "sb-1", Incarnation: 1, Spec: s}); err != nil {
		t.Fatal(err)
	}
	return n, m
}

// realResult is what one Exec sent, with any break of the stream contract.
type realResult struct {
	stdout, stderr bytes.Buffer
	exit           *gssv1.ExitStatus
	broken         []string
	err            error
}

// runExec runs req and collects its events. onStdout, if set, is called with
// the stdout received so far after each stdout event.
func runExec(ctx context.Context, m *Manager, req *gssv1.ExecRequest, onStdout func(string)) *realResult {
	r := &realResult{}
	r.err = m.Exec(ctx, req, func(ev *gssv1.ExecEvent) error {
		if r.exit != nil {
			r.broken = append(r.broken, "event after the exit event")
		}
		switch {
		case ev.GetExit() != nil:
			r.exit = ev.GetExit()
		case ev.GetStdout() != nil:
			r.stdout.Write(ev.GetStdout())
			if len(ev.GetStdout()) > MaxChunk {
				r.broken = append(r.broken, fmt.Sprintf("stdout chunk of %d bytes", len(ev.GetStdout())))
			}
			if onStdout != nil {
				onStdout(r.stdout.String())
			}
		case ev.GetStderr() != nil:
			r.stderr.Write(ev.GetStderr())
			if len(ev.GetStderr()) > MaxChunk {
				r.broken = append(r.broken, fmt.Sprintf("stderr chunk of %d bytes", len(ev.GetStderr())))
			}
		default:
			r.broken = append(r.broken, "empty event")
		}
		return nil
	})
	return r
}

// ok fails the test unless the Exec returned nil with an exit event and
// kept the stream contract.
func (r *realResult) ok(t *testing.T, what string) *gssv1.ExitStatus {
	t.Helper()
	if r.err != nil {
		t.Fatalf("%s: %v (stdout %q, stderr %q)", what, r.err, r.stdout.String(), r.stderr.String())
	}
	if r.exit == nil {
		t.Fatalf("%s: no exit event", what)
	}
	if len(r.broken) > 0 {
		t.Errorf("%s: stream contract broken: %q", what, r.broken)
	}
	return r.exit
}

func withExecTimeout(req *gssv1.ExecRequest, d time.Duration) *gssv1.ExecRequest {
	req.Timeout = durationpb.New(d)
	return req
}

func TestRealExecPython(t *testing.T) {
	_, m := execSandbox(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	r := runExec(ctx, m, pyReq("sb-1", "import pandas as pd; print(pd.__version__)"), nil)
	if exit := r.ok(t, "python"); exit.GetCode() != 0 || r.stdout.String() != "3.1.1\n" {
		t.Errorf("exit %v, stdout %q, stderr %q; want 0 and \"3.1.1\\n\"", exit, r.stdout.String(), r.stderr.String())
	}
}

func TestRealExecArgv(t *testing.T) {
	_, m := execSandbox(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	r := runExec(ctx, m, argvReq("sb-1", "sh", "-c", "echo out; echo err >&2; exit 3"), nil)
	if exit := r.ok(t, "argv"); exit.GetCode() != 3 || r.stdout.String() != "out\n" || r.stderr.String() != "err\n" {
		t.Errorf("exit %v, stdout %q, stderr %q; want 3, \"out\\n\", \"err\\n\"", exit, r.stdout.String(), r.stderr.String())
	}
}

func TestRealExecStdinEnv(t *testing.T) {
	_, m := execSandbox(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	py := pyReq("sb-1", `import os, sys; print(sys.stdin.read() + os.environ["X"])`)
	py.Stdin, py.Env = []byte("hello "), map[string]string{"X": "world"}
	r := runExec(ctx, m, py, nil)
	if exit := r.ok(t, "python"); exit.GetCode() != 0 || r.stdout.String() != "hello world\n" {
		t.Errorf("python: exit %v, stdout %q, stderr %q", exit, r.stdout.String(), r.stderr.String())
	}

	argv := argvReq("sb-1", "sh", "-c", `cat; printf %s "$X"`)
	argv.Stdin, argv.Env = []byte("in:"), map[string]string{"X": "x"}
	r = runExec(ctx, m, argv, nil)
	if exit := r.ok(t, "argv"); exit.GetCode() != 0 || r.stdout.String() != "in:x" {
		t.Errorf("argv: exit %v, stdout %q, stderr %q", exit, r.stdout.String(), r.stderr.String())
	}
}

func TestRealExecLargeBinary(t *testing.T) {
	_, m := execSandbox(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// 5 MiB of bytes 0..255 on stdout and 1 MiB of 255..0 on stderr, written
	// in turns so both pipes are busy, with no trailing newline.
	code := `import sys
out = bytes(range(256)) * 20480
err = bytes(reversed(range(256))) * 4096
o, e = sys.stdout.buffer, sys.stderr.buffer
for k in range(80):
    o.write(out[k * 65536:(k + 1) * 65536])
    e.write(err[k * 13108:(k + 1) * 13108])
o.flush()
e.flush()
`
	r := runExec(ctx, m, pyReq("sb-1", code), nil)
	if exit := r.ok(t, "large output"); exit.GetCode() != 0 {
		t.Fatalf("exit %v, stderr tail %q", exit, tailOf(r.stderr.Bytes()))
	}
	var up, down []byte
	for i := 0; i < 256; i++ {
		up, down = append(up, byte(i)), append(down, byte(255-i))
	}
	wantOut, wantErr := bytes.Repeat(up, 20480), bytes.Repeat(down, 4096)
	if got, want := sha256.Sum256(r.stdout.Bytes()), sha256.Sum256(wantOut); got != want {
		t.Errorf("stdout: %d bytes, sha256 %x; want %d bytes, %x", r.stdout.Len(), got, len(wantOut), want)
	}
	if got, want := sha256.Sum256(r.stderr.Bytes()), sha256.Sum256(wantErr); got != want {
		t.Errorf("stderr: %d bytes, sha256 %x; want %d bytes, %x", r.stderr.Len(), got, len(wantErr), want)
	}
}

func tailOf(b []byte) []byte {
	if len(b) > 512 {
		return b[len(b)-512:]
	}
	return b
}

func TestRealExecTimeout(t *testing.T) {
	_, m := execSandbox(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for _, tc := range []struct {
		name string
		req  *gssv1.ExecRequest
	}{
		{"python", pyReq("sb-1", "while True: pass")},                // the zygote kills it
		{"argv", argvReq("sb-1", "sh", "-c", "while :; do :; done")}, // the agent kills it
	} {
		start := time.Now()
		r := runExec(ctx, m, withExecTimeout(tc.req, time.Second), nil)
		took := time.Since(start)
		exit := r.ok(t, tc.name)
		t.Logf("%s: %v after %v", tc.name, exit, took)
		if exit.GetCode() != 137 || !exit.GetTimedOut() || exit.GetOomKilled() {
			t.Errorf("%s: exit %v, want 137 timed_out", tc.name, exit)
		}
		if took > 4*time.Second {
			t.Errorf("%s: a 1s timeout took %v", tc.name, took)
		}
		r = runExec(ctx, m, pyReq("sb-1", "print(1)"), nil)
		if exit := r.ok(t, "print after "+tc.name); exit.GetCode() != 0 || r.stdout.String() != "1\n" {
			t.Errorf("after %s: exit %v, stdout %q", tc.name, exit, r.stdout.String())
		}
	}
}

// countProcs is a python_code Exec that prints the number of processes in
// the sandbox.
func countProcs(t *testing.T, ctx context.Context, m *Manager) int {
	t.Helper()
	r := runExec(ctx, m, pyReq("sb-1", `import os; print(sum(1 for d in os.listdir("/proc") if d.isdigit()))`), nil)
	if exit := r.ok(t, "count processes"); exit.GetCode() != 0 {
		t.Fatalf("count processes: exit %v, stderr %q", exit, r.stderr.String())
	}
	n, err := strconv.Atoi(strings.TrimSpace(r.stdout.String()))
	if err != nil {
		t.Fatalf("count processes printed %q", r.stdout.String())
	}
	return n
}

// startedExec runs req, which prints "started" first, in the background. It
// returns once "started" arrived, and a channel with the Exec's result.
func startedExec(t *testing.T, ctx context.Context, m *Manager, req *gssv1.ExecRequest) <-chan *realResult {
	t.Helper()
	started := make(chan struct{})
	var once sync.Once
	res := make(chan *realResult, 1)
	go func() {
		res <- runExec(ctx, m, req, func(out string) {
			if strings.Contains(out, "started") {
				once.Do(func() { close(started) })
			}
		})
	}()
	select {
	case <-started:
	case r := <-res:
		t.Fatalf("Exec ended before it started: err %v, exit %v, stderr %q", r.err, r.exit, r.stderr.String())
	case <-time.After(30 * time.Second):
		t.Fatal("Exec did not start")
	}
	return res
}

// pgrep returns "pid cmdline" for each process whose command line matches
// pattern.
func pgrep(t *testing.T, pattern string) []string {
	t.Helper()
	out, err := exec.Command("pgrep", "-a", "-f", "--", pattern).Output()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return nil // no match
	}
	if err != nil {
		t.Fatalf("pgrep -f %q: %v", pattern, err)
	}
	return strings.Split(strings.TrimSpace(string(out)), "\n")
}

func TestRealExecCancel(t *testing.T) {
	n, m := execSandbox(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	before := countProcs(t, ctx, m)

	ectx, ecancel := context.WithCancel(ctx)
	defer ecancel()
	res := startedExec(t, ectx, m, pyReq("sb-1", `import time; print("started", flush=True); time.sleep(60)`))
	// The pattern finds this root's runsc exec client while the Exec runs,
	// so "nothing found" afterwards is not vacuous.
	pattern := "runsc --root " + regexp.QuoteMeta(n.rt.Root) + " .* exec "
	if clients := pgrep(t, pattern); len(clients) != 1 {
		t.Errorf("pgrep -f %q during the Exec = %q, want the one runsc exec client", pattern, clients)
	}
	time.Sleep(300 * time.Millisecond)
	ecancel()
	start := time.Now()
	var r *realResult
	select {
	case r = <-res:
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled Exec did not return")
	}
	took := time.Since(start)
	t.Logf("cancelled Exec returned after %v: %v", took, r.err)
	if status.Code(r.err) != codes.Canceled {
		t.Errorf("cancelled Exec = %v, want Canceled", r.err)
	}
	if took > 3*time.Second {
		t.Errorf("cancelled Exec took %v to return", took)
	}
	if r.exit != nil {
		t.Errorf("cancelled Exec sent exit %v", r.exit)
	}

	// No runsc exec client is left for this root.
	if clients := pgrep(t, pattern); len(clients) != 0 {
		t.Errorf("pgrep -f %q after the cancel = %q, want no process", pattern, clients)
	}

	// The worker is gone: the sandbox holds as many processes as before
	// (the supervisor reaps it shortly after pyfork is killed).
	deadline := time.Now().Add(3 * time.Second)
	after := countProcs(t, ctx, m)
	for after != before && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		after = countProcs(t, ctx, m)
	}
	t.Logf("processes in the sandbox: %d before, %d after", before, after)
	if after != before {
		t.Errorf("%d processes after the cancelled Exec, %d before", after, before)
	}
}

func TestRealExecMemoryLimit(t *testing.T) {
	// The smallest sandbox (D31): while the zygote imports numpy and
	// pandas the sandbox's memory.peak reaches about 135 MiB, so a 128 MiB
	// sandbox is OOM-killed before it is ready. Idle after priming it uses
	// about 60 MiB.
	_, m := execSandbox(t, &gssv1.SandboxSpec{Resources: &gssv1.Resources{CpuMillis: 500, MemoryBytes: 192 << 20}})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	code := `blocks = []
for i in range(64):
    b = bytearray(16 << 20)
    for j in range(0, len(b), 4096):
        b[j] = 1
    blocks.append(b)
    print(i, flush=True)
print("allocated all", flush=True)
`
	r := runExec(ctx, m, pyReq("sb-1", code), nil)
	if strings.Contains(r.stdout.String(), "allocated all") {
		t.Fatalf("1 GiB allocated in a 192 MiB sandbox: exit %v", r.exit)
	}
	lines := strings.Fields(r.stdout.String())
	blocks := "none"
	if len(lines) > 0 {
		blocks = lines[len(lines)-1]
	}
	if r.err == nil {
		exit := r.ok(t, "allocate")
		t.Logf("exit %v after block %s; stderr %q", exit, blocks, tailOf(r.stderr.Bytes()))
		if exit.GetCode() != 137 || !exit.GetOomKilled() {
			t.Fatalf("exit %v, want 137 with oom_killed", exit)
		}
		// The OOM killer took the process; the sandbox may have survived or not.
		after := runExec(ctx, m, pyReq("sb-1", "print(1)"), nil)
		switch {
		case after.err == nil:
			t.Logf("the sandbox still runs code: exit %v, stdout %q", after.exit, after.stdout.String())
		case status.Code(after.err) == codes.NotFound:
			t.Logf("the sandbox is gone: %v", after.err)
		default:
			t.Errorf("Exec after the OOM kill = %v, want success or NotFound", after.err)
		}
		return
	}
	// The OOM killer took the sentry: the sandbox is gone.
	t.Logf("Exec = %v after block %s", r.err, blocks)
	if status.Code(r.err) != codes.Unavailable {
		t.Fatalf("Exec = %v, want an exit 137 with oom_killed, or Unavailable", r.err)
	}
	next := runExec(ctx, m, pyReq("sb-1", "print(1)"), nil)
	if status.Code(next.err) != codes.NotFound {
		t.Errorf("Exec after the sandbox died = %v, want NotFound", next.err)
	}
}

// cpuUsage reads usage_usec from a cgroup's cpu.stat.
func cpuUsage(t *testing.T, dir string) time.Duration {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "cpu.stat"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if f := strings.Fields(line); len(f) == 2 && f[0] == "usage_usec" {
			us, err := strconv.ParseInt(f[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return time.Duration(us) * time.Microsecond
		}
	}
	t.Fatalf("no usage_usec in %s/cpu.stat", dir)
	return 0
}

func TestRealExecCPULimit(t *testing.T) {
	n, m := execSandbox(t, &gssv1.SandboxSpec{Resources: &gssv1.Resources{CpuMillis: 200, MemoryBytes: 256 << 20}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	code := `import time
end = time.monotonic() + 2
while time.monotonic() < end:
    pass
print(time.process_time())
`
	cg := n.cgroupDir("sb-1_1_")
	usedBefore, began := cpuUsage(t, cg), time.Now()
	r := runExec(ctx, m, pyReq("sb-1", code), nil)
	used, window := cpuUsage(t, cg)-usedBefore, time.Since(began)
	if exit := r.ok(t, "busy loop"); exit.GetCode() != 0 {
		t.Fatalf("exit %v, stderr %q", exit, r.stderr.String())
	}
	cpu, err := strconv.ParseFloat(strings.TrimSpace(r.stdout.String()), 64)
	if err != nil {
		t.Fatalf("stdout %q", r.stdout.String())
	}
	cpus := used.Seconds() / window.Seconds()
	t.Logf("2 s busy loop at 200m: process_time %.3f s in the sandbox; the cgroup used %v in %v (%.2f CPUs)",
		cpu, used, window, cpus)
	// Spec D30: gVisor's in-sandbox CPU time (process_time) counts the time
	// the host throttled the sandbox, so it is only logged. The limit is
	// checked where it is enforced, in the host cgroup's cpu.stat.
	if cpus > 0.30 {
		t.Errorf("the sandbox cgroup used %v of CPU in %v (%.2f CPUs), want at most 0.30 at 200m", used, window, cpus)
	}
}

func TestRealExecStatePersists(t *testing.T) {
	_, m := execSandbox(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	r := runExec(ctx, m, pyReq("sb-1", `open("/work/f", "w").write("kept")`), nil)
	if exit := r.ok(t, "write"); exit.GetCode() != 0 {
		t.Fatalf("write: exit %v, stderr %q", exit, r.stderr.String())
	}
	r = runExec(ctx, m, argvReq("sb-1", "cat", "/work/f"), nil)
	if exit := r.ok(t, "read"); exit.GetCode() != 0 || r.stdout.String() != "kept" {
		t.Errorf("read: exit %v, stdout %q, stderr %q", exit, r.stdout.String(), r.stderr.String())
	}
}

func TestRealDestroyDuringExec(t *testing.T) {
	n, m := execSandbox(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := startedExec(t, ctx, m, pyReq("sb-1", `import time; print("started", flush=True); time.sleep(60)`))

	start := time.Now()
	if err := m.Destroy(ctx, "sb-1", 0); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	took := time.Since(start)
	t.Logf("Destroy during an Exec took %v", took)
	if took > 5*time.Second {
		t.Errorf("Destroy took %v", took)
	}
	select {
	case r := <-res:
		if status.Code(r.err) != codes.Unavailable {
			t.Errorf("Exec = %v (exit %v), want Unavailable", r.err, r.exit)
		}
		t.Logf("Exec ended after %v: %v", time.Since(start), r.err)
	case <-time.After(10 * time.Second):
		t.Fatal("Exec did not end after Destroy")
	}
	rid := "sb-1_1_"
	if _, err := os.Stat(filepath.Join(n.state, "bundles", rid)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("bundle dir after Destroy: %v", err)
	}
	if _, err := os.Stat(n.cgroupDir(rid)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("cgroup dir after Destroy: %v", err)
	}
	if _, err := os.Stat(filepath.Join(n.state, "execs", rid)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("exec pid dir after Destroy: %v", err)
	}
	if got := n.listContainers(t); len(got) != 0 {
		t.Errorf("runsc list after Destroy = %q", got)
	}
}
