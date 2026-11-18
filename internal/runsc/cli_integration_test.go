//go:build integration && linux

package runsc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/testenv"
	"golang.org/x/sys/unix"
)

// testBundle writes a minimal bundle into a new directory under state: the
// output of `runsc spec -- python3 /gss/zygote.py`, patched for the shared
// read-only rootfs (bundle.Write does this properly).
func testBundle(t *testing.T, runscBin, state, name string) string {
	t.Helper()
	dir := filepath.Join(state, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(runscBin, "spec", "--", "python3", "/gss/zygote.py")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("runsc spec: %v: %s", err, out)
	}
	path := filepath.Join(dir, "config.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	cfg["root"] = map[string]any{"path": testenv.Rootfs(t), "readonly": true}
	proc := cfg["process"].(map[string]any)
	proc["terminal"] = false
	proc["user"] = map[string]any{"uid": 1000, "gid": 1000}
	proc["cwd"] = "/work"
	proc["env"] = []string{"PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME=/work", "GSS_PRIME=json"}
	tmpfs := func(dest string) map[string]any {
		return map[string]any{"destination": dest, "type": "tmpfs", "source": "tmpfs",
			"options": []string{"nosuid", "nodev", "mode=1777", "size=67108864"}}
	}
	cfg["mounts"] = append(cfg["mounts"].([]any), tmpfs("/tmp"), tmpfs("/work"))
	if raw, err = json.MarshalIndent(cfg, "", "  "); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// testCLI returns a CLI with its own root under /run/gss, removed at cleanup.
func testCLI(t *testing.T) (*CLI, string) {
	t.Helper()
	testenv.RequireRoot(t)
	bin := testenv.RunscPath(t)
	testenv.Rootfs(t)
	if err := os.MkdirAll("/run/gss", 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("/run/gss", "runsc-test-")
	if err != nil {
		t.Fatal(err)
	}
	c := NewCLI(bin, root)
	t.Cleanup(func() {
		if err := c.RemoveRoot(context.Background()); err != nil {
			t.Errorf("cleanup RemoveRoot: %v", err)
		}
	})
	return c, testenv.TempStateDir(t)
}

// startContainer creates and starts a container and deletes it at cleanup.
func startContainer(t *testing.T, ctx context.Context, c *CLI, ref Ref, bundle, logPath string) {
	t.Helper()
	t.Cleanup(func() {
		if err := c.Delete(context.Background(), ref); err != nil {
			t.Errorf("cleanup Delete %s: %v", ref.ID, err)
		}
	})
	if err := c.Create(ctx, ref, bundle, logPath); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := c.Start(ctx, ref); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

// pyfork runs code through /gss/pyfork and returns stdout and the exit code.
func pyfork(t *testing.T, ctx context.Context, c *CLI, ref Ref, code string) (string, int) {
	t.Helper()
	header := fmt.Sprintf(`{"code_len":%d,"timeout_s":0,"env":{}}`, len(code))
	var stdout, stderr bytes.Buffer
	exit, err := c.Exec(ctx, ref, ExecSpec{
		Args:   []string{"/gss/pyfork"},
		Stdin:  strings.NewReader(header + "\n" + code),
		Stdout: &stdout, Stderr: &stderr,
	})
	if err != nil {
		t.Fatalf("pyfork Exec: %v (stderr %q)", err, stderr.String())
	}
	if exit != 0 {
		t.Logf("pyfork exit %d, stderr %q", exit, stderr.String())
	}
	return stdout.String(), exit
}

func TestCLILifecycle(t *testing.T) {
	c, state := testCLI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	v, err := c.Version(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^release-\d{8}`).MatchString(v) {
		t.Errorf("Version = %q", v)
	}

	ref := Ref{ID: fmt.Sprintf("life-%d", os.Getpid()), Network: NetworkNone}
	bundle := testBundle(t, c.Binary, state, "bundle")
	startContainer(t, ctx, c, ref, bundle, filepath.Join(state, "container.log"))

	st, err := c.State(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if st.ID != ref.ID || st.Status != "running" || st.PID <= 0 {
		t.Errorf("State = %+v, want running with PID > 0", st)
	}

	var out, errOut bytes.Buffer
	code, err := c.Exec(ctx, ref, ExecSpec{Args: []string{"python3", "-c", "print('hi')"}, Stdout: &out, Stderr: &errOut})
	if err != nil || code != 0 || out.String() != "hi\n" {
		t.Errorf("Exec print = %d, %v, stdout %q, stderr %q", code, err, out.String(), errOut.String())
	}

	code, err = c.Exec(ctx, ref, ExecSpec{Args: []string{"python3", "-c", "import sys; sys.exit(3)"}})
	if err != nil || code != 3 {
		t.Errorf("Exec exit 3 = %d, %v", code, err)
	}

	pidFile := filepath.Join(state, "exec.pid")
	code, err = c.Exec(ctx, ref, ExecSpec{Args: []string{"true"}, InternalPIDFile: pidFile})
	if err != nil || code != 0 {
		t.Errorf("Exec with pid file = %d, %v", code, err)
	}
	raw, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("pid file: %v", err)
	}
	if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err != nil || pid <= 0 {
		t.Errorf("pid file holds %q", raw)
	}

	var missErr bytes.Buffer
	if code, err := c.Exec(ctx, ref, ExecSpec{Args: []string{"/no/such/binary"}, Stderr: &missErr}); err == nil ||
		!strings.Contains(err.Error(), "/no/such/binary") || missErr.Len() != 0 {
		t.Errorf("Exec of a missing binary = %d, %v; stderr forwarded %q", code, err, missErr.String())
	}
	missErr.Reset()
	bigArg := strings.Repeat("z", 20<<10)
	if code, err := c.Exec(ctx, ref, ExecSpec{Args: []string{"/no/such/binary", bigArg}, Stderr: &missErr}); err == nil ||
		!strings.Contains(err.Error(), "/no/such/binary") || missErr.Len() != 0 {
		t.Errorf("Exec of a missing binary with a 20 KiB argument = %d, %v; %d bytes forwarded to Stderr",
			code, err != nil, missErr.Len())
	}
	if code, err := c.Exec(ctx, ref, ExecSpec{Args: []string{"true"}, InternalPIDFile: "/nonexistent-gss-dir/x.pid"}); err == nil || code != -1 {
		t.Errorf("Exec with an unwritable InternalPIDFile = %d, %v; want -1 and an error", code, err)
	}
	if code, err := c.Exec(ctx, ref, ExecSpec{Args: []string{"sh", "-c", "exit 128"}}); err != nil || code != 128 {
		t.Errorf("Exec exit 128 = %d, %v; want 128, nil", code, err)
	}

	// A deadline kills the runsc client only; the process is killed by pid.
	sleepPID := filepath.Join(state, "sleep.pid")
	dctx, dcancel := context.WithTimeout(ctx, time.Second)
	began := time.Now()
	code, err = c.Exec(dctx, ref, ExecSpec{Args: []string{"sleep", "30"}, InternalPIDFile: sleepPID})
	dcancel()
	if !errors.Is(err, context.DeadlineExceeded) || code != -1 {
		t.Errorf("Exec sleep with 1 s deadline = %d, %v; want -1, deadline exceeded", code, err)
	}
	if d := time.Since(began); d > 5*time.Second {
		t.Errorf("Exec returned %v after the deadline", d)
	}
	raw, err = os.ReadFile(sleepPID)
	if err != nil {
		t.Fatalf("sleep pid file: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 0 {
		t.Fatalf("sleep pid file holds %q", raw)
	}
	if err := c.KillProcess(ctx, ref, pid); err != nil {
		t.Errorf("KillProcess(%d): %v", pid, err)
	}
	if err := c.KillProcess(ctx, ref, pid); err == nil {
		t.Errorf("second KillProcess(%d): want error", pid)
	}
	out.Reset()
	if code, err := c.Exec(ctx, ref, ExecSpec{Args: []string{"echo", "after"}, Stdout: &out}); err != nil || code != 0 || out.String() != "after\n" {
		t.Errorf("Exec after the kill = %d, %v, %q", code, err, out.String())
	}

	ids, err := c.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(ids, ref.ID) {
		t.Errorf("List = %q, missing %s", ids, ref.ID)
	}

	if err := c.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := c.State(ctx, ref); !errors.Is(err, ErrNotFound) {
		t.Errorf("State after Delete err = %v, want ErrNotFound", err)
	}
	if _, err := c.Exec(ctx, ref, ExecSpec{Args: []string{"true"}}); !errors.Is(err, ErrNotFound) {
		t.Errorf("Exec after Delete err = %v, want ErrNotFound", err)
	}
	if err := c.Delete(ctx, ref); err != nil {
		t.Errorf("second Delete = %v, want nil", err)
	}

	if err := c.RemoveRoot(ctx); err != nil {
		t.Fatalf("RemoveRoot: %v", err)
	}
	if _, err := os.Stat(c.Root); !os.IsNotExist(err) {
		t.Errorf("root %s still there (stat err %v)", c.Root, err)
	}
}

func TestCLICheckpointRestore(t *testing.T) {
	c, state := testCLI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	a := Ref{ID: fmt.Sprintf("ckpt-a-%d", os.Getpid()), Network: NetworkNone}
	bundleA := testBundle(t, c.Binary, state, "bundle-a")
	startContainer(t, ctx, c, a, bundleA, filepath.Join(state, "a.log"))

	if _, code := pyfork(t, ctx, c, a, "pass"); code != 0 {
		t.Fatalf("no-op pyfork exit %d", code)
	}

	image := filepath.Join(state, "image")
	if err := c.Checkpoint(ctx, a, image); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if _, err := os.Stat(filepath.Join(image, "checkpoint.img")); err != nil {
		t.Errorf("image: %v", err)
	}
	if st, err := c.State(ctx, a); err != nil || st.Status != "stopped" {
		t.Errorf("State after Checkpoint = %+v, %v; want stopped", st, err)
	}
	if _, err := c.Exec(ctx, a, ExecSpec{Args: []string{"true"}}); !errors.Is(err, ErrNotRunning) {
		t.Errorf("Exec after Checkpoint err = %v, want ErrNotRunning", err)
	}
	if err := c.Delete(ctx, a); err != nil {
		t.Fatalf("Delete A: %v", err)
	}

	// B gets the same config.json in a new directory: restore refuses a
	// bundle whose process.args differ from the checkpointed ones.
	b := Ref{ID: fmt.Sprintf("ckpt-b-%d", os.Getpid()), Network: NetworkNone}
	bundleB := filepath.Join(state, "bundle-b")
	if err := os.MkdirAll(bundleB, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg, err := os.ReadFile(filepath.Join(bundleA, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundleB, "config.json"), cfg, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Delete(context.Background(), b); err != nil {
			t.Errorf("cleanup Delete %s: %v", b.ID, err)
		}
	})
	if err := c.Create(ctx, b, bundleB, filepath.Join(state, "b.log")); err != nil {
		t.Fatalf("Create B: %v", err)
	}
	if err := c.Restore(ctx, b, bundleB, image); err != nil {
		t.Fatalf("Restore B: %v", err)
	}
	if st, err := c.State(ctx, b); err != nil || st.Status != "running" {
		t.Errorf("State after Restore = %+v, %v; want running", st, err)
	}

	out, code := pyfork(t, ctx, c, b, "print(6*7)")
	if code != 0 || out != "42\n" {
		t.Errorf("pyfork in B = %q, exit %d; want \"42\\n\", 0", out, code)
	}
}

// signalWriter closes ready the first time the output contains marker.
type signalWriter struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	marker string
	ready  chan struct{}
	done   bool
}

func (w *signalWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf.Write(p)
	if !w.done && strings.Contains(w.buf.String(), w.marker) {
		w.done = true
		close(w.ready)
	}
	return len(p), nil
}

func TestCLIExecSandboxDies(t *testing.T) {
	c, state := testCLI(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	bundle := testBundle(t, c.Binary, state, "bundle")

	// Each kill returns the PID it killed, or 0 when it killed none itself.
	sigkill := func(ref Ref) (int, error) {
		st, err := c.State(ctx, ref)
		if err != nil {
			return 0, err
		}
		return st.PID, syscall.Kill(st.PID, syscall.SIGKILL)
	}
	del := func(ref Ref) (int, error) { return 0, c.Delete(ctx, ref) }
	small := []string{"sh", "-c", "echo started >&2; sleep 30"}
	// With 1.5 GiB in the sandbox, runsc state can still say "running" just
	// after runsc exec saw the control connection close: it reports a
	// sandbox running until every thread of its process has exited.
	large := []string{"python3", "-c", "import sys, time\nb = bytes(range(256)) * (6 << 20)\nsys.stderr.write('started\\n'); sys.stderr.flush(); time.sleep(30)"}
	cases := []struct {
		name string
		kill func(ref Ref) (int, error)
		args []string
	}{
		{"sigkill", sigkill, small},
		{"sigkill-large", sigkill, large},
		{"delete", del, small},
	}
	for _, tc := range cases {
		name, kill := tc.name, tc.kill
		t.Run(name, func(t *testing.T) {
			ref := Ref{ID: fmt.Sprintf("dies-%s-%d", name, os.Getpid()), Network: NetworkNone}
			startContainer(t, ctx, c, ref, bundle, filepath.Join(state, name+".log"))
			errOut := &signalWriter{marker: "started", ready: make(chan struct{})}
			type result struct {
				code int
				err  error
			}
			res := make(chan result, 1)
			go func() {
				code, err := c.Exec(ctx, ref, ExecSpec{Args: tc.args, Stderr: errOut})
				res <- result{code, err}
			}()
			select {
			case <-errOut.ready:
			case <-time.After(30 * time.Second):
				t.Fatal("exec did not start")
			}
			pid, err := kill(ref)
			if err != nil {
				t.Fatalf("kill: %v", err)
			}
			watch := watchKilled(c, ref, pid)
			defer watch.stop()
			select {
			case r := <-res:
				// Delete first kills every process in the container, so
				// runsc exec may see the process die of SIGKILL (137)
				// before the sandbox goes: that exit was seen.
				killedFirst := name == "delete" && r.code == 137 && r.err == nil
				if !killedFirst && (r.code != -1 || !errors.Is(r.err, ErrNotRunning)) {
					t.Errorf("Exec = %d, %v; want -1, ErrNotRunning", r.code, r.err)
					watch.report(t)
				}
				t.Logf("Exec = %d, %v", r.code, r.err)
			case <-time.After(20 * time.Second):
				t.Fatal("Exec did not return after the sandbox died")
			}
		})
	}
}

// killWatch samples, from just after a sandbox process was killed, what
// runsc state and /proc show of it: once right away, then every 100 ms for
// 3 s. A test calls report when the sandbox was still reported running
// after the kill.
type killWatch struct {
	pid   int
	start time.Time
	quit  chan struct{}
	done  chan struct{}
	lines []string // written by the sampling goroutine until done is closed
}

// watchKilled starts sampling pid, the sandbox process of ref that was just
// killed. For pid 0 it returns nil, on which stop and report do nothing.
func watchKilled(c *CLI, ref Ref, pid int) *killWatch {
	if pid == 0 {
		return nil
	}
	w := &killWatch{pid: pid, start: time.Now(), quit: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			w.lines = append(w.lines, w.sample(c, ref))
			if time.Since(w.start) >= 3*time.Second {
				return
			}
			select {
			case <-w.quit:
				return
			case <-tick.C:
			}
		}
	}()
	return w
}

// sample is one line: the time since the kill, what runsc state says, and
// what /proc says of the killed process.
func (w *killWatch) sample(c *CLI, ref Ref) string {
	at := time.Since(w.start).Round(time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), execStateTimeout)
	defer cancel()
	state := ""
	if st, err := c.State(ctx, ref); err != nil {
		state = "failed: " + err.Error()
	} else {
		state = fmt.Sprintf("%s, pid %d", st.Status, st.PID)
	}
	return fmt.Sprintf("+%v: runsc state %s; %s", at, state, procState(w.pid))
}

// stop ends the sampling and waits for it.
func (w *killWatch) stop() {
	if w == nil {
		return
	}
	select {
	case <-w.quit:
	default:
		close(w.quit)
	}
	<-w.done
}

// report waits for the sampling to end and logs it, after the test
// process's place in the process tree: its PID and parent, whether it is a
// child subreaper (a killed sandbox it adopted stays a zombie until it reaps
// it), and the command name of PID 1.
func (w *killWatch) report(t *testing.T) {
	t.Helper()
	if w == nil {
		return
	}
	<-w.done
	t.Logf("killed sandbox pid %d", w.pid)
	t.Logf("test process pid %d, ppid %d, child subreaper %s; pid 1 is %s", os.Getpid(), os.Getppid(), childSubreaper(), procComm(1))
	for _, l := range w.lines {
		t.Logf("  %s", l)
	}
}

// procState is the state letter and parent of pid from /proc/<pid>/stat,
// or "gone", and whether /proc/<pid> exists.
func procState(pid int) string {
	dir := fmt.Sprintf("/proc/%d", pid)
	_, err := os.Stat(dir)
	exists := err == nil
	raw, err := os.ReadFile(filepath.Join(dir, "stat"))
	var s string
	switch {
	case errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH):
		s = "gone"
	case err != nil:
		s = "stat: " + err.Error()
	default:
		// The command name is in parentheses and may hold spaces; the
		// fields after it start with the state and the parent PID.
		var state string
		var ppid int
		if _, err := fmt.Sscan(string(raw[bytes.LastIndexByte(raw, ')')+1:]), &state, &ppid); err != nil {
			s = fmt.Sprintf("stat %q: %v", raw, err)
		} else {
			s = fmt.Sprintf("state %s, ppid %d (%s)", state, ppid, procComm(ppid))
		}
	}
	return fmt.Sprintf("%s: %s, exists %t", dir, s, exists)
}

// procComm is the command name of pid, or the error reading it.
func procComm(pid int) string {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
	if err != nil {
		return err.Error()
	}
	return strings.TrimSpace(string(raw))
}

// childSubreaper is PR_GET_CHILD_SUBREAPER of this process: "1" if
// orphaned descendants are reparented to it rather than to PID 1. The
// kernel writes the flag through the pointer in arg2 and returns 0, so
// PrctlRetInt cannot give it. The pointer is converted to uintptr in the
// unix.Syscall call itself, the form the unsafe rules allow; passed through
// unix.Prctl, a Go function, the variable could move before the kernel
// writes it.
func childSubreaper() string {
	var v int32
	if _, _, errno := unix.Syscall(unix.SYS_PRCTL, unix.PR_GET_CHILD_SUBREAPER, uintptr(unsafe.Pointer(&v)), 0); errno != 0 {
		return "unknown: " + errno.Error()
	}
	return strconv.Itoa(int(v))
}
