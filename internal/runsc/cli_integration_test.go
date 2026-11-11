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
	"testing"
	"time"

	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/testenv"
)

// testBundle writes a minimal bundle into a new directory under state: the
// output of `runsc spec -- python3 /gss/zygote.py`, patched for the shared
// read-only rootfs (Task 4's bundle.Write does this properly).
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
