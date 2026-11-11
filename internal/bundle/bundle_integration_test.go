//go:build integration && linux

package bundle_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bundle"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/testenv"
)

const probe = `
import os, socket

def raises(f):
    try:
        f()
    except OSError:
        return True
    return False

def write(path, mode="w"):
    with open(path, mode) as f:
        f.write("x")

print("uid1000", os.getuid() == 1000)
print("cwd", os.getcwd() == "/work")
write("/work/a"); write("/tmp/b")
print("tmpfs-writes", True)
print("usr-ro", raises(lambda: write("/usr/x")))
print("hosts-ro", raises(lambda: write("/etc/hosts", "a")))
def conn():
    s = socket.socket()
    s.settimeout(2)
    s.connect(("1.1.1.1", 53))
print("no-net", raises(conn))
`

func TestBundleInSandbox(t *testing.T) {
	testenv.RequireRoot(t)
	bin := testenv.RunscPath(t)
	rootfs := testenv.Rootfs(t)
	state := testenv.TempStateDir(t)
	if err := os.MkdirAll("/run/gss", 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("/run/gss", "bundle-test-")
	if err != nil {
		t.Fatal(err)
	}
	c := runsc.NewCLI(bin, root)
	t.Cleanup(func() {
		if err := c.RemoveRoot(context.Background()); err != nil {
			t.Errorf("cleanup RemoveRoot: %v", err)
		}
	})

	node := fmt.Sprintf("bt-%d", os.Getpid())
	id := "sb1"
	ref := runsc.Ref{ID: id, Network: runsc.NetworkNone}
	cg := filepath.Join("/sys/fs/cgroup", bundle.CgroupPath(node, id))
	t.Cleanup(func() {
		// The container is deleted first (registered below, runs before this).
		os.Remove(cg)
		os.Remove(filepath.Dir(cg))
	})

	o := bundle.Options{
		ID: id, Dir: filepath.Join(state, "bundles", id), RootFS: rootfs, NodeID: node,
		CPUMillis: 500, MemoryBytes: 256 << 20, Prime: "json",
	}
	if err := bundle.Write(o); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	t.Cleanup(func() {
		if err := c.Delete(context.Background(), ref); err != nil {
			t.Errorf("cleanup Delete: %v", err)
		}
	})
	if err := c.Create(ctx, ref, o.Dir, filepath.Join(state, "container.log")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := c.Start(ctx, ref); err != nil {
		t.Fatalf("Start: %v", err)
	}

	header := fmt.Sprintf(`{"code_len":%d,"timeout_s":0,"env":{}}`, len(probe))
	var stdout, stderr bytes.Buffer
	exit, err := c.Exec(ctx, ref, runsc.ExecSpec{
		Args:   []string{"/gss/pyfork"},
		Stdin:  strings.NewReader(header + "\n" + probe),
		Stdout: &stdout, Stderr: &stderr,
	})
	if err != nil || exit != 0 {
		t.Fatalf("pyfork exit %d, err %v, stdout %q, stderr %q", exit, err, stdout.String(), stderr.String())
	}
	want := "uid1000 True\ncwd True\ntmpfs-writes True\nusr-ro True\nhosts-ro True\nno-net True\n"
	if stdout.String() != want {
		t.Errorf("probe output:\n%s\nwant:\n%s\nstderr: %s", stdout.String(), want, stderr.String())
	}

	for file, want := range map[string]string{"cpu.max": "50000 100000", "memory.max": "268435456"} {
		raw, err := os.ReadFile(filepath.Join(cg, file))
		if err != nil {
			t.Errorf("%s: %v", file, err)
			continue
		}
		if got := strings.TrimSpace(string(raw)); got != want {
			t.Errorf("%s = %q, want %q", file, got, want)
		}
	}
}
