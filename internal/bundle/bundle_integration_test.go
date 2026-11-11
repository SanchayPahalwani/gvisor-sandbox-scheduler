//go:build integration && linux

package bundle_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
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
import errno, os, socket

def err(f):
    try:
        f()
    except OSError as e:
        return e.errno
    return 0

def write(path, mode="w"):
    with open(path, mode) as f:
        f.write("x")

def fill(path):
    chunk = b"x" * (1 << 20)
    n = 0
    try:
        with open(path, "wb") as f:
            while True:
                f.write(chunk)
                f.flush()
                n += 1
    except OSError as e:
        return e.errno, n

def status(key):
    for line in open("/proc/self/status"):
        if line.startswith(key + ":"):
            return line.split()[1]

def conn():
    s = socket.socket()
    s.settimeout(2)
    s.connect(("1.1.1.1", 53))

print("uid1000", os.getuid() == 1000)
print("cwd", os.getcwd() == "/work")
write("/work/a"); write("/tmp/b")
print("tmpfs-writes", True)
print("var-tmp-erofs", err(lambda: write("/var/tmp/x")) == errno.EROFS)
print("home-erofs", err(lambda: write("/home/sandbox/x")) == errno.EROFS)
print("usr-fails", err(lambda: write("/usr/x")) != 0)
print("hosts-fails", err(lambda: write("/etc/hosts", "a")) != 0)
mi = [l.split() for l in open("/proc/self/mountinfo") if l.split()[4] == "/etc/hosts"]
print("hosts-mount-ro", len(mi) == 1 and "ro" in mi[0][5].split(","))
print("hosts-content", open("/etc/hosts").read() == "127.0.0.1 localhost\n10.1.2.3 api.example\n")
print("dev-fails", err(lambda: write("/dev/x")) != 0)
print("caps-zero", all(int(status(k), 16) == 0 for k in ("CapEff", "CapPrm", "CapBnd")))
print("nonewprivs", status("NoNewPrivs") == "1")
e, mib = fill("/work/big")
print("work-enospc", e == errno.ENOSPC and mib >= 60)
print("no-net-unreachable", err(conn) == errno.ENETUNREACH)
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
		for _, d := range []string{cg, filepath.Dir(cg)} {
			if err := os.Remove(d); err != nil && !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("cleanup cgroup %s: %v", d, err)
			}
		}
	})

	o := bundle.Options{
		ID: id, Dir: filepath.Join(state, "bundles", id), RootFS: rootfs, NodeID: node,
		CPUMillis: 500, MemoryBytes: 256 << 20, Prime: "json",
		Hosts: []bundle.HostEntry{{IP: "10.1.2.3", Name: "api.example"}},
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
	want := "uid1000 True\ncwd True\ntmpfs-writes True\nvar-tmp-erofs True\nhome-erofs True\nusr-fails True\nhosts-fails True\n" +
		"hosts-mount-ro True\nhosts-content True\ndev-fails True\ncaps-zero True\nnonewprivs True\nwork-enospc True\nno-net-unreachable True\n"
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
