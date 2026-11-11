//go:build integration && linux

// Package testenv holds helpers for integration tests that need root, runsc and the
// python rootfs built by `make rootfs`.
package testenv

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	// RootfsDir is where scripts/build-rootfs.sh puts the python sandbox rootfs.
	RootfsDir = "/var/lib/gss/rootfs/python"
	testRoot  = "/var/lib/gss/test"
)

// RequireRoot skips the test unless it runs as root.
func RequireRoot(t testing.TB) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("needs root (run with make test-integration)")
	}
}

// Rootfs returns the python rootfs directory, or skips if it has not been built.
func Rootfs(t testing.TB) string {
	t.Helper()
	if _, err := os.Stat(filepath.Join(RootfsDir, "gss", "zygote.py")); err != nil {
		t.Skipf("no python rootfs at %s: run `make rootfs` first", RootfsDir)
	}
	return RootfsDir
}

// RunscPath returns the runsc binary from PATH or /usr/local/bin, or skips.
func RunscPath(t testing.TB) string {
	t.Helper()
	if p, err := exec.LookPath("runsc"); err == nil {
		return p
	}
	const fallback = "/usr/local/bin/runsc"
	if _, err := os.Stat(fallback); err == nil {
		return fallback
	}
	t.Skip("runsc not found on PATH or at " + fallback)
	return ""
}

// TempStateDir returns a new directory under /var/lib/gss/test/ that is removed when
// the test ends. It needs root.
func TempStateDir(t testing.TB) string {
	t.Helper()
	RequireRoot(t)
	if err := os.MkdirAll(testRoot, 0o755); err != nil {
		t.Fatalf("create %s: %v", testRoot, err)
	}
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	dir, err := os.MkdirTemp(testRoot, name+"-")
	if err != nil {
		t.Fatalf("create state dir: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove state dir %s: %v", dir, err)
		}
	})
	return dir
}
