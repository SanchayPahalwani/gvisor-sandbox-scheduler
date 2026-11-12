package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestLockFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "agent.lock") // the directory is created
	first, err := lockFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(path); err != nil || strings.TrimSpace(string(b)) != strconv.Itoa(os.Getpid()) {
		t.Errorf("lock file holds %q (%v), want this pid", b, err)
	}
	// flock locks belong to an open file, so a second open in this process
	// conflicts like another agent would.
	if _, err := lockFile(path); err == nil || !strings.Contains(err.Error(), "another node agent") ||
		!strings.Contains(err.Error(), strconv.Itoa(os.Getpid())) {
		t.Errorf("second lockFile = %v, want an error naming the holder's pid", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := lockFile(path)
	if err != nil {
		t.Fatalf("lockFile after the holder closed it: %v", err)
	}
	again.Close()
}
