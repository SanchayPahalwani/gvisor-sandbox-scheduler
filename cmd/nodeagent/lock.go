package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// lockFile takes an exclusive flock on path without waiting, creating the
// file and its directory. It fails when another process holds the lock,
// naming that process's pid, which the holder writes into the file.
//
// The lock lasts until the returned file is closed or the process exits
// (the descriptor is close-on-exec, so runsc children do not keep it). The
// file is never removed: a second agent could otherwise lock a new file
// while the first still holds the old one.
func lockFile(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		if !errors.Is(err, unix.EWOULDBLOCK) {
			return nil, fmt.Errorf("lock %s: %w", path, err)
		}
		holder := "another node agent"
		if b, err := os.ReadFile(path); err == nil && strings.TrimSpace(string(b)) != "" {
			holder += " (pid " + strings.TrimSpace(string(b)) + ")"
		}
		return nil, fmt.Errorf("%s is locked by %s: one agent per state directory and per runsc root", path, holder)
	}
	// The pid is only for the message above; failing to write it is no
	// reason to stop.
	if err := f.Truncate(0); err == nil {
		_, _ = f.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\n"), 0)
	}
	return f, nil
}
