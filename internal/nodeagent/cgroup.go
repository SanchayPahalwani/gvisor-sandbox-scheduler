package nodeagent

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Cgroups is the host-side cgroup access the manager needs. Paths are
// relative to the cgroup v2 mount, as bundle.CgroupPath returns them
// ("/gss/<node>/<runsc-id>").
type Cgroups interface {
	SetLimits(path string, cpuMillis, memoryBytes int64) error // writes cpu.max and memory.max
	OOMKills(path string) (uint64, error)                      // oom_kill counter from memory.events
	Remove(path string) error                                  // rmdir, nil if absent
	KillTree(path string) error                                // write 1 to cgroup.kill, then remove children
	Ensure(path string) error                                  // mkdir -p, nil if it exists
}

// cpuPeriod is the cpu.max period, the same as the one in the bundle.
const cpuPeriod = 100000

// rmdirBusyWait bounds how long removing one cgroup retries while rmdir says
// EBUSY: a deleted sandbox's cgroup stays busy until its processes are gone.
const rmdirBusyWait = time.Second

// cgroupFS implements Cgroups on a cgroup v2 mount.
type cgroupFS struct{ root string }

// NewCgroupFS returns Cgroups for the cgroup v2 mount at root ("/sys/fs/cgroup").
func NewCgroupFS(root string) Cgroups { return &cgroupFS{root: root} }

// dir maps a cgroup path to its directory. The path must be absolute, clean
// and not "/", so the result is always strictly below the root.
func (c *cgroupFS) dir(path string) (string, error) {
	if path == "" || path == "/" || path[0] != '/' || filepath.Clean(path) != path {
		return "", fmt.Errorf("cgroup: path %q must be absolute, clean and not /", path)
	}
	return filepath.Join(c.root, path), nil
}

func (c *cgroupFS) SetLimits(path string, cpuMillis, memoryBytes int64) error {
	if cpuMillis <= 0 || memoryBytes <= 0 {
		return fmt.Errorf("cgroup: limits must be positive, got %dm and %d bytes", cpuMillis, memoryBytes)
	}
	d, err := c.dir(path)
	if err != nil {
		return err
	}
	if err := writeInterface(filepath.Join(d, "cpu.max"), fmt.Sprintf("%d %d", cpuMillis*cpuPeriod/1000, cpuPeriod)); err != nil {
		return err
	}
	return writeInterface(filepath.Join(d, "memory.max"), strconv.FormatInt(memoryBytes, 10))
}

func (c *cgroupFS) OOMKills(path string) (uint64, error) {
	d, err := c.dir(path)
	if err != nil {
		return 0, err
	}
	b, err := os.ReadFile(filepath.Join(d, "memory.events"))
	if err != nil {
		return 0, fmt.Errorf("cgroup: %w", err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if f := strings.Fields(line); len(f) == 2 && f[0] == "oom_kill" {
			n, err := strconv.ParseUint(f[1], 10, 64)
			if err != nil {
				return 0, fmt.Errorf("cgroup: %s memory.events: %w", path, err)
			}
			return n, nil
		}
	}
	return 0, fmt.Errorf("cgroup: %s memory.events has no oom_kill line", path)
}

// Remove removes the cgroup and any cgroups below it.
func (c *cgroupFS) Remove(path string) error {
	d, err := c.dir(path)
	if err != nil {
		return err
	}
	return rmdirTree(d)
}

// KillTree kills every process in the cgroup's subtree, then removes the
// cgroups below it. The cgroup itself stays. A missing cgroup is not an error.
func (c *cgroupFS) KillTree(path string) error {
	d, err := c.dir(path)
	if err != nil {
		return err
	}
	ents, err := os.ReadDir(d)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cgroup: %w", err)
	}
	if err := writeInterface(filepath.Join(d, "cgroup.kill"), "1"); err != nil {
		return err
	}
	var errs []error
	for _, ent := range ents {
		if ent.IsDir() {
			errs = append(errs, rmdirTree(filepath.Join(d, ent.Name())))
		}
	}
	return errors.Join(errs...)
}

// Ensure creates the cgroup and any missing parents. runsc enables the
// controllers it needs in the parents itself.
func (c *cgroupFS) Ensure(path string) error {
	d, err := c.dir(path)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(d, 0o755); err != nil {
		return fmt.Errorf("cgroup: %w", err)
	}
	return nil
}

// writeInterface writes one cgroup interface file. It never creates the file:
// a missing file means a missing cgroup or controller.
func writeInterface(path, value string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return fmt.Errorf("cgroup: %w", err)
	}
	if _, err := f.WriteString(value); err != nil {
		f.Close()
		return fmt.Errorf("cgroup: write %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("cgroup: write %s: %w", path, err)
	}
	return nil
}

// rmdirTree removes the cgroup directory d and every cgroup below it, deepest
// first. A cgroup directory holds only interface files, which rmdir does not
// mind, so nothing but directories is removed. A missing d is not an error.
func rmdirTree(d string) error {
	ents, err := os.ReadDir(d)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cgroup: %w", err)
	}
	for _, ent := range ents {
		if ent.IsDir() {
			if err := rmdirTree(filepath.Join(d, ent.Name())); err != nil {
				return err
			}
		}
	}
	deadline := time.Now().Add(rmdirBusyWait)
	for {
		err := syscall.Rmdir(d)
		switch {
		case err == nil, errors.Is(err, syscall.ENOENT):
			return nil
		case errors.Is(err, syscall.EBUSY) && time.Now().Before(deadline):
			time.Sleep(10 * time.Millisecond)
		default:
			return fmt.Errorf("cgroup: rmdir %s: %w", d, err)
		}
	}
}
