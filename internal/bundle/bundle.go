// Package bundle generates the OCI bundle (config.json and a hosts file) that
// runsc runs for one sandbox (call D10: shared read-only rootfs, writable tmpfs
// at /work and /tmp only).
package bundle

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/opencontainers/runtime-spec/specs-go"
)

// HostEntry is one pinned name in the sandbox's /etc/hosts.
type HostEntry struct{ IP, Name string }

// Options describes one sandbox's bundle.
type Options struct {
	ID          string // runsc container id
	Dir         string // bundle directory to create
	RootFS      string // shared read-only rootfs
	NodeID      string
	CPUMillis   int64
	MemoryBytes int64
	NetNSPath   string      // "" = fresh empty network namespace (use with runsc.NetworkNone)
	Hosts       []HostEntry // pinned names; "127.0.0.1 localhost" is always written first
	Prime       string      // value of GSS_PRIME
}

// DefaultPrime is the module list the zygote imports before it serves requests.
const DefaultPrime = "json,re,math,random,statistics,datetime,collections,itertools,functools,decimal,csv,io,numpy,pandas"

const cpuPeriod uint64 = 100000

// CgroupPath is the cgroup runsc creates for a sandbox, relative to /sys/fs/cgroup.
func CgroupPath(nodeID, id string) string { return "/gss/" + nodeID + "/" + id }

func tmpfs(dest string) specs.Mount {
	return specs.Mount{Destination: dest, Type: "tmpfs", Source: "tmpfs",
		Options: []string{"nosuid", "nodev", "mode=1777", "size=67108864"}}
}

// Spec builds the runtime spec. The process section depends only on o.Prime:
// restore refuses a bundle whose args differ from the checkpoint's.
func Spec(o Options) *specs.Spec {
	quota := o.CPUMillis * 100
	period := cpuPeriod
	limit := o.MemoryBytes
	return &specs.Spec{
		Version:  "1.0.2",
		Root:     &specs.Root{Path: o.RootFS, Readonly: true},
		Hostname: "sandbox",
		Process: &specs.Process{
			Terminal:        false,
			User:            specs.User{UID: 1000, GID: 1000},
			Args:            []string{"python3", "/gss/zygote.py"},
			Cwd:             "/work",
			NoNewPrivileges: true,
			Capabilities:    &specs.LinuxCapabilities{},
			Rlimits:         []specs.POSIXRlimit{{Type: "RLIMIT_NOFILE", Hard: 1024, Soft: 1024}},
			Env: []string{
				"PATH=/usr/local/bin:/usr/bin:/bin",
				"HOME=/work",
				"LANG=C.UTF-8",
				"PYTHONUNBUFFERED=1",
				"OPENBLAS_NUM_THREADS=1",
				"OMP_NUM_THREADS=1",
				"GSS_PRIME=" + o.Prime,
			},
		},
		Mounts: []specs.Mount{
			{Destination: "/proc", Type: "proc", Source: "proc"},
			{Destination: "/dev", Type: "tmpfs", Source: "tmpfs"},
			{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "nodev", "ro"}},
			tmpfs("/tmp"),
			tmpfs("/work"),
			{Destination: "/etc/hosts", Type: "bind", Source: filepath.Join(o.Dir, "hosts"), Options: []string{"bind", "ro"}},
		},
		Linux: &specs.Linux{
			CgroupsPath: CgroupPath(o.NodeID, o.ID),
			Resources: &specs.LinuxResources{
				CPU:    &specs.LinuxCPU{Quota: &quota, Period: &period},
				Memory: &specs.LinuxMemory{Limit: &limit},
			},
			Namespaces: []specs.LinuxNamespace{
				{Type: specs.PIDNamespace},
				{Type: specs.IPCNamespace},
				{Type: specs.UTSNamespace},
				{Type: specs.MountNamespace},
				{Type: specs.NetworkNamespace, Path: o.NetNSPath},
			},
		},
	}
}

var (
	idRE       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$`)
	hostnameRE = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$`)
)

func validName(s string) bool { return idRE.MatchString(s) && s != "." && s != ".." }

// Validate rejects options that would fail open (no limits), put the cgroup
// outside /gss/<node>, or write a malformed hosts file.
func (o Options) Validate() error {
	if o.CPUMillis <= 0 {
		return fmt.Errorf("bundle: CPUMillis must be positive, got %d", o.CPUMillis)
	}
	if o.MemoryBytes <= 0 {
		return fmt.Errorf("bundle: MemoryBytes must be positive, got %d", o.MemoryBytes)
	}
	if !validName(o.ID) {
		return fmt.Errorf("bundle: ID %q is not a valid name", o.ID)
	}
	if !validName(o.NodeID) {
		return fmt.Errorf("bundle: NodeID %q is not a valid name", o.NodeID)
	}
	if !filepath.IsAbs(o.Dir) {
		return fmt.Errorf("bundle: Dir %q must be an absolute path", o.Dir)
	}
	if !filepath.IsAbs(o.RootFS) {
		return fmt.Errorf("bundle: RootFS %q must be an absolute path", o.RootFS)
	}
	if o.NetNSPath != "" && !filepath.IsAbs(o.NetNSPath) {
		return fmt.Errorf("bundle: NetNSPath %q must be empty or an absolute path", o.NetNSPath)
	}
	for _, h := range o.Hosts {
		if _, err := netip.ParseAddr(h.IP); err != nil {
			return fmt.Errorf("bundle: Hosts entry IP %q: %w", h.IP, err)
		}
		if len(h.Name) > 253 || !hostnameRE.MatchString(h.Name) {
			return fmt.Errorf("bundle: Hosts entry Name %q is not a valid hostname", h.Name)
		}
	}
	return nil
}

// Write validates o, creates o.Dir (mode 0700) and writes config.json and hosts into it.
func Write(o Options) error {
	if err := o.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(o.Dir, 0o700); err != nil {
		return fmt.Errorf("bundle: %w", err)
	}
	if err := os.Chmod(o.Dir, 0o700); err != nil {
		return fmt.Errorf("bundle: %w", err)
	}
	var hosts strings.Builder
	hosts.WriteString("127.0.0.1 localhost\n")
	for _, h := range o.Hosts {
		hosts.WriteString(h.IP + " " + h.Name + "\n")
	}
	if err := os.WriteFile(filepath.Join(o.Dir, "hosts"), []byte(hosts.String()), 0o644); err != nil {
		return fmt.Errorf("bundle: %w", err)
	}
	raw, err := json.MarshalIndent(Spec(o), "", "  ")
	if err != nil {
		return fmt.Errorf("bundle: %w", err)
	}
	if err := os.WriteFile(filepath.Join(o.Dir, "config.json"), raw, 0o644); err != nil {
		return fmt.Errorf("bundle: %w", err)
	}
	return nil
}
