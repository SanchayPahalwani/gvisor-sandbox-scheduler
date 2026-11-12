//go:build integration && linux

package nodeagent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/testenv"
)

const cgroupRoot = "/sys/fs/cgroup"

// realNode is one test node: a random node ID, its runsc root under /run/gss,
// a state directory, and a cleanup that removes every container, the root and
// the node's cgroup subtree.
type realNode struct {
	id, state string
	rt        *runsc.CLI
}

func newRealNode(t *testing.T) *realNode {
	t.Helper()
	testenv.RequireRoot(t)
	bin := testenv.RunscPath(t)
	testenv.Rootfs(t)
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	n := &realNode{id: "t" + hex.EncodeToString(b), state: testenv.TempStateDir(t)}
	n.rt = runsc.NewCLI(bin, "/run/gss/"+n.id)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		ids, err := n.rt.List(ctx)
		if err != nil {
			t.Errorf("cleanup list: %v", err)
		}
		for _, id := range ids {
			if err := n.rt.Delete(ctx, runsc.Ref{ID: id}); err != nil {
				t.Errorf("cleanup delete %s: %v", id, err)
			}
		}
		if err := n.rt.RemoveRoot(ctx); err != nil {
			t.Errorf("cleanup RemoveRoot: %v", err)
		}
		cg := NewCgroupFS(cgroupRoot)
		if err := cg.KillTree(nodeCgroup(n.id)); err != nil {
			t.Errorf("cleanup cgroups: %v", err)
		}
		if err := cg.Remove(nodeCgroup(n.id)); err != nil {
			t.Errorf("cleanup node cgroup: %v", err)
		}
	})
	return n
}

// manager returns a started manager for the node; the caller closes it.
func (n *realNode) manager(t *testing.T) *Manager {
	t.Helper()
	m, err := NewManager(Config{
		NodeID: n.id, Group: "test",
		CPUMillis: 4000, MemoryBytes: 4 << 30, MaxSandboxes: 8,
		RootFS: testenv.Rootfs(t), StateDir: n.state,
	}, Deps{Runtime: n.rt, Cgroups: NewCgroupFS(cgroupRoot)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return m
}

func (n *realNode) cgroupDir(rid string) string {
	return filepath.Join(cgroupRoot, "gss", n.id, rid)
}

func (n *realNode) listContainers(t *testing.T) []string {
	t.Helper()
	ids, err := n.rt.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

// childDirs returns the names of the directories in dir.
func childDirs(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out
}

func TestRealCreateDestroy(t *testing.T) {
	n := newRealNode(t)
	m := n.manager(t)
	defer m.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	resp, err := m.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: "sb-1", Spec: &gssv1.SandboxSpec{}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD || resp.GetReadyLatency().AsDuration() <= 0 {
		t.Errorf("response = %v, want COLD with a positive ready_latency", resp)
	}
	t.Logf("cold ready latency %v", resp.GetReadyLatency().AsDuration())
	rid := "sb-1_0_"
	if got := n.listContainers(t); len(got) != 1 || got[0] != rid {
		t.Errorf("runsc list = %q, want [%s]", got, rid)
	}
	mem, err := os.ReadFile(filepath.Join(n.cgroupDir(rid), "memory.max"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(mem)); got != "268435456" {
		t.Errorf("memory.max = %q, want 268435456", got)
	}
	cpu, err := os.ReadFile(filepath.Join(n.cgroupDir(rid), "cpu.max"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(cpu)); got != "50000 100000" {
		t.Errorf("cpu.max = %q, want 50000 100000", got)
	}

	if err := m.Destroy(ctx, "sb-1", 0); err != nil {
		t.Fatal(err)
	}
	if got := n.listContainers(t); len(got) != 0 {
		t.Errorf("runsc list after Destroy = %q", got)
	}
	if _, err := os.Stat(filepath.Join(n.state, "bundles", rid)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("bundle dir after Destroy: %v", err)
	}
	if _, err := os.Stat(n.cgroupDir(rid)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("cgroup dir after Destroy: %v", err)
	}
	if got := m.List(); len(got) != 0 {
		t.Errorf("List after Destroy = %v", got)
	}
}

// runsc removes the cgroup directories it created for a container when it
// deletes it; the node's own cgroup must not be one of them.
func TestRealDestroyFirstOfTwo(t *testing.T) {
	n := newRealNode(t)
	m := n.manager(t)
	defer m.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, id := range []string{"sb-1", "sb-2"} {
		if _, err := m.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: id, Incarnation: 1}); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	if err := m.Destroy(ctx, "sb-1", 0); err != nil {
		t.Fatalf("Destroy of the first sandbox: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("Destroy took %v", d)
	}
	if got := childDirs(t, filepath.Join(cgroupRoot, "gss", n.id)); len(got) != 1 || got[0] != "sb-2_1_" {
		t.Errorf("sandbox cgroups = %q, want [sb-2_1_]", got)
	}
	if err := m.Destroy(ctx, "sb-2", 0); err != nil {
		t.Fatal(err)
	}
	if got := n.listContainers(t); len(got) != 0 {
		t.Errorf("runsc list = %q", got)
	}
}

func TestRealRestartCleansUp(t *testing.T) {
	n := newRealNode(t)
	a := n.manager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, id := range []string{"sb-1", "sb-2"} {
		if _, err := a.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: id, Incarnation: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if got := childDirs(t, filepath.Join(cgroupRoot, "gss", n.id)); len(got) != 2 {
		t.Fatalf("sandbox cgroups before restart = %q", got)
	}
	// Agent A dies: its loops stop and nothing is torn down.
	a.stopLoops()

	b := n.manager(t)
	defer b.Close(context.Background())
	if got := n.listContainers(t); len(got) != 0 {
		t.Errorf("runsc list after restart = %q", got)
	}
	if got := childDirs(t, filepath.Join(cgroupRoot, "gss", n.id)); len(got) != 0 {
		t.Errorf("sandbox cgroups after restart = %q", got)
	}
	if got := childDirs(t, filepath.Join(n.state, "bundles")); len(got) != 0 {
		t.Errorf("bundles after restart = %q", got)
	}
	// The restarted node serves the same sandbox ID again.
	if _, err := b.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: "sb-1", Incarnation: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestRealSandboxProcessDeath(t *testing.T) {
	n := newRealNode(t)
	m := n.manager(t)
	defer m.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := m.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: "sb-1", Incarnation: 1}); err != nil {
		t.Fatal(err)
	}
	rid := "sb-1_1_"
	st, err := n.rt.State(ctx, runsc.Ref{ID: rid})
	if err != nil {
		t.Fatal(err)
	}
	cmdline, _ := os.ReadFile("/proc/" + strconv.Itoa(st.PID) + "/cmdline")
	t.Logf("runsc state pid %d: %q", st.PID, strings.ReplaceAll(string(cmdline), "\x00", " "))
	if err := syscall.Kill(st.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(m.List()) != 0 || len(n.listContainers(t)) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("dead sandbox still there: List %v, runsc list %q", m.List(), n.listContainers(t))
		}
		time.Sleep(100 * time.Millisecond)
	}
	for time.Now().Before(deadline) {
		if _, err := os.Stat(n.cgroupDir(rid)); errors.Is(err, fs.ErrNotExist) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Errorf("cgroup %s of the dead sandbox is still there", n.cgroupDir(rid))
}
