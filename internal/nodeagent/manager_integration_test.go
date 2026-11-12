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
	"sync"
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
	return n.managerNet(t, nil)
}

// managerNet is manager with np as the network policy (nil: none).
func (n *realNode) managerNet(t *testing.T, np NetPolicy) *Manager {
	t.Helper()
	return n.managerConfig(t, np, nil)
}

// managerConfig is managerNet with mod (if not nil) applied to the config.
func (n *realNode) managerConfig(t *testing.T, np NetPolicy, mod func(*Config)) *Manager {
	t.Helper()
	cfg := Config{
		NodeID: n.id, Group: "test",
		CPUMillis: 4000, MemoryBytes: 4 << 30, MaxSandboxes: 8,
		RootFS: testenv.Rootfs(t), StateDir: n.state,
	}
	if mod != nil {
		mod(&cfg)
	}
	m, err := NewManager(cfg, Deps{Runtime: n.rt, Cgroups: NewCgroupFS(cgroupRoot), Net: np})
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

// restoreManager returns a started manager on n, with np as its network
// policy, whose checkpoint images live in the node's state directory: each
// test builds its own image, and it goes with the state directory. The
// manager is closed at cleanup (before np, which was made earlier).
func restoreManager(t *testing.T, n *realNode, np NetPolicy) *Manager {
	t.Helper()
	m := n.managerConfig(t, np, func(c *Config) { c.CheckpointDir = filepath.Join(n.state, "checkpoints") })
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := m.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	if !m.ImageReady() {
		t.Fatal("Start did not build the checkpoint image")
	}
	return m
}

// readCgroup returns the trimmed content of one interface file of a sandbox's cgroup.
func (n *realNode) readCgroup(t *testing.T, rid, file string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(n.cgroupDir(rid), file))
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

func TestRealRestore(t *testing.T) {
	n := newRealNode(t)
	m := restoreManager(t, n, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	dir, err := m.EnsureImage(ctx) // the one Start built
	if err != nil {
		t.Fatal(err)
	}
	if got, want := filepath.Dir(dir), filepath.Join(n.state, "checkpoints"); got != want {
		t.Errorf("image directory %s is not in %s", dir, want)
	}
	fi, err := os.Stat(filepath.Join(dir, "checkpoint.img"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("image %s: checkpoint.img %d bytes", dir, fi.Size())
	if got := n.listContainers(t); len(got) != 0 {
		t.Errorf("runsc list after the image build = %q, want none", got)
	}

	// Two sandboxes restored from the image at the same time; sb-1 with
	// limits other than the image sandbox's.
	ids := []string{"sb-1", "sb-2"}
	specs := []*gssv1.SandboxSpec{{Resources: &gssv1.Resources{CpuMillis: 1000, MemoryBytes: 512 << 20}}, nil}
	resps, errs := make([]*gssv1.NodeCreateResponse, len(ids)), make([]error, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resps[i], errs[i] = m.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: id, Incarnation: 1, Spec: specs[i]})
		}()
	}
	wg.Wait()
	for i, id := range ids {
		if errs[i] != nil {
			t.Fatalf("Create %s: %v", id, errs[i])
		}
		if got := resps[i].GetStartedAs(); got != gssv1.StartMode_START_MODE_RESTORE {
			t.Errorf("%s started as %v, want RESTORE", id, got)
		}
		t.Logf("%s restore ready latency %v", id, resps[i].GetReadyLatency().AsDuration())
	}
	if got := n.listContainers(t); len(got) != 2 {
		t.Errorf("runsc list = %q, want sb-1_1_ and sb-2_1_", got)
	}
	for _, id := range ids {
		r := runExec(ctx, m, pyReq(id, `import sys; print("pandas" in sys.modules)`), nil)
		if exit := r.ok(t, id); exit.GetCode() != 0 || r.stdout.String() != "True\n" {
			t.Errorf("%s: exit %v, stdout %q, stderr %q; want 0 and \"True\\n\"", id, exit, r.stdout.String(), r.stderr.String())
		}
	}
	// The sentry reads its memory total at boot, so a restored sandbox may
	// report the image sandbox's size (256 MiB); logged, not checked.
	r := runExec(ctx, m, pyReq("sb-1", `print(next(l for l in open("/proc/meminfo") if l.startswith("MemTotal")).strip())`), nil)
	if exit := r.ok(t, "MemTotal"); exit.GetCode() == 0 {
		t.Logf("sb-1 (512 MiB, restored from an image of a 256 MiB sandbox): %s", strings.TrimSpace(r.stdout.String()))
	}
	for rid, want := range map[string][2]string{
		"sb-1_1_": {"100000 100000", "536870912"},
		"sb-2_1_": {"50000 100000", "268435456"},
	} {
		if got := n.readCgroup(t, rid, "cpu.max"); got != want[0] {
			t.Errorf("%s cpu.max = %q, want %q", rid, got, want[0])
		}
		if got := n.readCgroup(t, rid, "memory.max"); got != want[1] {
			t.Errorf("%s memory.max = %q, want %q", rid, got, want[1])
		}
	}
}

func TestRealRestoreEgress(t *testing.T) {
	n := newRealNode(t)
	tgt := testenv.EgressTarget(t)
	np := egressPolicy(t, n.id, tgt)
	m := restoreManager(t, n, np)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	egress := &gssv1.SandboxSpec{EgressAllow: []string{tgt.Addr + ":" + tgt.AllowedPort, "target.test:" + tgt.AllowedPort}}
	resp, err := m.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: "eg-1", Incarnation: 1, Spec: egress})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStartedAs() != gssv1.StartMode_START_MODE_RESTORE {
		t.Fatalf("started as %v, want RESTORE (a no-network image restored into an egress sandbox)", resp.GetStartedAs())
	}
	get := pyReq("eg-1", `import urllib.request
print(urllib.request.urlopen("http://target.test:8080/", timeout=5).status)`)
	r := runExec(ctx, m, get, nil)
	if exit := r.ok(t, "GET http://target.test:8080/"); exit.GetCode() != 0 || r.stdout.String() != "200\n" {
		t.Errorf("GET http://target.test:8080/: exit %v, stdout %q, stderr %q; want 0 and \"200\\n\"", exit, r.stdout.String(), r.stderr.String())
	}
	r = runExec(ctx, m, pyReq("eg-1", connectCode(tgt.Addr, tgt.BlockedPort)), nil)
	if r.ok(t, "connect to 8081"); strings.TrimSpace(r.stdout.String()) != "timeout" {
		t.Errorf("%s:8081 (not allowed): %q (stderr %q), want timeout", tgt.Addr, r.stdout.String(), r.stderr.String())
	}
}

// netnsCount counts the network namespaces whose name starts with prefix.
func netnsCount(t *testing.T, prefix string) int {
	t.Helper()
	n := 0
	for _, line := range strings.Split(testenv.MustRun(t, "ip", "netns", "list"), "\n") {
		if strings.HasPrefix(line, prefix) {
			n++
		}
	}
	return n
}

// TestRealRestoreBrokenImage restores an egress sandbox from an image whose
// checkpoint.img was cut in half: the Create falls back to cold, and nothing
// of the failed restore is left (container, cgroup, network namespace).
func TestRealRestoreBrokenImage(t *testing.T) {
	n := newRealNode(t)
	tgt := testenv.EgressTarget(t)
	np := egressPolicy(t, n.id, tgt)
	m := restoreManager(t, n, np)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	dir, err := m.EnsureImage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	img := filepath.Join(dir, "checkpoint.img")
	fi, err := os.Stat(img)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(img, fi.Size()/2); err != nil {
		t.Fatal(err)
	}

	egress := &gssv1.SandboxSpec{EgressAllow: []string{tgt.Addr + ":" + tgt.AllowedPort}}
	resp, err := m.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: "eg-1", Incarnation: 1, Spec: egress})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("started as %v, want COLD after the broken restore", resp.GetStartedAs())
	}
	t.Logf("ready latency with the fallback: %v", resp.GetReadyLatency().AsDuration())
	// A failed runsc restore drops the image at once; a restored sandbox that
	// does not get ready drops it only on the third in a row. Which of the
	// two a truncated image gives is runsc's business, so it is only logged.
	t.Logf("image still used after one broken restore: %v", m.ImageReady())
	rid := "eg-1_1_"
	if got := n.listContainers(t); len(got) != 1 || got[0] != rid {
		t.Errorf("runsc list = %q, want only the cold container %s", got, rid)
	}
	if got := childDirs(t, filepath.Join(cgroupRoot, "gss", n.id)); len(got) != 1 || got[0] != rid {
		t.Errorf("cgroups under /gss/%s = %q, want only %s", n.id, got, rid)
	}
	nsPrefix := "gss-" + n.id + "-"
	if got := netnsCount(t, nsPrefix); got != 1 {
		t.Errorf("%d network namespaces %s*, want 1 (the cold sandbox's)", got, nsPrefix)
	}
	r := runExec(ctx, m, pyReq("eg-1", connectCode(tgt.Addr, tgt.AllowedPort)), nil)
	if r.ok(t, "connect from the cold sandbox"); strings.TrimSpace(r.stdout.String()) != "connected" {
		t.Errorf("%s:%s from the cold sandbox: %q (stderr %q), want connected", tgt.Addr, tgt.AllowedPort, r.stdout.String(), r.stderr.String())
	}
	if err := m.Destroy(ctx, "eg-1", 0); err != nil {
		t.Fatal(err)
	}
	if got := netnsCount(t, nsPrefix); got != 0 {
		t.Errorf("%d network namespaces %s* after Destroy, want 0", got, nsPrefix)
	}
}

func TestRealColdStillWorks(t *testing.T) {
	n := newRealNode(t)
	m := restoreManager(t, n, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cold, err := m.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: "sb-1", Incarnation: 1,
		Spec: &gssv1.SandboxSpec{StartMode: gssv1.StartMode_START_MODE_COLD}})
	if err != nil {
		t.Fatal(err)
	}
	if cold.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("forced COLD started as %v", cold.GetStartedAs())
	}
	auto, err := m.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: "sb-2", Incarnation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if auto.GetStartedAs() != gssv1.StartMode_START_MODE_RESTORE {
		t.Errorf("AUTO on the same node started as %v, want RESTORE", auto.GetStartedAs())
	}
	t.Logf("ready latency: cold %v, restore %v", cold.GetReadyLatency().AsDuration(), auto.GetReadyLatency().AsDuration())
	r := runExec(ctx, m, pyReq("sb-1", `print("hello")`), nil)
	if exit := r.ok(t, "cold sandbox"); exit.GetCode() != 0 || r.stdout.String() != "hello\n" {
		t.Errorf("cold sandbox: exit %v, stdout %q, stderr %q", exit, r.stdout.String(), r.stderr.String())
	}
}

// waitWarmReady polls m until n pool sandboxes are ready, for up to limit,
// and returns how long it took.
func waitWarmReady(t *testing.T, m *Manager, n int, limit time.Duration, what string) time.Duration {
	t.Helper()
	begin := time.Now()
	for m.WarmReady() != n {
		if time.Since(begin) > limit {
			t.Fatalf("%s: WarmReady = %d after %v, want %d", what, m.WarmReady(), limit, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return time.Since(begin)
}

func TestRealWarmClaim(t *testing.T) {
	n := newRealNode(t)
	m := n.managerConfig(t, nil, func(c *Config) {
		c.CheckpointDir = filepath.Join(n.state, "checkpoints")
		c.WarmPoolSize = 2
	})
	closed := false
	t.Cleanup(func() {
		if closed {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := m.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	if !m.ImageReady() {
		t.Fatal("Start did not build the checkpoint image")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	d := waitWarmReady(t, m, 2, time.Minute, "pool after Start")
	t.Logf("pool full %v after Start", d)
	if got := n.listContainers(t); len(got) != 2 || !strings.HasPrefix(got[0], "warm_") || !strings.HasPrefix(got[1], "warm_") {
		t.Errorf("runsc list = %q, want two pool sandboxes", got)
	}
	if got := m.List(); len(got) != 0 {
		t.Errorf("List = %v, want no pool sandboxes", got)
	}

	resp, err := m.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: "sb-1", Incarnation: 1,
		Spec: &gssv1.SandboxSpec{Resources: &gssv1.Resources{CpuMillis: 1000, MemoryBytes: 512 << 20}}})
	if err != nil {
		t.Fatal(err)
	}
	claimed := time.Now()
	if resp.GetStartedAs() != gssv1.StartMode_START_MODE_WARM {
		t.Fatalf("started as %v, want WARM", resp.GetStartedAs())
	}
	lat := resp.GetReadyLatency().AsDuration()
	t.Logf("warm ready latency %v", lat)
	if lat <= 0 || lat >= 50*time.Millisecond {
		t.Errorf("warm ready latency %v, want under 50ms", lat)
	}
	m.mu.Lock()
	rid := m.sandboxes["sb-1"].ref.ID
	m.mu.Unlock()
	if !strings.HasPrefix(rid, "warm_") {
		t.Errorf("sb-1 runs as %s, want a pool sandbox", rid)
	}
	if got := n.readCgroup(t, rid, "cpu.max"); got != "100000 100000" {
		t.Errorf("cpu.max = %q, want \"100000 100000\"", got)
	}
	if got := n.readCgroup(t, rid, "memory.max"); got != "536870912" {
		t.Errorf("memory.max = %q, want 536870912", got)
	}
	r := runExec(ctx, m, pyReq("sb-1", `import sys; print("pandas" in sys.modules)`), nil)
	if exit := r.ok(t, "python in the claimed sandbox"); exit.GetCode() != 0 || r.stdout.String() != "True\n" {
		t.Errorf("exit %v, stdout %q, stderr %q; want 0 and \"True\\n\"", exit, r.stdout.String(), r.stderr.String())
	}
	if _, err := os.Stat(filepath.Join(n.state, "execs", rid)); err != nil {
		t.Errorf("exec pid directory of %s: %v", rid, err)
	}

	waitWarmReady(t, m, 2, 10*time.Second-time.Since(claimed), "pool after the claim")
	t.Logf("pool full again %v after the claim", time.Since(claimed))
	if got := n.listContainers(t); len(got) != 3 {
		t.Errorf("runsc list = %q, want sb-1's and two pool sandboxes", got)
	}

	if err := m.Destroy(ctx, "sb-1", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(n.cgroupDir(rid)); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("cgroup of %s after Destroy: %v", rid, err)
	}
	closed = true
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if got := n.listContainers(t); len(got) != 0 {
		t.Errorf("runsc list after Close = %q", got)
	}
	if got := childDirs(t, filepath.Join(cgroupRoot, "gss", n.id)); len(got) != 0 {
		t.Errorf("sandbox cgroups after Close = %q", got)
	}
}
