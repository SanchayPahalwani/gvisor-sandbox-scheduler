//go:build integration && linux

package nodeagent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/netpolicy"
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
	m, err := NewManager(Config{
		NodeID: n.id, Group: "test",
		CPUMillis: 4000, MemoryBytes: 4 << 30, MaxSandboxes: 8,
		RootFS: testenv.Rootfs(t), StateDir: n.state,
	}, Deps{Runtime: n.rt, Cgroups: NewCgroupFS(cgroupRoot), Net: np})
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

// The egress target of TestRealEgressSandbox: the fixture of
// internal/netpolicy's integration test, with the same names, so that each
// test removes what the other may have left.
const (
	targetNS   = "gss-test-target"
	targetVeth = "gsstgt0"
	targetHost = "10.210.0.1"
	targetAddr = "10.210.0.2"
)

func mustRun(t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v: %s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

// egressTarget creates the target namespace, joined to the host by a veth,
// with HTTP servers on targetAddr:8080 and :8081.
func egressTarget(t *testing.T) {
	t.Helper()
	cleanup := func() {
		_ = exec.Command("ip", "netns", "del", targetNS).Run()
		_ = exec.Command("ip", "link", "del", targetVeth).Run()
	}
	cleanup()
	t.Cleanup(cleanup)
	mustRun(t, "ip", "netns", "add", targetNS)
	mustRun(t, "ip", "link", "add", targetVeth, "type", "veth", "peer", "name", "eth0", "netns", targetNS)
	mustRun(t, "ip", "addr", "add", targetHost+"/30", "dev", targetVeth)
	mustRun(t, "ip", "link", "set", targetVeth, "up")
	mustRun(t, "ip", "-n", targetNS, "addr", "add", targetAddr+"/30", "dev", "eth0")
	mustRun(t, "ip", "-n", targetNS, "link", "set", "eth0", "up")
	mustRun(t, "ip", "-n", targetNS, "link", "set", "lo", "up")
	mustRun(t, "ip", "-n", targetNS, "route", "add", "default", "via", targetHost, "dev", "eth0")
	for _, port := range []string{"8080", "8081"} {
		cmd := exec.Command("ip", "netns", "exec", targetNS, "python3", "-m", "http.server", port, "--bind", "0.0.0.0")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start http.server %s: %v", port, err)
		}
		t.Cleanup(func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		})
		deadline := time.Now().Add(10 * time.Second)
		for {
			c, err := net.DialTimeout("tcp", net.JoinHostPort(targetAddr, port), time.Second)
			if err == nil {
				c.Close()
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("http.server on %s:%s did not answer: %v", targetAddr, port, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// egressPolicy returns a real network policy for node, initialised on a
// random 10.208.<n>.0/24 and closed at cleanup. It resolves target.test to
// targetAddr and no other name.
func egressPolicy(t *testing.T, node string) *netpolicy.Manager {
	t.Helper()
	n, err := rand.Int(rand.Reader, big.NewInt(256))
	if err != nil {
		t.Fatal(err)
	}
	np, err := netpolicy.New(node, netip.MustParsePrefix(fmt.Sprintf("10.208.%d.0/24", n.Int64())), nil)
	if err != nil {
		t.Fatal(err)
	}
	np.SetResolver(func(ctx context.Context, host string) ([]netip.Addr, error) {
		if host == "target.test" {
			return []netip.Addr{netip.MustParseAddr(targetAddr)}, nil
		}
		return nil, fmt.Errorf("no such host %q", host)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := np.Init(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := np.Close(ctx); err != nil {
			t.Errorf("netpolicy Close: %v", err)
		}
	})
	return np
}

// connectCode is python_code that tries a TCP connection to addr:port and
// prints "connected", "timeout" or "error <reason>".
func connectCode(addr string, port int) string {
	return fmt.Sprintf(`import socket
s = socket.socket()
s.settimeout(2)
try:
    s.connect((%q, %d))
    print("connected")
except socket.timeout:
    print("timeout")
except OSError as e:
    print("error", e)
`, addr, port)
}

func TestRealEgressSandbox(t *testing.T) {
	n := newRealNode(t)
	egressTarget(t)
	np := egressPolicy(t, n.id)
	m := n.managerNet(t, np)
	t.Cleanup(func() { // before np's Close: the manager's teardowns use it
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := m.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	egress := &gssv1.SandboxSpec{EgressAllow: []string{targetAddr + ":8080", "target.test:8080"}}
	if _, err := m.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: "eg-1", Incarnation: 1, Spec: egress}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: "none-1", Incarnation: 1}); err != nil {
		t.Fatal(err)
	}

	get := pyReq("eg-1", `import urllib.request
print(urllib.request.urlopen("http://target.test:8080/", timeout=5).status)`)
	r := runExec(ctx, m, get, nil)
	if exit := r.ok(t, "GET http://target.test:8080/"); exit.GetCode() != 0 || r.stdout.String() != "200\n" {
		t.Errorf("GET http://target.test:8080/: exit %v, stdout %q, stderr %q; want 0 and \"200\\n\"", exit, r.stdout.String(), r.stderr.String())
	}
	r = runExec(ctx, m, pyReq("eg-1", connectCode(targetAddr, 8081)), nil)
	if r.ok(t, "connect to 8081"); strings.TrimSpace(r.stdout.String()) != "timeout" {
		t.Errorf("%s:8081 (not allowed): %q (stderr %q), want timeout", targetAddr, r.stdout.String(), r.stderr.String())
	}
	r = runExec(ctx, m, pyReq("none-1", connectCode(targetAddr, 8080)), nil)
	if r.ok(t, "connect without egress"); !strings.Contains(r.stdout.String(), "Network is unreachable") {
		t.Errorf("%s:8080 from a sandbox without egress: %q (stderr %q), want \"Network is unreachable\"", targetAddr, r.stdout.String(), r.stderr.String())
	}

	// netpolicy names the namespace gss-<node>-<hash> and the chain sb_<hash>,
	// in the table gss_<node> (node IDs here hold only letters and digits).
	nsPrefix, table := "gss-"+n.id+"-", "gss_"+n.id
	if out := mustRun(t, "ip", "netns", "list"); !strings.Contains(out, nsPrefix) {
		t.Fatalf("no namespace %s* for the egress sandbox:\n%s", nsPrefix, out)
	}
	if out := mustRun(t, "nft", "list", "table", "inet", table); !strings.Contains(out, "chain sb_") {
		t.Fatalf("no sandbox chain in table %s:\n%s", table, out)
	}
	if err := m.Destroy(ctx, "eg-1", 0); err != nil {
		t.Fatal(err)
	}
	if out := mustRun(t, "ip", "netns", "list"); strings.Contains(out, nsPrefix) {
		t.Errorf("namespace left after Destroy:\n%s", out)
	}
	if out := mustRun(t, "nft", "list", "table", "inet", table); strings.Contains(out, "chain sb_") {
		t.Errorf("sandbox chain left after Destroy:\n%s", out)
	}
}
