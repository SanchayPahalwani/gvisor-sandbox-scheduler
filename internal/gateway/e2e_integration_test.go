//go:build integration && linux

package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/nodeagent"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/testenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// The end-to-end tests run the real scheduler and two real node agents
// (groups a and b) as child processes against runsc and the python rootfs.
// The binaries are built from this checkout into the test's state
// directory, so a stale bin/ is never what runs. They need root:
// make test-integration PKG=./internal/gateway/...

const (
	// e2eAgentStart bounds an agent's start-up, which may build the
	// checkpoint image.
	e2eAgentStart = 2 * time.Minute
	e2eStop       = 90 * time.Second // a SIGTERM'd process destroys its sandboxes first
	e2eRPC        = time.Minute
	cgroupRoot    = "/sys/fs/cgroup"
)

var (
	schedulerServingRE = regexp.MustCompile(`msg="scheduler serving" listen=(\S+)`)
	agentServingRE     = regexp.MustCompile(`msg="node agent serving" .*\blisten=(\S+)`)
)

// e2eLog collects a process's output across its runs.
type e2eLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *e2eLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *e2eLog) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Len()
}

// Since returns the output after the first n bytes.
func (l *e2eLog) Since(n int) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return string(l.b.Bytes()[n:])
}

func (l *e2eLog) String() string { return l.Since(0) }

// procRun is one run of a process.
type procRun struct {
	cmd    *exec.Cmd
	exited chan struct{} // closed once Wait returned
	err    error         // from Wait; read after exited is closed
}

// proc is a binary the test starts, stops, kills and starts again.
type proc struct {
	name string
	bin  string
	args []string
	logs *e2eLog
	cur  *procRun // nil before the first start
}

// tryStart starts the process and waits until its output, from this run,
// matches ready; it returns the first submatch.
func (p *proc) tryStart(ready *regexp.Regexp, timeout time.Duration) (string, error) {
	mark := p.logs.Len()
	r := &procRun{cmd: exec.Command(p.bin, p.args...), exited: make(chan struct{})}
	r.cmd.Stdout, r.cmd.Stderr = p.logs, p.logs
	r.cmd.WaitDelay = 5 * time.Second
	if err := r.cmd.Start(); err != nil {
		return "", fmt.Errorf("start %s: %w", p.name, err)
	}
	go func() {
		r.err = r.cmd.Wait()
		close(r.exited)
	}()
	p.cur = r
	deadline := time.Now().Add(timeout)
	for {
		if m := ready.FindStringSubmatch(p.logs.Since(mark)); m != nil {
			return m[1], nil
		}
		select {
		case <-r.exited:
			return "", fmt.Errorf("%s exited before it was ready: %v", p.name, r.err)
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("%s not ready after %v", p.name, timeout)
		}
	}
}

func (p *proc) start(t *testing.T, ready *regexp.Regexp, timeout time.Duration) string {
	t.Helper()
	s, err := p.tryStart(ready, timeout)
	if err != nil {
		t.Fatalf("%v; output:\n%s", err, p.logs)
	}
	return s
}

func (p *proc) running() bool {
	if p.cur == nil {
		return false
	}
	select {
	case <-p.cur.exited:
		return false
	default:
		return true
	}
}

// stop sends SIGTERM and waits for the exit, which must be clean. A
// process still running after e2eStop is killed.
func (p *proc) stop() error {
	if !p.running() {
		return nil
	}
	if err := p.cur.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return err
	}
	select {
	case <-p.cur.exited:
		return p.cur.err
	case <-time.After(e2eStop):
		_ = p.cur.cmd.Process.Kill()
		<-p.cur.exited
		return fmt.Errorf("%s still running %v after SIGTERM; killed", p.name, e2eStop)
	}
}

// kill sends SIGKILL and waits for the exit.
func (p *proc) kill(t *testing.T) {
	t.Helper()
	if !p.running() {
		t.Fatalf("%s is not running", p.name)
	}
	if err := p.cur.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-p.cur.exited
}

type e2eCluster struct {
	t         *testing.T
	runsc     string
	sched     *proc
	schedAddr string
	agents    map[string]*proc  // by group
	ids       map[string]string // node ID by group
	c         gssv1.SandboxServiceClient
}

// newE2E builds the binaries and starts the scheduler and the agents of
// groups a and b (warm pool 1, one shared checkpoint directory, egress
// ranges 10.201.0.0/24 and 10.201.1.0/24), and waits for both nodes to be
// READY. Everything is stopped and removed when the test ends.
func newE2E(t *testing.T) *e2eCluster {
	t.Helper()
	testenv.RequireRoot(t)
	rootfs := testenv.Rootfs(t)
	c := &e2eCluster{t: t, runsc: testenv.RunscPath(t), agents: map[string]*proc{}, ids: map[string]string{}}
	dir := testenv.TempStateDir(t)
	schedBin, agentBin := buildBinaries(t, filepath.Join(dir, "bin"))
	b := make([]byte, 3)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(b) // node IDs differ from run to run; a sorts before b

	c.sched = &proc{name: "scheduler", bin: schedBin, args: []string{"--listen", "127.0.0.1:0"}, logs: &e2eLog{}}
	for _, g := range []string{"a", "b"} {
		id := "e2e-" + g + "-" + suffix
		c.ids[g] = id
		c.agents[g] = &proc{name: "node agent " + id, bin: agentBin, logs: &e2eLog{}}
		t.Cleanup(func() { cleanupNode(t, c.runsc, id) }) // runs after c.close
	}
	t.Cleanup(c.close)

	c.schedAddr = c.sched.start(t, schedulerServingRE, 30*time.Second)
	c.sched.args = []string{"--listen", c.schedAddr} // a restart listens where the agents look
	for i, g := range []string{"a", "b"} {
		id := c.ids[g]
		c.agents[g].args = []string{
			"--node-id", id, "--group", g, "--listen", "127.0.0.1:0", "--scheduler", c.schedAddr,
			"--warm-pool-size", "1",
			"--state-dir", filepath.Join(dir, id),
			"--checkpoint-dir", filepath.Join(dir, "checkpoints"),
			"--egress-cidr", fmt.Sprintf("10.201.%d.0/24", i),
			"--rootfs", rootfs, "--runsc", c.runsc,
		}
		c.agents[g].start(t, agentServingRE, e2eAgentStart)
	}

	// Reconnects within a second of a scheduler restart (grpc-go's default
	// backoff could make the client, not the scheduler, miss the 5 s).
	conn, err := grpc.NewClient(c.schedAddr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(reconnectParams))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	c.c = gssv1.NewSandboxServiceClient(conn)
	c.waitFor("both nodes READY", 15*time.Second, func() bool {
		ready := 0
		for _, n := range c.nodes() {
			if n.GetState() == gssv1.NodeState_NODE_STATE_READY {
				ready++
			}
		}
		return ready == 2
	})
	return c
}

// buildBinaries builds cmd/scheduler and cmd/nodeagent from this checkout
// into dir.
func buildBinaries(t *testing.T, dir string) (scheduler, agent string) {
	t.Helper()
	gobin, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("go not found on PATH: %v", err)
	}
	const mod = "github.com/SanchayPahalwani/gvisor-sandbox-scheduler"
	cmd := exec.Command(gobin, "build", "-buildvcs=false", "-o", dir+"/", mod+"/cmd/scheduler", mod+"/cmd/nodeagent")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return filepath.Join(dir, "scheduler"), filepath.Join(dir, "nodeagent")
}

// close stops the agents and then the scheduler. An agent the test killed
// is started again first: its start sweeps what the killed one left and
// its clean stop removes the rest. On failure every process's output is
// logged.
func (c *e2eCluster) close() {
	t := c.t
	for _, g := range []string{"a", "b"} {
		p := c.agents[g]
		if p.cur != nil && !p.running() {
			if _, err := p.tryStart(agentServingRE, e2eAgentStart); err != nil {
				t.Errorf("restart %s for its cleanup: %v", p.name, err)
				continue
			}
		}
		if err := p.stop(); err != nil {
			t.Errorf("stop %s: %v", p.name, err)
		}
	}
	if err := c.sched.stop(); err != nil {
		t.Errorf("stop the scheduler: %v", err)
	}
	if t.Failed() {
		for _, p := range []*proc{c.sched, c.agents["a"], c.agents["b"]} {
			t.Logf("%s output:\n%s", p.name, p.logs)
		}
	}
}

// cleanupNode removes whatever a node agent that did not stop cleanly left:
// its runsc containers and root, its cgroups, its nftables table and the
// root's lock file.
func cleanupNode(t *testing.T, runscBin, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	rt := runsc.NewCLI(runscBin, "/run/gss/"+id)
	if _, err := os.Stat(rt.Root); err == nil {
		ids, err := rt.List(ctx)
		if err != nil {
			t.Errorf("cleanup: runsc list %s: %v", rt.Root, err)
		}
		for _, cid := range ids {
			if err := rt.Delete(ctx, runsc.Ref{ID: cid}); err != nil {
				t.Errorf("cleanup: runsc delete %s: %v", cid, err)
			}
		}
		if err := rt.RemoveRoot(ctx); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}
	cg := nodeagent.NewCgroupFS(cgroupRoot)
	if err := cg.KillTree("/gss/" + id); err != nil {
		t.Errorf("cleanup: cgroups of %s: %v", id, err)
	}
	if err := cg.Remove("/gss/" + id); err != nil {
		t.Errorf("cleanup: cgroup of %s: %v", id, err)
	}
	_ = exec.Command("nft", "delete", "table", "inet", "gss_"+strings.ReplaceAll(id, "-", "_")).Run()
	_ = os.Remove(rt.Root + ".lock")
}

func (c *e2eCluster) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), e2eRPC)
	c.t.Cleanup(cancel)
	return ctx
}

// waitFor polls cond every 100 ms until it holds, for at most timeout.
func (c *e2eCluster) waitFor(what string, timeout time.Duration, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			c.t.Fatalf("not %s within %v", what, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *e2eCluster) nodes() []*gssv1.Node {
	c.t.Helper()
	resp, err := c.c.ListNodes(c.ctx(), &gssv1.ListNodesRequest{})
	if err != nil {
		c.t.Fatalf("ListNodes: %v", err)
	}
	return resp.GetNodes()
}

func (c *e2eCluster) node(id string) *gssv1.Node {
	for _, n := range c.nodes() {
		if n.GetInfo().GetId() == id {
			return n
		}
	}
	return nil
}

func (c *e2eCluster) create(s *gssv1.SandboxSpec) *gssv1.Sandbox {
	c.t.Helper()
	sb, err := c.c.CreateSandbox(c.ctx(), &gssv1.CreateSandboxRequest{Spec: s})
	if err != nil {
		c.t.Fatalf("CreateSandbox: %v", err)
	}
	if sb.GetState() != gssv1.SandboxState_SANDBOX_STATE_READY {
		c.t.Fatalf("CreateSandbox returned %v, want READY", sb)
	}
	return sb
}

func (c *e2eCluster) get(id string) *gssv1.Sandbox {
	c.t.Helper()
	sb, err := c.c.GetSandbox(c.ctx(), &gssv1.GetSandboxRequest{SandboxId: id})
	if err != nil {
		c.t.Fatalf("GetSandbox %s: %v", id, err)
	}
	return sb
}

func (c *e2eCluster) destroy(id string) {
	c.t.Helper()
	if _, err := c.c.DestroySandbox(c.ctx(), &gssv1.DestroySandboxRequest{SandboxId: id}); err != nil {
		c.t.Fatalf("DestroySandbox %s: %v", id, err)
	}
}

// python runs code in sandbox id and returns its stdout once the exit
// event came with code 0.
func (c *e2eCluster) python(id, code string) (string, error) {
	stream, err := c.c.Exec(c.ctx(), &gssv1.ExecRequest{SandboxId: id, Command: &gssv1.ExecRequest_PythonCode{PythonCode: code}})
	if err != nil {
		return "", err
	}
	var out, errOut strings.Builder
	var status *gssv1.ExitStatus
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return out.String(), err
		}
		switch {
		case ev.GetExit() != nil:
			status = ev.GetExit()
		case ev.GetStdout() != nil:
			out.Write(ev.GetStdout())
		default:
			errOut.Write(ev.GetStderr())
		}
	}
	if status == nil || status.GetCode() != 0 || status.GetTimedOut() || status.GetOomKilled() {
		return out.String(), fmt.Errorf("exit %v, stderr %q", status, errOut.String())
	}
	return out.String(), nil
}

func (c *e2eCluster) mustPrint4(id string) {
	c.t.Helper()
	out, err := c.python(id, "print(2+2)")
	if err != nil || out != "4\n" {
		c.t.Fatalf("Exec print(2+2) in %s: stdout %q, %v; want \"4\\n\"", id, out, err)
	}
}

// containersOf lists the runsc containers of sandbox id under the node's
// root: those whose runsc ID starts with "<id>_", which is the ID of a cold
// or restore start. A sandbox claimed from the warm pool keeps its pool ID
// ("warm_..."), so a test that checks for leftovers with this creates its
// sandbox with coldSpec.
func (c *e2eCluster) containersOf(nodeID, id string) []string {
	c.t.Helper()
	rt := runsc.NewCLI(c.runsc, "/run/gss/"+nodeID)
	if _, err := os.Stat(rt.Root); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	ids, err := rt.List(c.ctx())
	if err != nil {
		c.t.Fatalf("runsc list %s: %v", rt.Root, err)
	}
	var out []string
	for _, cid := range ids {
		if strings.HasPrefix(cid, id+"_") {
			out = append(out, cid)
		}
	}
	return out
}

// coldSpec forces the cold path, so that the sandbox's runsc IDs are
// "<id>_<incarnation>_" and containersOf finds them.
func coldSpec() *gssv1.SandboxSpec {
	return &gssv1.SandboxSpec{StartMode: gssv1.StartMode_START_MODE_COLD}
}

// writeCgroupKill writes 1 to dir/cgroup.kill: every process in the
// cgroup's subtree is killed.
func writeCgroupKill(dir string) error {
	f, err := os.OpenFile(filepath.Join(dir, "cgroup.kill"), os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	if _, err := f.WriteString("1"); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// TestE2ECreateExecDestroy: through SandboxService, a sandbox is created,
// runs code and is destroyed; both nodes are listed READY. It starts cold:
// the leftover check finds containers by the sandbox's own runsc ID, which a
// warm claim would not use.
func TestE2ECreateExecDestroy(t *testing.T) {
	c := newE2E(t)
	sb := c.create(coldSpec())
	if sb.GetIncarnation() != 1 || sb.GetNodeId() == "" || len(sb.GetPlacement().GetTop()) == 0 ||
		sb.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("sandbox = %v, want incarnation 1 on a node, with a placement, started cold", sb)
	}
	if left := c.containersOf(sb.GetNodeId(), sb.GetId()); len(left) != 1 {
		t.Errorf("runsc containers of %s before the destroy: %q, want one", sb.GetId(), left)
	}
	c.mustPrint4(sb.GetId())
	c.destroy(sb.GetId())
	if got := c.get(sb.GetId()); got.GetState() != gssv1.SandboxState_SANDBOX_STATE_DESTROYED {
		t.Errorf("after DestroySandbox: %v, want DESTROYED", got.GetState())
	}
	c.waitFor("no container of the sandbox left", 10*time.Second, func() bool {
		return len(c.containersOf(sb.GetNodeId(), sb.GetId())) == 0
	})
	nodes := c.nodes()
	if len(nodes) != 2 {
		t.Fatalf("ListNodes = %v, want 2 nodes", nodes)
	}
	for _, n := range nodes {
		if n.GetState() != gssv1.NodeState_NODE_STATE_READY {
			t.Errorf("node %s is %v, want READY", n.GetInfo().GetId(), n.GetState())
		}
	}
}

// TestE2ENodeLoss: the node holding a sandbox dies (the agent is killed and
// so are its sandboxes); within 15 s the sandbox runs code on the other
// node as incarnation 2.
func TestE2ENodeLoss(t *testing.T) {
	c := newE2E(t)
	a, b := c.ids["a"], c.ids["b"]
	// Bin-packing fills node a first; half a node each, so the third is on b.
	spec := &gssv1.SandboxSpec{Resources: &gssv1.Resources{CpuMillis: 2000, MemoryBytes: 512 << 20}}
	var onB *gssv1.Sandbox
	var onA []string
	for range 4 {
		sb := c.create(spec)
		if sb.GetNodeId() == b {
			onB = sb
			break
		}
		onA = append(onA, sb.GetId())
	}
	if onB == nil {
		t.Fatalf("no sandbox on node %s after 4 creates", b)
	}
	for _, id := range onA {
		c.destroy(id) // room on node a for the move
	}

	killed := time.Now()
	c.agents["b"].kill(t)
	if err := writeCgroupKill(filepath.Join(cgroupRoot, "gss", b)); err != nil {
		t.Fatalf("kill the sandboxes of node %s: %v", b, err)
	}
	c.waitFor("the sandbox READY on node a at incarnation 2", 15*time.Second, func() bool {
		sb := c.get(onB.GetId())
		return sb.GetState() == gssv1.SandboxState_SANDBOX_STATE_READY && sb.GetNodeId() == a && sb.GetIncarnation() == 2
	})
	t.Logf("sandbox %s READY on %s %v after the kill", onB.GetId(), a, time.Since(killed).Round(time.Millisecond))
	c.mustPrint4(onB.GetId())
	if n := c.node(b); n.GetState() != gssv1.NodeState_NODE_STATE_DEAD {
		t.Errorf("node %s is %v, want DEAD", b, n.GetState())
	}
}

// TestE2EAgentRestart: an agent holding a sandbox is killed and started
// again with the same node ID once the sandbox has moved: it is READY and
// empty, and runsc has nothing of the sandbox under its root. The sandbox
// starts cold, so that the leftover check by its runsc ID means something.
func TestE2EAgentRestart(t *testing.T) {
	c := newE2E(t)
	sb := c.create(coldSpec())
	if left := c.containersOf(sb.GetNodeId(), sb.GetId()); len(left) != 1 {
		t.Fatalf("runsc containers of %s before the kill: %q, want one", sb.GetId(), left)
	}
	holder, other := "a", "b"
	if sb.GetNodeId() == c.ids["b"] {
		holder, other = "b", "a"
	}
	holderID := c.ids[holder]
	c.agents[holder].kill(t)
	c.waitFor("the sandbox READY on the other node", 15*time.Second, func() bool {
		got := c.get(sb.GetId())
		return got.GetState() == gssv1.SandboxState_SANDBOX_STATE_READY && got.GetNodeId() == c.ids[other] && got.GetIncarnation() == 2
	})
	restarted := time.Now()
	c.agents[holder].start(t, agentServingRE, e2eAgentStart)
	c.waitFor("the restarted node READY", 15*time.Second, func() bool {
		n := c.node(holderID)
		return n != nil && n.GetState() == gssv1.NodeState_NODE_STATE_READY && n.GetLastHeartbeat().AsTime().After(restarted)
	})
	if n := c.node(holderID); n.GetSandboxes() != 0 || n.GetAllocated().GetCpuMillis() != 0 {
		t.Errorf("restarted node %s holds %d sandboxes (%v allocated), want none", holderID, n.GetSandboxes(), n.GetAllocated())
	}
	if left := c.containersOf(holderID, sb.GetId()); len(left) != 0 {
		t.Errorf("runsc containers of %s left on %s: %q", sb.GetId(), holderID, left)
	}
	c.mustPrint4(sb.GetId())
}

// TestE2ESchedulerRestart: after the scheduler restarts, the nodes register
// again and the sandbox they report is adopted within 5 s and runs code.
func TestE2ESchedulerRestart(t *testing.T) {
	c := newE2E(t)
	sb := c.create(nil)
	if err := c.sched.stop(); err != nil {
		t.Fatalf("stop the scheduler: %v", err)
	}
	c.sched.start(t, schedulerServingRE, 30*time.Second)
	c.waitFor("the sandbox adopted READY", 5*time.Second, func() bool {
		resp, err := c.c.ListSandboxes(c.ctx(), &gssv1.ListSandboxesRequest{})
		if err != nil {
			return false // the client's connection may still be reconnecting
		}
		for _, got := range resp.GetSandboxes() {
			if got.GetId() == sb.GetId() {
				return got.GetState() == gssv1.SandboxState_SANDBOX_STATE_READY && got.GetNodeId() == sb.GetNodeId() &&
					got.GetIncarnation() == sb.GetIncarnation()
			}
		}
		return false
	})
	c.mustPrint4(sb.GetId())
}
