//go:build integration && linux

package nodeagent

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/testenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// buildNodeAgent builds cmd/nodeagent from this checkout into a directory
// under /var/lib/gss/test, so the test never runs a stale bin/nodeagent.
func buildNodeAgent(t *testing.T) string {
	t.Helper()
	gobin, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("go not found on PATH: %v", err)
	}
	bin := filepath.Join(testenv.TempStateDir(t), "nodeagent")
	cmd := exec.Command(gobin, "build", "-o", bin, "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/cmd/nodeagent")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build nodeagent: %v\n%s", err, out)
	}
	return bin
}

// agentProc is a node agent running as a child process.
type agentProc struct {
	cmd    *exec.Cmd
	logs   *syncBuffer   // its stderr
	exited chan struct{} // closed once Wait returned
	err    error         // from Wait; read after exited is closed
}

// listenRE finds the address in the agent's start-up line.
var listenRE = regexp.MustCompile(`msg="node agent serving" .*\blisten=(\S+)`)

// startAgent starts bin with args and waits for it to serve. It returns the
// process and the address it listens on. A process still running at
// cleanup is killed.
func startAgent(t *testing.T, bin string, args ...string) (*agentProc, string) {
	t.Helper()
	p := &agentProc{cmd: exec.Command(bin, args...), logs: &syncBuffer{}, exited: make(chan struct{})}
	p.cmd.Stderr = p.logs
	p.cmd.WaitDelay = 5 * time.Second
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		p.err = p.cmd.Wait()
		close(p.exited)
	}()
	t.Cleanup(func() {
		select {
		case <-p.exited:
		default:
			_ = p.cmd.Process.Kill()
			<-p.exited
		}
		t.Logf("node agent stderr:\n%s", p.logs.String())
	})
	deadline := time.Now().Add(time.Minute)
	for {
		if m := listenRE.FindStringSubmatch(p.logs.String()); m != nil {
			return p, m[1]
		}
		select {
		case <-p.exited:
			t.Fatalf("node agent exited before serving: %v\n%s", p.err, p.logs.String())
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("node agent not serving after a minute:\n%s", p.logs.String())
		}
	}
}

// TestRealNodeAgentBinary drives the real binary over gRPC: create, Exec,
// destroy, then SIGTERM with a second sandbox still running, which the
// shutdown destroys before it removes the runsc root.
func TestRealNodeAgentBinary(t *testing.T) {
	n := newRealNode(t) // node ID, state dir, and the cleanup of /run/gss/<node> and its cgroups
	bin := buildNodeAgent(t)
	r, err := rand.Int(rand.Reader, big.NewInt(256))
	if err != nil {
		t.Fatal(err)
	}
	table := "gss_" + n.id // netpolicy's table; node IDs here hold only letters and digits
	t.Cleanup(func() { _ = exec.Command("nft", "delete", "table", "inet", table).Run() })
	p, addr := startAgent(t, bin,
		"--node-id", n.id, "--listen", "127.0.0.1:0",
		"--warm-pool-size=0", "--checkpoint-dir=",
		"--state-dir", n.state, "--rootfs", testenv.Rootfs(t), "--runsc", testenv.RunscPath(t),
		"--egress-cidr", fmt.Sprintf("10.207.%d.0/24", r.Int64()))

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := gssv1.NewNodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	for _, id := range []string{"sb-1", "sb-2"} {
		resp, err := c.CreateSandbox(ctx, &gssv1.NodeCreateRequest{SandboxId: id, Incarnation: 1})
		if err != nil {
			t.Fatalf("CreateSandbox %s: %v", id, err)
		}
		if resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
			t.Errorf("CreateSandbox %s started as %v, want COLD (the pool is off)", id, resp.GetStartedAs())
		}
	}
	stream, err := c.Exec(ctx, pyReq("sb-1", `print("hello")`))
	if err != nil {
		t.Fatal(err)
	}
	evs, err := recvAll(stream)
	if err != nil {
		t.Fatalf("Exec: %v (events %q)", err, evs)
	}
	var stdout string
	for _, ev := range evs[:max(len(evs)-1, 0)] {
		s, ok := strings.CutPrefix(ev, "stdout ")
		if !ok {
			t.Errorf("unexpected event %q", ev)
		}
		stdout += s
	}
	if len(evs) == 0 || evs[len(evs)-1] != "exit 0 false false" || stdout != "hello\n" {
		t.Errorf("Exec events %q; want stdout \"hello\\n\" then exit 0", evs)
	}
	if _, err := c.DestroySandbox(ctx, &gssv1.DestroySandboxRequest{SandboxId: "sb-1", Incarnation: 1}); err != nil {
		t.Fatal(err)
	}
	if got := n.listContainers(t); len(got) != 1 || got[0] != "sb-2_1_" {
		t.Errorf("runsc list after DestroySandbox sb-1 = %q, want [sb-2_1_]", got)
	}

	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.exited:
	case <-time.After(10 * time.Second):
		t.Fatalf("node agent still running 10 s after SIGTERM:\n%s", p.logs.String())
	}
	if p.err != nil {
		t.Errorf("node agent exit after SIGTERM: %v, want status 0", p.err)
	}
	if _, err := os.Stat(n.rt.Root); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("runsc root %s after shutdown: %v, want it gone", n.rt.Root, err)
	}
	if _, err := os.Stat(n.cgroupDir("sb-2_1_")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("cgroup of sb-2 after shutdown: %v, want it gone", err)
	}
	if out, err := exec.Command("nft", "list", "table", "inet", table).CombinedOutput(); err == nil {
		t.Errorf("table %s still there after shutdown:\n%s", table, out)
	}
	logs := p.logs.String()
	for _, want := range []string{
		`msg=create sandbox=sb-1 incarnation=1 mode=cold ms=`,
		`msg=destroy sandbox=sb-1 incarnation=1 ms=`,
		`msg="node agent stopped"`,
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("node agent log has no %q", want)
		}
	}
}
