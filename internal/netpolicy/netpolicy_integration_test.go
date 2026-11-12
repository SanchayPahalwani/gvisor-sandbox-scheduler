//go:build integration && linux

package netpolicy

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
	"net"
	"net/netip"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/testenv"
)

const (
	targetNS   = "gss-test-target"
	targetVeth = "gsstgt0"
	targetHost = "10.210.0.1"
	targetAddr = "10.210.0.2"
)

func cmdOut(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	return string(out), err
}

func mustRun(t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := cmdOut(name, args...)
	if err != nil {
		t.Fatalf("%s %s: %v: %s", name, strings.Join(args, " "), err, out)
	}
	return out
}

// testManager returns a Manager for a random node id and a random 10.209.<n>.0/24,
// initialised, and closed when the test ends.
func testManager(t *testing.T) (*Manager, string) {
	t.Helper()
	b := make([]byte, 3)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	n, err := rand.Int(rand.Reader, big.NewInt(256))
	if err != nil {
		t.Fatal(err)
	}
	node := "t-" + hex.EncodeToString(b)
	m, err := New(node, netip.MustParsePrefix(fmt.Sprintf("10.209.%d.0/24", n.Int64())), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := m.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	return m, node
}

// targetFixture creates the target namespace joined to the host by a veth,
// with HTTP servers on 8080 and 8081, and a host listener on 8099.
func targetFixture(t *testing.T) {
	t.Helper()
	cleanup := func() {
		_, _ = cmdOut("ip", "netns", "del", targetNS)
		_, _ = cmdOut("ip", "link", "del", targetVeth)
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

	ln, err := net.Listen("tcp", ":8099")
	if err != nil {
		t.Fatalf("listen on :8099: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
}

// connectPy connects to argv[1]:argv[2] from source port argv[3] (0 = any).
// With a fourth argument it keeps the connection open for 10 minutes.
const connectPy = `import socket, sys, time
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
if int(sys.argv[3]):
    s.bind(("0.0.0.0", int(sys.argv[3])))
s.settimeout(2)
try:
    s.connect((sys.argv[1], int(sys.argv[2])))
    print("connected", flush=True)
    if len(sys.argv) > 4:
        time.sleep(600)
except socket.timeout:
    print("timeout")
except OSError as e:
    print("error", e)
`

func nsName(nsPath string) string { return strings.TrimPrefix(nsPath, netnsDir) }

// connect tries a TCP connection from inside the namespace at nsPath and
// returns "connected", "timeout" or "error ...".
func connect(t *testing.T, nsPath, addr, port string) string {
	t.Helper()
	return connectFrom(t, nsPath, addr, port, 0)
}

func connectFrom(t *testing.T, nsPath, addr, port string, srcPort int) string {
	t.Helper()
	out := mustRun(t, "ip", "netns", "exec", nsName(nsPath), "python3", "-c", connectPy, addr, port, fmt.Sprint(srcPort))
	return strings.TrimSpace(out)
}

func TestEgressAllowlist(t *testing.T) {
	testenv.RequireRoot(t)
	targetFixture(t)
	m, node := testManager(t)
	ctx := context.Background()

	path, hosts, err := m.Setup(ctx, "sb1", []string{targetAddr + ":8080"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Teardown(context.Background(), "sb1") })
	if len(hosts) != 0 {
		t.Errorf("hosts = %v", hosts)
	}
	if want := netnsDir + m.names.netns("sb1"); path != want {
		t.Errorf("path = %q, want %q", path, want)
	}
	hostSide, _ := m.slotAddrs(m.sbs["sb1"].slot)

	if got := connect(t, path, targetAddr, "8080"); got != "connected" {
		t.Errorf("%s:8080 (allowed): %s", targetAddr, got)
	}
	if got := connect(t, path, targetAddr, "8081"); got != "timeout" {
		t.Errorf("%s:8081 (not allowed): %s, want timeout", targetAddr, got)
	}
	if got := connect(t, path, hostSide.String(), "8099"); got != "timeout" {
		t.Errorf("host veth %s:8099: %s, want timeout", hostSide, got)
	}

	if err := m.Teardown(ctx, "sb1"); err != nil {
		t.Fatal(err)
	}
	table := mustRun(t, "nft", "list", "table", "inet", m.names.table)
	if strings.Contains(table, "sb_") {
		t.Errorf("table still has a sandbox chain after Teardown:\n%s", table)
	}
	if out := mustRun(t, "ip", "netns", "list"); strings.Contains(out, "gss-"+node+"-") {
		t.Errorf("namespace left after Teardown:\n%s", out)
	}
	if out := mustRun(t, "ip", "-o", "link", "show"); strings.Contains(out, m.names.prefix) {
		t.Errorf("veth left after Teardown:\n%s", out)
	}
	if err := m.Teardown(ctx, "sb1"); err != nil {
		t.Errorf("second Teardown: %v", err)
	}

	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if out, err := cmdOut("nft", "list", "table", "inet", m.names.table); err == nil {
		t.Errorf("table still exists after Close:\n%s", out)
	}
	if err := m.Teardown(ctx, "sb1"); err != nil {
		t.Errorf("Teardown after Close: %v", err)
	}
}

func TestInitRemovesLeftovers(t *testing.T) {
	testenv.RequireRoot(t)
	m1, node := testManager(t)
	ctx := context.Background()
	for _, id := range []string{"sb-a", "sb-b"} {
		if _, _, err := m1.Setup(ctx, id, []string{"10.210.0.2:8080"}); err != nil {
			t.Fatal(err)
		}
	}
	// Drop m1 without Teardown; a new Manager for the same node cleans up.
	m2, err := New(node, m1.cidr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if out := mustRun(t, "ip", "netns", "list"); strings.Contains(out, "gss-"+node+"-") {
		t.Errorf("namespaces left after Init:\n%s", out)
	}
	if out := mustRun(t, "ip", "-o", "link", "show"); strings.Contains(out, m2.names.prefix) {
		t.Errorf("veths left after Init:\n%s", out)
	}
	table := mustRun(t, "nft", "list", "table", "inet", m2.names.table)
	if strings.Contains(table, "sb_") || strings.Contains(table, ": jump") {
		t.Errorf("sandbox rules left after Init:\n%s", table)
	}
	// The allocator starts empty again.
	if _, _, err := m2.Setup(ctx, "sb-a", []string{"10.210.0.2:8080"}); err != nil {
		t.Fatalf("Setup after Init: %v", err)
	}
	if err := m2.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestStaleFlowNotInherited replays the review finding: a flow opened by an
// earlier sandbox on the same /30 must not be usable by the next sandbox.
func TestStaleFlowNotInherited(t *testing.T) {
	testenv.RequireRoot(t)
	targetFixture(t)
	m, _ := testManager(t)
	ctx := context.Background()

	oldPath, _, err := m.Setup(ctx, "old", []string{targetAddr + ":8081"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Teardown(context.Background(), "old") })
	_, oldGuest := m.slotAddrs(m.sbs["old"].slot)

	// The old sandbox holds a connection to 8081 from source port 40001.
	hold := exec.Command("ip", "netns", "exec", nsName(oldPath), "python3", "-c", connectPy, targetAddr, "8081", "40001", "hold")
	stdout, err := hold.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := hold.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hold.Process.Kill(); _ = hold.Wait() })
	line, _ := bufio.NewReader(stdout).ReadString('\n')
	if strings.TrimSpace(line) != "connected" {
		t.Fatalf("old sandbox holding connection: %q", line)
	}

	// Teardown in the package's order while the flow is open, then the process dies.
	if err := m.Teardown(ctx, "old"); err != nil {
		t.Fatal(err)
	}
	_ = hold.Process.Kill()
	_ = hold.Wait()

	// The target forgets the flow without the host seeing a FIN or RST.
	mustRun(t, "ip", "netns", "exec", targetNS, "nft", "add", "table", "inet", "gss_test_tgtdrop")
	t.Cleanup(func() {
		_, _ = cmdOut("ip", "netns", "exec", targetNS, "nft", "delete", "table", "inet", "gss_test_tgtdrop")
	})
	mustRun(t, "ip", "netns", "exec", targetNS, "nft", "add", "chain", "inet", "gss_test_tgtdrop", "out",
		"{ type filter hook output priority filter; policy accept; }")
	mustRun(t, "ip", "netns", "exec", targetNS, "nft", "add", "rule", "inet", "gss_test_tgtdrop", "out", "tcp", "sport", "8081", "drop")
	mustRun(t, "ip", "netns", "exec", targetNS, "ss", "-K", "state", "established", "( sport = :8081 )")
	mustRun(t, "ip", "netns", "exec", targetNS, "nft", "delete", "table", "inet", "gss_test_tgtdrop")

	newPath, _, err := m.Setup(ctx, "new", []string{targetAddr + ":8080"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Teardown(context.Background(), "new") })
	if _, g := m.slotAddrs(m.sbs["new"].slot); g != oldGuest {
		t.Fatalf("new sandbox got %s, want the old sandbox's %s", g, oldGuest)
	}
	if got := connectFrom(t, newPath, targetAddr, "8080", 40002); got != "connected" {
		t.Errorf("new sandbox -> 8080 (allowed): %s", got)
	}
	if got := connectFrom(t, newPath, targetAddr, "8081", 40002); got != "timeout" {
		t.Errorf("new sandbox -> 8081 from a fresh port: %s, want timeout", got)
	}
	if got := connectFrom(t, newPath, targetAddr, "8081", 40001); got != "timeout" {
		t.Errorf("new sandbox -> 8081 from the old sandbox's port: %s, want timeout", got)
	}
}

func TestNoSandboxToSandbox(t *testing.T) {
	testenv.RequireRoot(t)
	m, _ := testManager(t)
	ctx := context.Background()
	bPath, _, err := m.Setup(ctx, "b", []string{m.cidr.String() + ":9000"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Teardown(context.Background(), "b") })
	cPath, _, err := m.Setup(ctx, "c", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Teardown(context.Background(), "c") })
	_, cGuest := m.slotAddrs(m.sbs["c"].slot)

	srv := exec.Command("ip", "netns", "exec", nsName(cPath), "python3", "-m", "http.server", "9000", "--bind", "0.0.0.0")
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Process.Kill(); _ = srv.Wait() })
	deadline := time.Now().Add(10 * time.Second)
	for connect(t, cPath, cGuest.String(), "9000") != "connected" {
		if time.Now().After(deadline) {
			t.Fatal("server in sandbox c did not start")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := connect(t, bPath, cGuest.String(), "9000"); got != "timeout" {
		t.Errorf("sandbox b -> sandbox c %s:9000 (b's list covers %s): %s, want timeout", cGuest, m.cidr, got)
	}
}

const udpRecvPy = `import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("0.0.0.0", int(sys.argv[1])))
print("ready", flush=True)
s.settimeout(4)
try:
    while True:
        d, _ = s.recvfrom(100)
        print("got", d.decode(), flush=True)
except socket.timeout:
    pass
`

const udpSendPy = `import socket, sys
socket.socket(socket.AF_INET, socket.SOCK_DGRAM).sendto(sys.argv[3].encode(), (sys.argv[1], int(sys.argv[2])))
`

func TestUDPAndICMPDropped(t *testing.T) {
	testenv.RequireRoot(t)
	targetFixture(t)
	m, _ := testManager(t)
	ctx := context.Background()
	path, _, err := m.Setup(ctx, "sb1", []string{targetAddr + ":8080"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Teardown(context.Background(), "sb1") })

	recv := exec.Command("ip", "netns", "exec", targetNS, "python3", "-c", udpRecvPy, "8080")
	stdout, err := recv.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := recv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recv.Process.Kill(); _ = recv.Wait() })
	rd := bufio.NewReader(stdout)
	if line, _ := rd.ReadString('\n'); strings.TrimSpace(line) != "ready" {
		t.Fatalf("udp receiver: %q", line)
	}
	mustRun(t, "ip", "netns", "exec", nsName(path), "python3", "-c", udpSendPy, targetAddr, "8080", "sandbox")
	mustRun(t, "python3", "-c", udpSendPy, targetAddr, "8080", "host") // control: not through the sandbox
	var got []string
	for {
		line, err := rd.ReadString('\n')
		if line != "" {
			got = append(got, strings.TrimSpace(line))
		}
		if err != nil {
			break
		}
	}
	if !slices.Contains(got, "got host") {
		t.Errorf("control datagram from the host not received: %v", got)
	}
	if slices.Contains(got, "got sandbox") {
		t.Errorf("UDP from the sandbox to an allowed address and port was delivered: %v", got)
	}

	if out, err := cmdOut("ping", "-c", "1", "-W", "2", targetAddr); err != nil {
		t.Errorf("control ping from the host failed: %v: %s", err, out)
	}
	if out, err := cmdOut("ip", "netns", "exec", nsName(path), "ping", "-c", "1", "-W", "2", targetAddr); err == nil {
		t.Errorf("ping from the sandbox to an allowed address succeeded:\n%s", out)
	}
}

func TestInitRefusesOverlappingNode(t *testing.T) {
	testenv.RequireRoot(t)
	m1, node1 := testManager(t)
	m2, err := New(node1+"-x", m1.cidr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.Init(context.Background()); err == nil {
		_ = m2.Close(context.Background())
		t.Fatal("Init of a node with an overlapping range: want error")
	} else {
		t.Logf("Init refused: %v", err)
	}
	table := mustRun(t, "nft", "-t", "list", "table", "inet", m1.names.table)
	if !strings.Contains(table, tableComment(m1.names, m1.cidr)) || !strings.Contains(table, "chain forward") {
		t.Errorf("first node's table changed:\n%s", table)
	}
	if out, err := cmdOut("nft", "list", "table", "inet", m2.names.table); err == nil {
		t.Errorf("refused node created its table:\n%s", out)
	}
}
