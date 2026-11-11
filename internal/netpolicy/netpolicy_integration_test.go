//go:build integration && linux

package netpolicy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
	"net"
	"net/netip"
	"os/exec"
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

const connectPy = `import socket, sys
try:
    socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=2).close()
    print("connected")
except socket.timeout:
    print("timeout")
except OSError as e:
    print("error", e)
`

// connect tries a TCP connection from inside the namespace at nsPath and
// returns "connected", "timeout" or "error ...".
func connect(t *testing.T, nsPath, addr, port string) string {
	t.Helper()
	ns := strings.TrimPrefix(nsPath, netnsDir)
	out := mustRun(t, "ip", "netns", "exec", ns, "python3", "-c", connectPy, addr, port)
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
	hostSide, _ := m.slotAddrs(m.byID["sb1"])

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
