//go:build integration && linux

package testenv

import (
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Target is the host that egress tests send sandbox traffic to; see
// EgressTarget.
type Target struct {
	NS          string // its network namespace
	Addr        string // its address; the HTTP servers answer on it
	OtherAddr   string // its second address, serving the same ports; tests keep it off their allowlists
	HostAddr    string // the host's address on the link to it
	AllowedPort string // an HTTP server that tests put on their allowlists
	BlockedPort string // an HTTP server that tests leave off
	HostPort    string // a TCP listener on every address of the host (the node itself), which no sandbox may reach
}

// The fixture's names and addresses are fixed, so no two tests that use it
// may run at once: such tests do not call t.Parallel, and
// `make test-integration` runs one package at a time (go test -p 1).
const (
	targetNS   = "gss-test-target"
	targetVeth = "gsstgt0"
)

// EgressTarget creates the target: a network namespace joined to the host
// by a veth, with addresses 10.210.0.2/30 and 10.210.0.6/30 (the host's
// ends are .1 and .5), HTTP servers on 8080 and 8081, and a TCP listener on
// the host's port 8099. What an earlier run left is removed first, and
// everything is removed at cleanup.
func EgressTarget(t testing.TB) Target {
	t.Helper()
	RequireRoot(t)
	tgt := Target{
		NS: targetNS, Addr: "10.210.0.2", OtherAddr: "10.210.0.6", HostAddr: "10.210.0.1",
		AllowedPort: "8080", BlockedPort: "8081", HostPort: "8099",
	}
	cleanup := func() {
		_ = exec.Command("ip", "netns", "del", targetNS).Run()
		_ = exec.Command("ip", "link", "del", targetVeth).Run()
	}
	cleanup()
	t.Cleanup(cleanup)
	MustRun(t, "ip", "netns", "add", targetNS)
	MustRun(t, "ip", "link", "add", targetVeth, "type", "veth", "peer", "name", "eth0", "netns", targetNS)
	MustRun(t, "ip", "addr", "add", tgt.HostAddr+"/30", "dev", targetVeth)
	MustRun(t, "ip", "addr", "add", "10.210.0.5/30", "dev", targetVeth)
	MustRun(t, "ip", "link", "set", targetVeth, "up")
	MustRun(t, "ip", "-n", targetNS, "addr", "add", tgt.Addr+"/30", "dev", "eth0")
	MustRun(t, "ip", "-n", targetNS, "addr", "add", tgt.OtherAddr+"/30", "dev", "eth0")
	MustRun(t, "ip", "-n", targetNS, "link", "set", "eth0", "up")
	MustRun(t, "ip", "-n", targetNS, "link", "set", "lo", "up")
	MustRun(t, "ip", "-n", targetNS, "route", "add", "default", "via", tgt.HostAddr, "dev", "eth0")

	for _, port := range []string{tgt.AllowedPort, tgt.BlockedPort} {
		cmd := exec.Command("ip", "netns", "exec", targetNS, "python3", "-m", "http.server", port, "--bind", "0.0.0.0")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start http.server %s: %v", port, err)
		}
		t.Cleanup(func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		})
		waitDial(t, net.JoinHostPort(tgt.Addr, port))
	}
	// The host reaches the second address too, so a sandbox that cannot is
	// stopped by its policy.
	waitDial(t, net.JoinHostPort(tgt.OtherAddr, tgt.AllowedPort))

	ln, err := net.Listen("tcp", ":"+tgt.HostPort)
	if err != nil {
		t.Fatalf("listen on :%s: %v", tgt.HostPort, err)
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
	return tgt
}

// waitDial waits up to 10 s for a TCP connection to addr from the host.
func waitDial(t testing.TB, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			c.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not answer: %v", addr, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// MustRun runs a host command and returns its combined output; it fails the
// test if the command fails.
func MustRun(t testing.TB, name string, args ...string) string {
	t.Helper()
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v: %s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}
