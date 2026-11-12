//go:build integration && linux

package nodeagent

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"net/netip"
	"strings"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/netpolicy"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/testenv"
)

// egressPolicy returns a real network policy for node, initialised on a
// random 10.208.<n>.0/24 and closed at cleanup. It resolves target.test to
// tgt.Addr and no other name.
func egressPolicy(t *testing.T, node string, tgt testenv.Target) *netpolicy.Manager {
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
			return []netip.Addr{netip.MustParseAddr(tgt.Addr)}, nil
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
func connectCode(addr, port string) string {
	return fmt.Sprintf(`import socket
s = socket.socket()
s.settimeout(2)
try:
    s.connect((%q, %s))
    print("connected")
except socket.timeout:
    print("timeout")
except OSError as e:
    print("error", e)
`, addr, port)
}

// TestRealEgressSandbox runs probes from inside gVisor sandboxes on a node
// with a real network policy. The target fixture uses fixed names and
// addresses (see testenv.EgressTarget): this test must not run in parallel
// with another that uses it.
func TestRealEgressSandbox(t *testing.T) {
	n := newRealNode(t)
	tgt := testenv.EgressTarget(t)
	np := egressPolicy(t, n.id, tgt)
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

	egress := &gssv1.SandboxSpec{EgressAllow: []string{tgt.Addr + ":" + tgt.AllowedPort, "target.test:" + tgt.AllowedPort}}
	if _, err := m.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: "eg-1", Incarnation: 1, Spec: egress}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: "none-1", Incarnation: 1}); err != nil {
		t.Fatal(err)
	}

	// The allowed address, by its pinned name and by address.
	get := pyReq("eg-1", fmt.Sprintf(`import urllib.request
print(urllib.request.urlopen("http://target.test:%s/", timeout=5).status)`, tgt.AllowedPort))
	r := runExec(ctx, m, get, nil)
	if exit := r.ok(t, "GET http://target.test/"); exit.GetCode() != 0 || r.stdout.String() != "200\n" {
		t.Errorf("GET http://target.test:%s/: exit %v, stdout %q, stderr %q; want 0 and \"200\\n\"", tgt.AllowedPort, exit, r.stdout.String(), r.stderr.String())
	}
	r = runExec(ctx, m, pyReq("eg-1", connectCode(tgt.Addr, tgt.AllowedPort)), nil)
	if r.ok(t, "connect to the allowed address"); strings.TrimSpace(r.stdout.String()) != "connected" {
		t.Errorf("%s:%s (allowed): %q (stderr %q), want connected", tgt.Addr, tgt.AllowedPort, r.stdout.String(), r.stderr.String())
	}
	// The fixture checked that the host reaches each of these.
	for _, p := range []struct{ what, addr, port string }{
		{"the allowed address on a port not on the list", tgt.Addr, tgt.BlockedPort},
		{"an address not on the list, on the allowed port", tgt.OtherAddr, tgt.AllowedPort},
		{"a listener on the host (the node itself)", tgt.HostAddr, tgt.HostPort},
	} {
		r := runExec(ctx, m, pyReq("eg-1", connectCode(p.addr, p.port)), nil)
		if r.ok(t, p.what); strings.TrimSpace(r.stdout.String()) != "timeout" {
			t.Errorf("%s, %s:%s: %q (stderr %q), want timeout", p.what, p.addr, p.port, r.stdout.String(), r.stderr.String())
		}
	}
	r = runExec(ctx, m, pyReq("none-1", connectCode(tgt.Addr, tgt.AllowedPort)), nil)
	if r.ok(t, "connect without egress"); !strings.Contains(r.stdout.String(), "Network is unreachable") {
		t.Errorf("%s:%s from a sandbox without egress: %q (stderr %q), want \"Network is unreachable\"", tgt.Addr, tgt.AllowedPort, r.stdout.String(), r.stderr.String())
	}

	// netpolicy names the namespace gss-<node>-<hash> and the chain sb_<hash>,
	// in the table gss_<node> (node IDs here hold only letters and digits).
	nsPrefix, table := "gss-"+n.id+"-", "gss_"+n.id
	if out := testenv.MustRun(t, "ip", "netns", "list"); !strings.Contains(out, nsPrefix) {
		t.Fatalf("no namespace %s* for the egress sandbox:\n%s", nsPrefix, out)
	}
	if out := testenv.MustRun(t, "nft", "list", "table", "inet", table); !strings.Contains(out, "chain sb_") {
		t.Fatalf("no sandbox chain in table %s:\n%s", table, out)
	}
	if err := m.Destroy(ctx, "eg-1", 0); err != nil {
		t.Fatal(err)
	}
	if out := testenv.MustRun(t, "ip", "netns", "list"); strings.Contains(out, nsPrefix) {
		t.Errorf("namespace left after Destroy:\n%s", out)
	}
	if out := testenv.MustRun(t, "nft", "list", "table", "inet", table); strings.Contains(out, "chain sb_") {
		t.Errorf("sandbox chain left after Destroy:\n%s", out)
	}
}
