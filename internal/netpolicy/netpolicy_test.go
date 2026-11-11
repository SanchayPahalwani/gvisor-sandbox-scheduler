package netpolicy

import (
	"context"
	"errors"
	"net/netip"
	"reflect"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// recorder is a Runner that records every command and answers from fail and
// out, keyed by a prefix of the command line.
type recorder struct {
	mu    sync.Mutex
	cmds  []string
	stdin []string
	fail  func(cmd string) error
	out   map[string]string
}

func (r *recorder) run(ctx context.Context, stdin, name string, args ...string) (string, error) {
	cmd := strings.Join(append([]string{name}, args...), " ")
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cmds = append(r.cmds, cmd)
	r.stdin = append(r.stdin, stdin)
	if r.fail != nil {
		if err := r.fail(cmd); err != nil {
			return "", err
		}
	}
	return r.out[cmd], nil
}

func (r *recorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cmds, r.stdin = nil, nil
}

func newTestManager(t *testing.T, cidr string, r *recorder) *Manager {
	t.Helper()
	m, err := New("a1", netip.MustParsePrefix(cidr), r.run)
	if err != nil {
		t.Fatal(err)
	}
	m.SetResolver(fakeResolver)
	return m
}

func TestNew(t *testing.T) {
	r := &recorder{}
	for _, c := range []struct{ node, cidr string }{
		{"", "10.200.0.0/20"},
		{"a/b", "10.200.0.0/20"},
		{"a b", "10.200.0.0/20"},
		{"..", "10.200.0.0/20"},
		{strings.Repeat("a", 64), "10.200.0.0/20"},
		{"a1", "10.200.0.0/31"},
		{"a1", "2001:db8::/64"},
	} {
		if _, err := New(c.node, netip.MustParsePrefix(c.cidr), r.run); err == nil {
			t.Errorf("New(%q, %s): want error", c.node, c.cidr)
		}
	}
	if _, err := New("a1", netip.Prefix{}, r.run); err == nil {
		t.Error("New with an invalid prefix: want error")
	}
	if _, err := New("t-1.x_y", netip.MustParsePrefix("10.200.0.0/30"), nil); err != nil {
		t.Errorf("New: %v", err)
	}
}

func TestSetupCommands(t *testing.T) {
	r := &recorder{}
	m := newTestManager(t, "10.200.0.0/20", r)
	path, hosts, err := m.Setup(context.Background(), "sb1", []string{"1.1.1.1:443", "10.0.0.0/8:8080"})
	if err != nil {
		t.Fatal(err)
	}
	if path != "/var/run/netns/gss-a1-320983ce" {
		t.Errorf("path = %q", path)
	}
	if len(hosts) != 0 {
		t.Errorf("hosts = %v, want none", hosts)
	}
	want := []string{
		"ip netns add gss-a1-320983ce",
		"ip link add gf55f320983ce type veth peer name eth0 netns gss-a1-320983ce",
		"ip addr add 10.200.0.1/30 dev gf55f320983ce",
		"ip link set gf55f320983ce up",
		"ip -n gss-a1-320983ce addr add 10.200.0.2/30 dev eth0",
		"ip -n gss-a1-320983ce link set eth0 up",
		"ip -n gss-a1-320983ce link set lo up",
		"ip -n gss-a1-320983ce route add default via 10.200.0.1 dev eth0",
		"nft -f -",
	}
	if !reflect.DeepEqual(r.cmds, want) {
		t.Fatalf("commands:\n%s\nwant:\n%s", strings.Join(r.cmds, "\n"), strings.Join(want, "\n"))
	}
	wantScript := renderSetup(m.names, "sb1", []Rule{
		{netip.MustParsePrefix("1.1.1.1/32"), 443},
		{netip.MustParsePrefix("10.0.0.0/8"), 8080},
	})
	if r.stdin[len(r.stdin)-1] != wantScript {
		t.Errorf("nft stdin:\n%s\nwant:\n%s", r.stdin[len(r.stdin)-1], wantScript)
	}

	_, hosts, err = m.Setup(context.Background(), "sb2", []string{"api.example:443"})
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 1 || hosts[0].IP != "10.9.9.9" || hosts[0].Name != "api.example" {
		t.Errorf("hosts = %v", hosts)
	}
	if _, _, err := m.Setup(context.Background(), "sb2", []string{"1.1.1.1:443"}); status.Code(err) != codes.AlreadyExists {
		t.Errorf("second Setup of sb2: %v, want AlreadyExists", err)
	}
}

func TestSetupBadInputCreatesNothing(t *testing.T) {
	r := &recorder{}
	m := newTestManager(t, "10.200.0.0/20", r)
	for _, allow := range [][]string{{"x"}, {"nope:443"}} {
		if _, _, err := m.Setup(context.Background(), "sb1", allow); status.Code(err) != codes.InvalidArgument {
			t.Errorf("Setup(%q): %v, want InvalidArgument", allow, err)
		}
	}
	if _, _, err := m.Setup(context.Background(), "", []string{"1.1.1.1:443"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("Setup with empty id: %v, want InvalidArgument", err)
	}
	if len(r.cmds) != 0 {
		t.Errorf("commands run for bad input: %v", r.cmds)
	}
}

func TestSetupRollback(t *testing.T) {
	r := &recorder{fail: func(cmd string) error {
		if strings.HasPrefix(cmd, "nft ") {
			return errors.New("nft: boom")
		}
		return nil
	}}
	m := newTestManager(t, "10.200.0.0/28", r)
	_, _, err := m.Setup(context.Background(), "sb1", []string{"1.1.1.1:443"})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Setup error = %v, want the nft failure", err)
	}
	has := func(c string) bool {
		for _, x := range r.cmds {
			if x == c {
				return true
			}
		}
		return false
	}
	for _, c := range []string{"ip link del gf55f320983ce", "ip netns del gss-a1-320983ce"} {
		if !has(c) {
			t.Errorf("rollback did not run %q; commands:\n%s", c, strings.Join(r.cmds, "\n"))
		}
	}
	// The subnet is free again: four more sandboxes fit in the /28.
	r.fail = nil
	for i, id := range []string{"s0", "s1", "s2", "s3"} {
		if _, _, err := m.Setup(context.Background(), id, []string{"1.1.1.1:443"}); err != nil {
			t.Fatalf("Setup %d after rollback: %v", i, err)
		}
	}
}

// hostAddr returns the host-side address from the recorded "ip addr add" command.
func hostAddr(t *testing.T, r *recorder) string {
	t.Helper()
	for _, c := range r.cmds {
		if strings.HasPrefix(c, "ip addr add ") {
			return strings.Fields(c)[3]
		}
	}
	t.Fatalf("no ip addr add in %v", r.cmds)
	return ""
}

func TestSubnetAllocation(t *testing.T) {
	r := &recorder{}
	m := newTestManager(t, "10.201.0.0/28", r)
	ctx := context.Background()
	for i, want := range []string{"10.201.0.1/30", "10.201.0.5/30", "10.201.0.9/30", "10.201.0.13/30"} {
		r.reset()
		if _, _, err := m.Setup(ctx, "s"+string(rune('0'+i)), []string{"1.1.1.1:443"}); err != nil {
			t.Fatal(err)
		}
		if got := hostAddr(t, r); got != want {
			t.Errorf("sandbox %d host address %s, want %s", i, got, want)
		}
	}
	r.reset()
	if _, _, err := m.Setup(ctx, "s4", []string{"1.1.1.1:443"}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("fifth Setup: %v, want ResourceExhausted", err)
	}
	if len(r.cmds) != 0 {
		t.Errorf("fifth Setup ran commands: %v", r.cmds)
	}
	if err := m.Teardown(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	r.reset()
	if _, _, err := m.Setup(ctx, "s4", []string{"1.1.1.1:443"}); err != nil {
		t.Fatal(err)
	}
	if got := hostAddr(t, r); got != "10.201.0.5/30" {
		t.Errorf("after Teardown host address %s, want 10.201.0.5/30", got)
	}
}

func TestTeardownIdempotent(t *testing.T) {
	r := &recorder{}
	m := newTestManager(t, "10.200.0.0/20", r)
	ctx := context.Background()
	if _, _, err := m.Setup(ctx, "sb1", []string{"1.1.1.1:443"}); err != nil {
		t.Fatal(err)
	}
	r.reset()
	if err := m.Teardown(ctx, "sb1"); err != nil {
		t.Fatal(err)
	}
	want := []string{"nft -f -", "ip link del gf55f320983ce", "ip netns del gss-a1-320983ce"}
	if !reflect.DeepEqual(r.cmds, want) {
		t.Errorf("Teardown commands %v, want %v", r.cmds, want)
	}
	if r.stdin[0] != renderTeardown(m.names, "sb1") {
		t.Errorf("Teardown nft script:\n%s", r.stdin[0])
	}

	// Everything already gone, including the table: still no error.
	r.fail = func(cmd string) error {
		switch {
		case strings.HasPrefix(cmd, "nft "):
			return errors.New("Error: No such file or directory")
		case strings.HasPrefix(cmd, "ip link del"):
			return errors.New(`Cannot find device "gf55f320983ce"`)
		case strings.HasPrefix(cmd, "ip netns del"):
			return errors.New(`Cannot remove namespace file "/var/run/netns/gss-a1-320983ce": No such file or directory`)
		}
		return nil
	}
	for i := 0; i < 2; i++ {
		if err := m.Teardown(ctx, "sb1"); err != nil {
			t.Errorf("Teardown %d of a missing sandbox: %v", i, err)
		}
	}

	// A real failure is reported.
	r.fail = func(cmd string) error {
		if strings.HasPrefix(cmd, "ip link del") {
			return errors.New("RTNETLINK answers: Operation not permitted")
		}
		return nil
	}
	if err := m.Teardown(ctx, "sb1"); err == nil {
		t.Error("Teardown with a failing link delete: want error")
	}
	// The table exists but the nft teardown fails: reported.
	r.fail = func(cmd string) error {
		if cmd == "nft -f -" {
			return errors.New("Error: Could not process rule: Device or resource busy")
		}
		return nil
	}
	if err := m.Teardown(ctx, "sb1"); err == nil {
		t.Error("Teardown with a failing nft step and an existing table: want error")
	}
}

func TestInitAndClose(t *testing.T) {
	r := &recorder{out: map[string]string{
		"ip netns list": "gss-a1-0123abcd (id: 3)\ngss-a1-target\ngss-other-0123abcd\ngss-a1-89abcdef\n",
		"ip -o link show": "1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536\n" +
			"7: gf55f0123abcd@if2: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500\n" +
			"8: gaaaa0123abcd@if2: <BROADCAST,MULTICAST> mtu 1500\n",
	}}
	m := newTestManager(t, "10.200.0.0/20", r)
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"ip netns list",
		"ip netns del gss-a1-0123abcd",
		"ip netns del gss-a1-89abcdef",
		"ip -o link show",
		"ip link del gf55f0123abcd",
		"sysctl -w net.ipv4.ip_forward=1",
		"nft -f -",
	}
	if !reflect.DeepEqual(r.cmds, want) {
		t.Fatalf("Init commands:\n%s\nwant:\n%s", strings.Join(r.cmds, "\n"), strings.Join(want, "\n"))
	}
	if r.stdin[len(r.stdin)-1] != renderInit(m.names, m.cidr) {
		t.Error("Init nft script differs from renderInit")
	}

	r.reset()
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if r.cmds[0] != "nft -f -" || r.stdin[0] != "add table inet gss_a1\ndelete table inet gss_a1\n" {
		t.Errorf("Close first command %q stdin %q", r.cmds[0], r.stdin[0])
	}
	if !reflect.DeepEqual(r.cmds[1:], want[:5]) {
		t.Errorf("Close cleanup commands %v", r.cmds[1:])
	}
}
