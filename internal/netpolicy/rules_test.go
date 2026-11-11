package netpolicy

import (
	"context"
	"errors"
	"net/netip"
	"reflect"
	"strings"
	"testing"

	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bundle"
)

func fakeResolver(ctx context.Context, host string) ([]netip.Addr, error) {
	switch host {
	case "api.example":
		return []netip.Addr{netip.MustParseAddr("10.9.9.9"), netip.MustParseAddr("2001:db8::1"), netip.MustParseAddr("10.9.9.10")}, nil
	case "v6only.example":
		return []netip.Addr{netip.MustParseAddr("2001:db8::2")}, nil
	}
	return nil, errors.New("no such host")
}

func TestParseAllow(t *testing.T) {
	ok := []struct {
		in    []string
		rules []Rule
		hosts []bundle.HostEntry
	}{
		{[]string{"10.1.2.3:8080"}, []Rule{{netip.MustParsePrefix("10.1.2.3/32"), 8080}}, nil},
		{[]string{"10.0.0.0/8:443"}, []Rule{{netip.MustParsePrefix("10.0.0.0/8"), 443}}, nil},
		{[]string{"api.example:443"}, []Rule{
			{netip.MustParsePrefix("10.9.9.9/32"), 443},
			{netip.MustParsePrefix("10.9.9.10/32"), 443},
		}, []bundle.HostEntry{{IP: "10.9.9.9", Name: "api.example"}}},
		{[]string{"api.example:443", "api.example:80", "10.1.2.3:80", "10.1.2.3:80"}, []Rule{
			{netip.MustParsePrefix("10.9.9.9/32"), 443},
			{netip.MustParsePrefix("10.9.9.10/32"), 443},
			{netip.MustParsePrefix("10.9.9.9/32"), 80},
			{netip.MustParsePrefix("10.9.9.10/32"), 80},
			{netip.MustParsePrefix("10.1.2.3/32"), 80},
		}, []bundle.HostEntry{{IP: "10.9.9.9", Name: "api.example"}}},
		{[]string{"10.1.2.3/8:1"}, []Rule{{netip.MustParsePrefix("10.0.0.0/8"), 1}}, nil},
	}
	for _, c := range ok {
		rules, hosts, err := ParseAllow(c.in, fakeResolver)
		if err != nil {
			t.Errorf("ParseAllow(%q): %v", c.in, err)
			continue
		}
		if !reflect.DeepEqual(rules, c.rules) {
			t.Errorf("ParseAllow(%q) rules = %v, want %v", c.in, rules, c.rules)
		}
		if !reflect.DeepEqual(hosts, c.hosts) {
			t.Errorf("ParseAllow(%q) hosts = %v, want %v", c.in, hosts, c.hosts)
		}
	}

	bad := []struct{ in, want string }{
		{"nope:443", "nope"},
		{"x", ""},
		{"v6only.example:443", "v6only.example"},
		{"10.1.2.3:0", ""},
		{"10.1.2.3:65536", ""},
		{"10.1.2.3:", ""},
		{":443", ""},
		{"[2001:db8::1]:443", ""},
		{"2001:db8::/32:443", ""},
		{"10.1.2.3:http", ""},
		{"bad host:443", ""},
		{"-a.example:443", ""},
		{"10.0.0.0/33:443", ""},
	}
	for _, c := range bad {
		_, _, err := ParseAllow([]string{c.in}, fakeResolver)
		if err == nil {
			t.Errorf("ParseAllow(%q): want error", c.in)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("ParseAllow(%q) error %q does not name %q", c.in, err, c.want)
		}
	}
	if _, _, err := ParseAllow([]string{"api.example:443"}, nil); err == nil {
		t.Error("ParseAllow with a host name and a nil resolver: want error")
	}
}

func TestRenderInit(t *testing.T) {
	n := newNames("a1")
	got := renderInit(n, netip.MustParsePrefix("10.200.0.0/20"))
	want := `add table inet gss_a1
delete table inet gss_a1
table inet gss_a1 {
	map sandbox_out {
		type ifname : verdict
	}
	chain forward {
		type filter hook forward priority filter; policy accept;
		iifname "gf55f*" ct state established,related accept
		iifname vmap @sandbox_out
		iifname "gf55f*" counter drop
		oifname "gf55f*" ct state established,related accept
		oifname "gf55f*" counter drop
	}
	chain input {
		type filter hook input priority filter; policy accept;
		iifname "gf55f*" counter drop
	}
	chain postrouting {
		type nat hook postrouting priority srcnat; policy accept;
		ip saddr 10.200.0.0/20 oifname != "gf55f*" masquerade
	}
}
`
	if got != want {
		t.Errorf("renderInit:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderSetup(t *testing.T) {
	n := newNames("a1")
	rules := []Rule{
		{netip.MustParsePrefix("1.1.1.1/32"), 443},
		{netip.MustParsePrefix("10.0.0.0/8"), 8080},
	}
	got := renderSetup(n, "sb1", rules)
	want := `add chain inet gss_a1 sb_320983ce
flush chain inet gss_a1 sb_320983ce
add rule inet gss_a1 sb_320983ce ip daddr 1.1.1.1/32 tcp dport 443 accept
add rule inet gss_a1 sb_320983ce ip daddr 10.0.0.0/8 tcp dport 8080 accept
add element inet gss_a1 sandbox_out { "gf55f320983ce" : jump sb_320983ce }
`
	if got != want {
		t.Errorf("renderSetup:\n%s\nwant:\n%s", got, want)
	}

	gotTD := renderTeardown(n, "sb1")
	wantTD := `add chain inet gss_a1 sb_320983ce
add element inet gss_a1 sandbox_out { "gf55f320983ce" : jump sb_320983ce }
delete element inet gss_a1 sandbox_out { "gf55f320983ce" }
flush chain inet gss_a1 sb_320983ce
delete chain inet gss_a1 sb_320983ce
`
	if gotTD != wantTD {
		t.Errorf("renderTeardown:\n%s\nwant:\n%s", gotTD, wantTD)
	}
}

func TestNames(t *testing.T) {
	n := newNames("a1")
	v := n.veth("sb1")
	if len(v) != 13 || !strings.HasPrefix(v, n.prefix) || len(n.prefix) != 5 {
		t.Errorf("veth %q prefix %q: want 13 chars starting with the 5-char node prefix", v, n.prefix)
	}
	if v != "gf55f320983ce" {
		t.Errorf("veth = %q", v)
	}
	if got := n.netns("sb1"); got != "gss-a1-320983ce" {
		t.Errorf("netns = %q", got)
	}
	if got := n.chain("sb1"); got != "sb_320983ce" {
		t.Errorf("chain = %q", got)
	}
	if got := newNames("a-1").table; got != "gss_a_1" {
		t.Errorf("table for a-1 = %q, want gss_a_1", got)
	}
	if got := newNames("a.b_c").table; got != "gss_a_b_c" {
		t.Errorf("table for a.b_c = %q", got)
	}
	// A runsc id with characters that are not allowed in kernel names is only hashed.
	id := "sb-1a2b3c4d5e6f_1_"
	for _, s := range []string{n.veth(id), n.netns(id), n.chain(id)} {
		if strings.Contains(s, "1a2b3c4d5e6f") {
			t.Errorf("name %q contains the id", s)
		}
	}
	if !n.ownsNetns("gss-a1-0123abcd") || n.ownsNetns("gss-a1-target") || n.ownsNetns("gss-a1-0123abcd-x") {
		t.Error("ownsNetns matches the wrong names")
	}
	if !n.ownsLink(n.prefix+"0123abcd") || n.ownsLink(n.prefix+"0123") || n.ownsLink("eth0") {
		t.Error("ownsLink matches the wrong names")
	}
}
