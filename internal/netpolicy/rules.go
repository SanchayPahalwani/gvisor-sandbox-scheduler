// Package netpolicy gives a sandbox an egress allowlist: a network namespace, a
// veth pair with a /30 from the node's range, and nftables rules that let the
// sandbox open TCP connections to the allowed addresses and ports and nothing
// else (spec section 6.5, call D12).
package netpolicy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/netip"
	"strconv"
	"strings"

	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bundle"
)

// Rule allows TCP to every address in Prefix on Port.
type Rule struct {
	Prefix netip.Prefix
	Port   uint16
}

// ParseAllow turns "host:port" | "ip:port" | "cidr:port" into rules, resolving host
// names with resolve (IPv4 results only). Each resolved name is also returned as a host entry.
func ParseAllow(entries []string, resolve func(ctx context.Context, host string) ([]netip.Addr, error)) ([]Rule, []bundle.HostEntry, error) {
	return parseAllow(context.Background(), entries, resolve)
}

func parseAllow(ctx context.Context, entries []string, resolve func(ctx context.Context, host string) ([]netip.Addr, error)) ([]Rule, []bundle.HostEntry, error) {
	var rules []Rule
	var hosts []bundle.HostEntry
	seenRule := map[Rule]bool{}
	seenHost := map[string]bool{}
	add := func(r Rule) {
		if !seenRule[r] {
			seenRule[r] = true
			rules = append(rules, r)
		}
	}
	for _, e := range entries {
		i := strings.LastIndexByte(e, ':')
		if i < 0 {
			return nil, nil, fmt.Errorf("egress entry %q: want host:port, ip:port or cidr:port", e)
		}
		host, portStr := e[:i], e[i+1:]
		port, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil || port == 0 {
			return nil, nil, fmt.Errorf("egress entry %q: port %q is not in 1-65535", e, portStr)
		}
		if strings.Contains(host, ":") {
			return nil, nil, fmt.Errorf("egress entry %q: only IPv4 is supported", e)
		}
		switch {
		case strings.Contains(host, "/"):
			p, err := netip.ParsePrefix(host)
			if err != nil || !p.Addr().Is4() {
				return nil, nil, fmt.Errorf("egress entry %q: %q is not an IPv4 CIDR", e, host)
			}
			add(Rule{p.Masked(), uint16(port)})
		case strings.Trim(host, "0123456789.") == "":
			a, err := netip.ParseAddr(host)
			if err != nil || !a.Is4() {
				return nil, nil, fmt.Errorf("egress entry %q: %q is not an IPv4 address", e, host)
			}
			add(Rule{netip.PrefixFrom(a, 32), uint16(port)})
		default:
			if !validHostname(host) {
				return nil, nil, fmt.Errorf("egress entry %q: %q is not a valid host name", e, host)
			}
			if resolve == nil {
				return nil, nil, fmt.Errorf("egress entry %q: no resolver for host %q", e, host)
			}
			addrs, err := resolve(ctx, host)
			if err != nil {
				return nil, nil, fmt.Errorf("egress entry %q: resolve %s: %w", e, host, err)
			}
			var v4 []netip.Addr
			for _, a := range addrs {
				if a = a.Unmap(); a.Is4() {
					v4 = append(v4, a)
				}
			}
			if len(v4) == 0 {
				return nil, nil, fmt.Errorf("egress entry %q: %s has no IPv4 address", e, host)
			}
			for _, a := range v4 {
				add(Rule{netip.PrefixFrom(a, 32), uint16(port)})
			}
			if !seenHost[host] {
				seenHost[host] = true
				hosts = append(hosts, bundle.HostEntry{IP: v4[0].String(), Name: host})
			}
		}
	}
	return rules, hosts, nil
}

// validHostname checks RFC 1123 labels: [A-Za-z0-9-], 1-63 characters, no
// leading or trailing hyphen, 253 characters in total.
func validHostname(h string) bool {
	if h == "" || len(h) > 253 {
		return false
	}
	for _, l := range strings.Split(h, ".") {
		if len(l) < 1 || len(l) > 63 || l[0] == '-' || l[len(l)-1] == '-' {
			return false
		}
		for _, c := range l {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}

func hexHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// names derives every kernel object name for one node. Sandbox ids are only
// ever hashed into names, never used directly.
type names struct {
	nodeID string
	table  string // gss_<node id with non-alphanumerics as _>
	prefix string // "g" + 4 hex of sha256(node id); every host veth starts with it
}

func newNames(nodeID string) names {
	t := []byte(nodeID)
	for i, c := range t {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
			t[i] = '_'
		}
	}
	return names{nodeID: nodeID, table: "gss_" + string(t), prefix: "g" + hexHash(nodeID)[:4]}
}

func idHash(id string) string { return hexHash(id)[:8] }

func (n names) veth(id string) string  { return n.prefix + idHash(id) }
func (n names) netns(id string) string { return "gss-" + n.nodeID + "-" + idHash(id) }
func (n names) chain(id string) string { return "sb_" + idHash(id) }

func isHex8(s string) bool {
	if len(s) != 8 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// ownsNetns reports whether a namespace name is one this node creates.
func (n names) ownsNetns(name string) bool {
	rest, ok := strings.CutPrefix(name, "gss-"+n.nodeID+"-")
	return ok && isHex8(rest)
}

// ownsLink reports whether a link name is a host veth this node creates.
func (n names) ownsLink(name string) bool {
	rest, ok := strings.CutPrefix(name, n.prefix)
	return ok && isHex8(rest)
}

// renderInit is the nft script that replaces this node's table with an empty one.
func renderInit(n names, cidr netip.Prefix) string {
	var b strings.Builder
	fmt.Fprintf(&b, "add table inet %s\ndelete table inet %s\n", n.table, n.table)
	fmt.Fprintf(&b, "table inet %s {\n", n.table)
	b.WriteString("\tmap sandbox_out {\n\t\ttype ifname : verdict\n\t}\n")
	b.WriteString("\tchain forward {\n\t\ttype filter hook forward priority filter; policy accept;\n")
	fmt.Fprintf(&b, "\t\tiifname \"%s*\" ct state established,related accept\n", n.prefix)
	b.WriteString("\t\tiifname vmap @sandbox_out\n")
	fmt.Fprintf(&b, "\t\tiifname \"%s*\" counter drop\n", n.prefix)
	fmt.Fprintf(&b, "\t\toifname \"%s*\" ct state established,related accept\n", n.prefix)
	fmt.Fprintf(&b, "\t\toifname \"%s*\" counter drop\n", n.prefix)
	b.WriteString("\t}\n")
	b.WriteString("\tchain input {\n\t\ttype filter hook input priority filter; policy accept;\n")
	fmt.Fprintf(&b, "\t\tiifname \"%s*\" counter drop\n", n.prefix)
	b.WriteString("\t}\n")
	b.WriteString("\tchain postrouting {\n\t\ttype nat hook postrouting priority srcnat; policy accept;\n")
	fmt.Fprintf(&b, "\t\tip saddr %s oifname != \"%s*\" masquerade\n", cidr, n.prefix)
	b.WriteString("\t}\n}\n")
	return b.String()
}

// renderDeleteTable deletes this node's table whether or not it exists.
func renderDeleteTable(n names) string {
	return fmt.Sprintf("add table inet %s\ndelete table inet %s\n", n.table, n.table)
}

// renderSetup adds the sandbox's chain and maps its veth to it.
func renderSetup(n names, id string, rules []Rule) string {
	var b strings.Builder
	c := n.chain(id)
	fmt.Fprintf(&b, "add chain inet %s %s\n", n.table, c)
	fmt.Fprintf(&b, "flush chain inet %s %s\n", n.table, c)
	for _, r := range rules {
		fmt.Fprintf(&b, "add rule inet %s %s ip daddr %s tcp dport %d accept\n", n.table, c, r.Prefix, r.Port)
	}
	fmt.Fprintf(&b, "add element inet %s sandbox_out { \"%s\" : jump %s }\n", n.table, n.veth(id), c)
	return b.String()
}

// renderTeardown removes the sandbox's map element and chain. It first adds
// both so that it succeeds whether or not they exist (the table must exist).
func renderTeardown(n names, id string) string {
	var b strings.Builder
	c, v := n.chain(id), n.veth(id)
	fmt.Fprintf(&b, "add chain inet %s %s\n", n.table, c)
	fmt.Fprintf(&b, "add element inet %s sandbox_out { \"%s\" : jump %s }\n", n.table, v, c)
	fmt.Fprintf(&b, "delete element inet %s sandbox_out { \"%s\" }\n", n.table, v)
	fmt.Fprintf(&b, "flush chain inet %s %s\n", n.table, c)
	fmt.Fprintf(&b, "delete chain inet %s %s\n", n.table, c)
	return b.String()
}
