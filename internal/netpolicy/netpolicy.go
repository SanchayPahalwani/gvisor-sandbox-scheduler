package netpolicy

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bundle"
)

// Runner runs a host command; tests replace it.
type Runner func(ctx context.Context, stdin string, name string, args ...string) (stdout string, err error)

// guestIf is the interface name inside the sandbox's namespace; gVisor takes it over.
const guestIf = "eth0"

// netnsDir is where `ip netns add` puts namespace files.
const netnsDir = "/var/run/netns/"

// nodeIDRE limits node ids to [A-Za-z0-9-] so that the table name (with - as _)
// is distinct for distinct node ids.
var nodeIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,31}$`)

// sandbox is one sandbox's network: its /30 and the generation its flows are marked with.
type sandbox struct {
	slot int
	gen  uint32
}

// Manager owns this node's nftables table, sandbox namespaces and veths.
type Manager struct {
	names names
	cidr  netip.Prefix
	slots int // number of /30s in cidr
	run   Runner

	// life is held shared by Setup and Teardown for their whole run and
	// exclusively by Init and Close, so those never interleave with a
	// sandbox's commands.
	life sync.RWMutex

	mu      sync.Mutex
	closed  bool // set by Close; Setup and Init then fail
	resolve func(ctx context.Context, host string) ([]netip.Addr, error)
	gen     uint32             // last generation issued; never 0 once issued
	used    map[int]string     // slot -> sandbox id
	sbs     map[string]sandbox // sandbox id -> its network
}

// New returns a Manager for nodeID handing out /30s from cidr (e.g. 10.200.0.0/20).
// A nil run executes real commands.
func New(nodeID string, cidr netip.Prefix, run Runner) (*Manager, error) {
	if !nodeIDRE.MatchString(nodeID) {
		return nil, fmt.Errorf("netpolicy: invalid node id %q: want [A-Za-z0-9][A-Za-z0-9-]{0,31}", nodeID)
	}
	if !cidr.IsValid() || !cidr.Addr().Is4() || cidr.Bits() > 30 {
		return nil, fmt.Errorf("netpolicy: egress range %v must be an IPv4 prefix of /30 or larger", cidr)
	}
	if run == nil {
		run = execRunner
	}
	bits := 30 - cidr.Bits()
	if bits > 20 {
		bits = 20 // more than a million sandboxes per node is not a real limit
	}
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, fmt.Errorf("netpolicy: random generation: %w", err)
	}
	return &Manager{
		names:   newNames(nodeID),
		cidr:    cidr.Masked(),
		slots:   1 << bits,
		run:     run,
		resolve: defaultResolve,
		gen:     binary.BigEndian.Uint32(b[:]),
		used:    map[int]string{},
		sbs:     map[string]sandbox{},
	}, nil
}

func defaultResolve(ctx context.Context, host string) ([]netip.Addr, error) {
	return net.DefaultResolver.LookupNetIP(ctx, "ip4", host)
}

// SetResolver replaces the host name resolver (default net.DefaultResolver).
func (m *Manager) SetResolver(resolve func(ctx context.Context, host string) ([]netip.Addr, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resolve = resolve
}

// Init removes this node's leftovers, enables net.ipv4.ip_forward and creates
// the table. It refuses, changing nothing, when a table with this node's name
// belongs to another node or another node's table covers an overlapping range.
func (m *Manager) Init(ctx context.Context) error {
	m.life.Lock()
	defer m.life.Unlock()
	if m.isClosed() {
		return status.Error(codes.FailedPrecondition, "netpolicy: manager is closed")
	}
	if err := m.checkOtherNodes(ctx); err != nil {
		return err
	}
	if err := m.removeLeftovers(ctx); err != nil {
		return err
	}
	m.resetSlots()
	if _, err := m.run(ctx, "", "sysctl", "-w", "net.ipv4.ip_forward=1"); err != nil {
		return fmt.Errorf("netpolicy: enable forwarding: %w", err)
	}
	if _, err := m.run(ctx, renderInit(m.names, m.cidr), "nft", "-f", "-"); err != nil {
		return fmt.Errorf("netpolicy: create table %s: %w", m.names.table, err)
	}
	return nil
}

// checkOtherNodes reads the owner comment of every inet gss_* table.
func (m *Manager) checkOtherNodes(ctx context.Context) error {
	out, err := m.run(ctx, "", "nft", "list", "tables")
	if err != nil {
		return fmt.Errorf("netpolicy: list tables: %w", err)
	}
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) != 3 || f[0] != "table" || f[1] != "inet" || !strings.HasPrefix(f[2], "gss_") {
			continue
		}
		listing, err := m.run(ctx, "", "nft", "-t", "list", "table", "inet", f[2])
		if err != nil {
			if notFound(err) {
				continue // deleted since `nft list tables` (another node's Close)
			}
			return fmt.Errorf("netpolicy: list table %s: %w", f[2], err)
		}
		node, cidr, ok := parseComment(listing)
		if !ok || node == m.names.nodeID {
			continue
		}
		if f[2] == m.names.table {
			return status.Errorf(codes.FailedPrecondition, "netpolicy: table %s belongs to node %q", f[2], node)
		}
		if cidr.Overlaps(m.cidr) {
			return status.Errorf(codes.FailedPrecondition, "netpolicy: egress range %s overlaps %s of node %q (table %s)", m.cidr, cidr, node, f[2])
		}
		if p := newNames(node).prefix; p == m.names.prefix {
			return status.Errorf(codes.FailedPrecondition, "netpolicy: nodes %q and %q share the veth prefix %s", m.names.nodeID, node, p)
		}
	}
	return nil
}

// Close removes every namespace and veth of this node and then the table. If
// removing them fails the table stays (running sandboxes stay filtered) and
// the error is returned. net.ipv4.ip_forward stays on: it is VM-wide and other
// nodes rely on it.
func (m *Manager) Close(ctx context.Context) error {
	m.life.Lock()
	defer m.life.Unlock()
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	if err := m.removeLeftovers(ctx); err != nil {
		return err
	}
	if _, err := m.run(ctx, renderDeleteTable(m.names), "nft", "-f", "-"); err != nil {
		return fmt.Errorf("netpolicy: delete table %s: %w", m.names.table, err)
	}
	m.resetSlots()
	return nil
}

func (m *Manager) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *Manager) resetSlots() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.used = map[int]string{}
	m.sbs = map[string]sandbox{}
}

// listNetns returns the names of all network namespaces.
func (m *Manager) listNetns(ctx context.Context) ([]string, error) {
	out, err := m.run(ctx, "", "ip", "netns", "list")
	if err != nil {
		return nil, fmt.Errorf("netpolicy: list namespaces: %w", err)
	}
	var names []string
	for _, line := range strings.Split(out, "\n") {
		if f := strings.Fields(line); len(f) > 0 {
			names = append(names, f[0])
		}
	}
	return names, nil
}

// listLinks returns the names of all links in the host namespace.
func (m *Manager) listLinks(ctx context.Context) ([]string, error) {
	out, err := m.run(ctx, "", "ip", "-o", "link", "show")
	if err != nil {
		return nil, fmt.Errorf("netpolicy: list links: %w", err)
	}
	var names []string
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		name, _, _ := strings.Cut(strings.TrimSuffix(f[1], ":"), "@")
		names = append(names, name)
	}
	return names, nil
}

// removeLeftovers deletes every namespace and host veth that carries this node's names.
func (m *Manager) removeLeftovers(ctx context.Context) error {
	var errs []error
	nss, err := m.listNetns(ctx)
	if err != nil {
		return err
	}
	for _, ns := range nss {
		if !m.names.ownsNetns(ns) {
			continue
		}
		if _, err := m.run(ctx, "", "ip", "netns", "del", ns); err != nil && !notFound(err) {
			errs = append(errs, fmt.Errorf("netpolicy: delete namespace %s: %w", ns, err))
		}
	}
	links, err := m.listLinks(ctx)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	for _, l := range links {
		if !m.names.ownsLink(l) {
			continue
		}
		if _, err := m.run(ctx, "", "ip", "link", "del", l); err != nil && !notFound(err) {
			errs = append(errs, fmt.Errorf("netpolicy: delete link %s: %w", l, err))
		}
	}
	return errors.Join(errs...)
}

// allocate reserves the lowest free /30 and a fresh non-zero generation for id.
func (m *Manager) allocate(id string) (sandbox, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sbs[id]; ok {
		return sandbox{}, status.Errorf(codes.AlreadyExists, "netpolicy: sandbox %q already has a network", id)
	}
	for i := 0; i < m.slots; i++ {
		if _, ok := m.used[i]; !ok {
			m.gen++
			if m.gen == 0 {
				m.gen++
			}
			sb := sandbox{slot: i, gen: m.gen}
			m.used[i] = id
			m.sbs[id] = sb
			return sb, nil
		}
	}
	return sandbox{}, status.Errorf(codes.ResourceExhausted, "netpolicy: no free /30 in %s", m.cidr)
}

func (m *Manager) release(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sb, ok := m.sbs[id]; ok {
		delete(m.sbs, id)
		delete(m.used, sb.slot)
	}
}

// lookup returns the generation and guest address of id; gen is 0 when this
// Manager did not set it up.
func (m *Manager) lookup(id string) (gen uint32, guest netip.Addr) {
	m.mu.Lock()
	sb, ok := m.sbs[id]
	m.mu.Unlock()
	if !ok {
		return 0, netip.Addr{}
	}
	_, guest = m.slotAddrs(sb.slot)
	return sb.gen, guest
}

// slotAddrs returns the host and guest addresses of slot i.
func (m *Manager) slotAddrs(i int) (host, guest netip.Addr) {
	b := m.cidr.Addr().As4()
	v := binary.BigEndian.Uint32(b[:]) + uint32(i)*4
	binary.BigEndian.PutUint32(b[:], v)
	base := netip.AddrFrom4(b)
	return base.Next(), base.Next().Next()
}

// allowError maps a ParseAllow error to the error Setup returns.
func allowError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("netpolicy: %w", err)
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && (dnsErr.IsTemporary || dnsErr.IsTimeout) {
		return status.Error(codes.Unavailable, "netpolicy: "+err.Error())
	}
	return status.Error(codes.InvalidArgument, "netpolicy: "+err.Error())
}

// Setup creates the sandbox's namespace, veth pair and nftables chain, and
// returns the namespace path and the host entries to pin. On failure it
// removes what it created.
func (m *Manager) Setup(ctx context.Context, id string, allow []string) (netnsPath string, hosts []bundle.HostEntry, err error) {
	if id == "" {
		return "", nil, status.Error(codes.InvalidArgument, "netpolicy: empty sandbox id")
	}
	m.life.RLock()
	defer m.life.RUnlock()
	m.mu.Lock()
	closed, resolve := m.closed, m.resolve
	m.mu.Unlock()
	if closed {
		return "", nil, status.Error(codes.FailedPrecondition, "netpolicy: manager is closed")
	}
	rules, hosts, err := parseAllow(ctx, allow, resolve)
	if err != nil {
		return "", nil, allowError(err)
	}
	sb, err := m.allocate(id)
	if err != nil {
		return "", nil, err
	}
	ns, veth := m.names.netns(id), m.names.veth(id)

	// Rollback deletes by name, so never adopt a name that already exists.
	if err := m.checkFree(ctx, ns, veth); err != nil {
		m.release(id)
		return "", nil, err
	}

	hostIP, guestIP := m.slotAddrs(sb.slot)
	steps := [][]string{
		{"ip", "netns", "add", ns},
		{"ip", "link", "add", veth, "type", "veth", "peer", "name", guestIf, "netns", ns},
		{"ip", "addr", "add", hostIP.String() + "/30", "dev", veth},
		{"ip", "link", "set", veth, "up"},
		{"ip", "-n", ns, "addr", "add", guestIP.String() + "/30", "dev", guestIf},
		{"ip", "-n", ns, "link", "set", guestIf, "up"},
		{"ip", "-n", ns, "link", "set", "lo", "up"},
		{"ip", "-n", ns, "route", "add", "default", "via", hostIP.String(), "dev", guestIf},
	}
	fail := func(err error) (string, []bundle.HostEntry, error) {
		// Best effort with a context that outlives a cancelled Setup. If the
		// rollback fails the /30 stays reserved, as after a failed Teardown.
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if terr := m.teardown(cctx, id, sb.gen, guestIP); terr != nil {
			return "", nil, errors.Join(err, fmt.Errorf("netpolicy: rollback: %w", terr))
		}
		m.release(id)
		return "", nil, err
	}
	for _, s := range steps {
		if _, err := m.run(ctx, "", s[0], s[1:]...); err != nil {
			return fail(fmt.Errorf("netpolicy: setup %s: %w", id, err))
		}
	}
	if _, err := m.run(ctx, renderSetup(m.names, id, sb.gen, guestIP, rules), "nft", "-f", "-"); err != nil {
		return fail(fmt.Errorf("netpolicy: setup %s: add rules: %w", id, err))
	}
	return netnsDir + ns, hosts, nil
}

// checkFree fails when the namespace or veth name is already in use.
func (m *Manager) checkFree(ctx context.Context, ns, veth string) error {
	nss, err := m.listNetns(ctx)
	if err != nil {
		return err
	}
	if slices.Contains(nss, ns) {
		return status.Errorf(codes.AlreadyExists, "netpolicy: namespace %s already exists", ns)
	}
	links, err := m.listLinks(ctx)
	if err != nil {
		return err
	}
	if slices.Contains(links, veth) {
		return status.Errorf(codes.AlreadyExists, "netpolicy: link %s already exists", veth)
	}
	return nil
}

// Teardown removes the sandbox's live element, map element, chain, veth and
// namespace. Missing pieces are not errors. On error the /30 stays reserved.
func (m *Manager) Teardown(ctx context.Context, id string) error {
	if id == "" {
		return status.Error(codes.InvalidArgument, "netpolicy: empty sandbox id")
	}
	m.life.RLock()
	defer m.life.RUnlock()
	gen, guest := m.lookup(id)
	if err := m.teardown(ctx, id, gen, guest); err != nil {
		return err
	}
	m.release(id)
	return nil
}

func (m *Manager) teardown(ctx context.Context, id string, gen uint32, guest netip.Addr) error {
	var errs []error
	if _, err := m.run(ctx, renderTeardown(m.names, id, gen, guest), "nft", "-f", "-"); err != nil {
		// The script succeeds whenever the table exists; without the table
		// there is nothing to remove.
		if _, lerr := m.run(ctx, "", "nft", "list", "table", "inet", m.names.table); lerr == nil {
			errs = append(errs, fmt.Errorf("netpolicy: teardown %s: remove rules: %w", id, err))
		}
	}
	veth, ns := m.names.veth(id), m.names.netns(id)
	if _, err := m.run(ctx, "", "ip", "link", "del", veth); err != nil && !notFound(err) {
		errs = append(errs, fmt.Errorf("netpolicy: teardown %s: delete link %s: %w", id, veth, err))
	}
	if _, err := m.run(ctx, "", "ip", "netns", "del", ns); err != nil && !notFound(err) {
		errs = append(errs, fmt.Errorf("netpolicy: teardown %s: delete namespace %s: %w", id, ns, err))
	}
	return errors.Join(errs...)
}

// notFound reports whether a command failed because its object does not exist.
func notFound(err error) bool {
	s := err.Error()
	return strings.Contains(s, "Cannot find device") || strings.Contains(s, "No such file or directory")
}

// execRunner runs a real command; the error carries the command line and its stderr.
func execRunner(ctx context.Context, stdin string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.WaitDelay = 2 * time.Second
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if len(msg) > 2048 {
			msg = msg[len(msg)-2048:]
		}
		return stdout.String(), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, msg)
	}
	return stdout.String(), nil
}
