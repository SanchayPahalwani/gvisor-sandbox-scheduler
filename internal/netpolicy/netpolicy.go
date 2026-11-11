package netpolicy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os/exec"
	"regexp"
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

var nodeIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$`)

// Manager owns this node's nftables table, sandbox namespaces and veths.
type Manager struct {
	names   names
	cidr    netip.Prefix
	slots   int // number of /30s in cidr
	run     Runner
	mu      sync.Mutex
	resolve func(ctx context.Context, host string) ([]netip.Addr, error)
	used    map[int]string // slot -> sandbox id
	byID    map[string]int // sandbox id -> slot
}

// New returns a Manager for nodeID handing out /30s from cidr (e.g. 10.200.0.0/20).
// A nil run executes real commands.
func New(nodeID string, cidr netip.Prefix, run Runner) (*Manager, error) {
	if !nodeIDRE.MatchString(nodeID) || nodeID == "." || nodeID == ".." {
		return nil, fmt.Errorf("netpolicy: invalid node id %q", nodeID)
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
	return &Manager{
		names:   newNames(nodeID),
		cidr:    cidr.Masked(),
		slots:   1 << bits,
		run:     run,
		resolve: defaultResolve,
		used:    map[int]string{},
		byID:    map[string]int{},
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

// Init removes this node's leftovers, enables net.ipv4.ip_forward and creates the table.
func (m *Manager) Init(ctx context.Context) error {
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

// Close deletes the table and every namespace of this node.
func (m *Manager) Close(ctx context.Context) error {
	var errs []error
	if _, err := m.run(ctx, renderDeleteTable(m.names), "nft", "-f", "-"); err != nil {
		errs = append(errs, fmt.Errorf("netpolicy: delete table %s: %w", m.names.table, err))
	}
	if err := m.removeLeftovers(ctx); err != nil {
		errs = append(errs, err)
	}
	m.resetSlots()
	return errors.Join(errs...)
}

func (m *Manager) resetSlots() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.used = map[int]string{}
	m.byID = map[string]int{}
}

// removeLeftovers deletes every namespace and host veth that carries this node's names.
func (m *Manager) removeLeftovers(ctx context.Context) error {
	var errs []error
	out, err := m.run(ctx, "", "ip", "netns", "list")
	if err != nil {
		return fmt.Errorf("netpolicy: list namespaces: %w", err)
	}
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) == 0 || !m.names.ownsNetns(f[0]) {
			continue
		}
		if _, err := m.run(ctx, "", "ip", "netns", "del", f[0]); err != nil && !notFound(err) {
			errs = append(errs, fmt.Errorf("netpolicy: delete namespace %s: %w", f[0], err))
		}
	}
	out, err = m.run(ctx, "", "ip", "-o", "link", "show")
	if err != nil {
		return errors.Join(append(errs, fmt.Errorf("netpolicy: list links: %w", err))...)
	}
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		name, _, _ := strings.Cut(strings.TrimSuffix(f[1], ":"), "@")
		if !m.names.ownsLink(name) {
			continue
		}
		if _, err := m.run(ctx, "", "ip", "link", "del", name); err != nil && !notFound(err) {
			errs = append(errs, fmt.Errorf("netpolicy: delete link %s: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

// allocate reserves the lowest free /30 for id.
func (m *Manager) allocate(id string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.byID[id]; ok {
		return 0, status.Errorf(codes.AlreadyExists, "netpolicy: sandbox %q already has a network", id)
	}
	for i := 0; i < m.slots; i++ {
		if _, ok := m.used[i]; !ok {
			m.used[i] = id
			m.byID[id] = i
			return i, nil
		}
	}
	return 0, status.Errorf(codes.ResourceExhausted, "netpolicy: no free /30 in %s", m.cidr)
}

func (m *Manager) release(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if i, ok := m.byID[id]; ok {
		delete(m.byID, id)
		delete(m.used, i)
	}
}

// slotAddrs returns the host and guest addresses of slot i.
func (m *Manager) slotAddrs(i int) (host, guest netip.Addr) {
	b := m.cidr.Addr().As4()
	v := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	v += uint32(i) * 4
	base := netip.AddrFrom4([4]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
	return base.Next(), base.Next().Next()
}

// Setup creates the sandbox's namespace, veth pair and nftables chain, and
// returns the namespace path and the host entries to pin. On failure it
// removes what it created.
func (m *Manager) Setup(ctx context.Context, id string, allow []string) (netnsPath string, hosts []bundle.HostEntry, err error) {
	if id == "" {
		return "", nil, status.Error(codes.InvalidArgument, "netpolicy: empty sandbox id")
	}
	m.mu.Lock()
	resolve := m.resolve
	m.mu.Unlock()
	rules, hosts, err := parseAllow(ctx, allow, resolve)
	if err != nil {
		return "", nil, status.Error(codes.InvalidArgument, "netpolicy: "+err.Error())
	}
	slot, err := m.allocate(id)
	if err != nil {
		return "", nil, err
	}
	hostIP, guestIP := m.slotAddrs(slot)
	ns, veth := m.names.netns(id), m.names.veth(id)
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
		// Best effort with a context that outlives a cancelled Setup.
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_ = m.teardown(cctx, id)
		m.release(id)
		return "", nil, err
	}
	for _, s := range steps {
		if _, err := m.run(ctx, "", s[0], s[1:]...); err != nil {
			return fail(fmt.Errorf("netpolicy: setup %s: %w", id, err))
		}
	}
	if _, err := m.run(ctx, renderSetup(m.names, id, rules), "nft", "-f", "-"); err != nil {
		return fail(fmt.Errorf("netpolicy: setup %s: add rules: %w", id, err))
	}
	return netnsDir + ns, hosts, nil
}

// Teardown removes the sandbox's map element, chain, veth and namespace.
// Missing pieces are not errors.
func (m *Manager) Teardown(ctx context.Context, id string) error {
	if id == "" {
		return status.Error(codes.InvalidArgument, "netpolicy: empty sandbox id")
	}
	if err := m.teardown(ctx, id); err != nil {
		return err
	}
	m.release(id)
	return nil
}

func (m *Manager) teardown(ctx context.Context, id string) error {
	var errs []error
	if _, err := m.run(ctx, renderTeardown(m.names, id), "nft", "-f", "-"); err != nil {
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
