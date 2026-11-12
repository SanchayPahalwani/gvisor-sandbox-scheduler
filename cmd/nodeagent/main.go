// Command nodeagent runs one node: it serves NodeService over gRPC and runs
// each sandbox as a runsc container (spec section 6). It needs Linux and
// root; on any other system, or without root, runsc or the sandbox rootfs,
// it exits at once with a message saying why.
//
// SIGINT or SIGTERM stops it: no new RPCs, every sandbox destroyed, the
// node's network table removed, the runsc root removed. A second signal
// during that ends the process at once.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"syscall"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/netpolicy"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/nodeagent"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
	"google.golang.org/grpc"
)

// The network policy implements the manager's NetPolicy.
var _ nodeagent.NetPolicy = (*netpolicy.Manager)(nil)

// nodeIDRE is what netpolicy accepts as a node ID (it becomes part of the
// nftables table, namespace and veth names); the manager accepts more.
var nodeIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,31}$`)

const (
	cgroupRoot = "/sys/fs/cgroup"
	// startTimeout bounds the start-up: runsc --version, the network
	// policy's and the manager's removal of an earlier agent's leftovers.
	startTimeout = 2 * time.Minute
	// closeTimeout bounds the shutdown's waits: for Creates in flight, and
	// for the network and runsc cleanup after the sandboxes are gone.
	closeTimeout = time.Minute
	// drainTimeout is how long the server waits, once the sandboxes are
	// destroyed, for RPCs still in flight (an Exec stuck sending to a
	// client that stopped reading) before it cancels them.
	drainTimeout = 5 * time.Second
)

// options are the parsed and checked flags.
type options struct {
	nodeID, group     string
	listen, advertise string // advertise "" = 127.0.0.1:<the port listened on>
	scheduler         string
	cpuMillis         int64
	memoryBytes       int64
	maxSandboxes      int
	warmPoolSize      int
	rootfs, stateDir  string
	runscRoot         string
	checkpointDir     string // "" disables checkpoint/restore
	egressCIDR        netip.Prefix
	runsc             string
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	o, err := parseFlags(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	}
	if err != nil {
		os.Exit(2) // parseFlags printed the error
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, stop, o); err != nil {
		slog.Error("node agent failed", "node", o.nodeID, "err", err)
		os.Exit(1)
	}
}

// parseFlags parses and checks the command line and fills in the defaults
// that depend on the node ID. It prints any error, with the usage for a
// flag the flag package rejects, to out.
func parseFlags(args []string, out io.Writer) (*options, error) {
	fs := flag.NewFlagSet("nodeagent", flag.ContinueOnError)
	fs.SetOutput(out)
	o := &options{}
	var memoryMiB int64
	var egress string
	fs.StringVar(&o.nodeID, "node-id", "", "node ID (required): 1 to 32 characters of [A-Za-z0-9-], starting with a letter or digit")
	fs.StringVar(&o.group, "group", "default", "node group label")
	fs.StringVar(&o.listen, "listen", ":7101", "gRPC listen address")
	fs.StringVar(&o.advertise, "advertise", "", "address the scheduler reaches this node at (default 127.0.0.1:<listen port>)")
	fs.StringVar(&o.scheduler, "scheduler", "", "scheduler address to register with (default: none, the node runs standalone)")
	fs.Int64Var(&o.cpuMillis, "cpu-millis", 4000, "declared CPU capacity in millicores")
	fs.Int64Var(&memoryMiB, "memory-mib", 2048, "declared memory capacity in MiB")
	fs.IntVar(&o.maxSandboxes, "max-sandboxes", 16, "most sandboxes the node holds at once")
	fs.IntVar(&o.warmPoolSize, "warm-pool-size", 2, "warm sandboxes kept ready (0 disables the pool)")
	fs.StringVar(&o.rootfs, "rootfs", "/var/lib/gss/rootfs/python", "sandbox root filesystem, built by `make rootfs`")
	fs.StringVar(&o.stateDir, "state-dir", "", "state directory (default /var/lib/gss/nodes/<node-id>)")
	fs.StringVar(&o.runscRoot, "runsc-root", "", "runsc --root directory (default /run/gss/<node-id>)")
	fs.StringVar(&o.checkpointDir, "checkpoint-dir", "/var/lib/gss/checkpoints", "checkpoint image directory (empty disables restore)")
	fs.StringVar(&egress, "egress-cidr", "10.200.0.0/20", "IPv4 range the node hands out /30s from for egress sandboxes")
	fs.StringVar(&o.runsc, "runsc", "runsc", "runsc binary (a name looked up on PATH, or a path)")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	fail := func(format string, a ...any) (*options, error) {
		err := fmt.Errorf(format, a...)
		fmt.Fprintf(out, "nodeagent: %v\n", err)
		return nil, err
	}
	if fs.NArg() > 0 {
		return fail("unexpected arguments %q", fs.Args())
	}
	if o.nodeID == "" {
		return fail("--node-id is required")
	}
	if !nodeIDRE.MatchString(o.nodeID) {
		return fail("--node-id %q must be 1 to 32 characters of [A-Za-z0-9-], starting with a letter or digit", o.nodeID)
	}
	if o.group == "" {
		return fail("--group must not be empty")
	}
	if _, _, err := net.SplitHostPort(o.listen); err != nil {
		return fail("--listen %q: %v", o.listen, err)
	}
	if o.advertise != "" {
		if _, _, err := net.SplitHostPort(o.advertise); err != nil {
			return fail("--advertise %q: %v", o.advertise, err)
		}
	}
	if o.scheduler != "" {
		return fail("--scheduler: registering with a scheduler is not implemented yet; leave it empty to run standalone")
	}
	if o.cpuMillis <= 0 || memoryMiB <= 0 || o.maxSandboxes <= 0 {
		return fail("--cpu-millis, --memory-mib and --max-sandboxes must be positive, got %d, %d and %d", o.cpuMillis, memoryMiB, o.maxSandboxes)
	}
	if memoryMiB > math.MaxInt64>>20 {
		return fail("--memory-mib %d is too large", memoryMiB)
	}
	o.memoryBytes = memoryMiB << 20
	if o.warmPoolSize < 0 {
		return fail("--warm-pool-size must not be negative, got %d", o.warmPoolSize)
	}
	if o.stateDir == "" {
		o.stateDir = "/var/lib/gss/nodes/" + o.nodeID
	}
	if o.runscRoot == "" {
		o.runscRoot = "/run/gss/" + o.nodeID
	}
	for _, p := range []struct{ flag, path string }{
		{"--rootfs", o.rootfs}, {"--state-dir", o.stateDir}, {"--runsc-root", o.runscRoot}, {"--checkpoint-dir", o.checkpointDir},
	} {
		if p.flag == "--checkpoint-dir" && p.path == "" {
			continue
		}
		if !filepath.IsAbs(p.path) || filepath.Clean(p.path) == "/" {
			return fail("%s %q must be an absolute path other than /", p.flag, p.path)
		}
	}
	cidr, err := netip.ParsePrefix(egress)
	if err != nil {
		return fail("--egress-cidr: %v", err)
	}
	o.egressCIDR = cidr
	return o, nil
}

// run starts the node and serves until ctx ends (a signal) or the server
// fails, then shuts the node down. stop restores the default signal
// handling, so that a second signal ends the process.
func run(ctx context.Context, stop context.CancelFunc, o *options) error {
	if runtime.GOOS != "linux" {
		return fmt.Errorf("the node agent runs only on Linux (gVisor needs a Linux kernel), not on %s", runtime.GOOS)
	}
	if os.Geteuid() != 0 {
		return errors.New("the node agent must run as root: runsc needs it for cgroups and namespaces")
	}
	runscPath, err := exec.LookPath(o.runsc)
	if err != nil {
		return fmt.Errorf("runsc not found (--runsc %q): %w", o.runsc, err)
	}
	if err := checkRootfs(o.rootfs); err != nil {
		return err
	}

	sctx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()
	rt := runsc.NewCLI(runscPath, o.runscRoot)
	version, err := rt.Version(sctx)
	if err != nil {
		return fmt.Errorf("runsc %s does not run: %w", runscPath, err)
	}
	lis, err := net.Listen("tcp", o.listen)
	if err != nil {
		return err
	}
	defer lis.Close() // the server closes it; this covers a failed start
	advertise := o.advertise
	if advertise == "" {
		// The port actually listened on, also when --listen asked for port 0.
		_, port, err := net.SplitHostPort(lis.Addr().String())
		if err != nil {
			return err
		}
		advertise = net.JoinHostPort("127.0.0.1", port)
	}

	// The network policy removes an earlier agent's namespaces and table
	// before the manager starts sandboxes; it refuses to start when another
	// node on this host owns the table or an overlapping range.
	np, err := netpolicy.New(o.nodeID, o.egressCIDR, nil)
	if err != nil {
		return err
	}
	if err := np.Init(sctx); err != nil {
		return err
	}
	m, err := nodeagent.NewManager(nodeagent.Config{
		NodeID: o.nodeID, Group: o.group,
		CPUMillis: o.cpuMillis, MemoryBytes: o.memoryBytes, MaxSandboxes: o.maxSandboxes,
		RootFS: o.rootfs, StateDir: o.stateDir, CheckpointDir: o.checkpointDir,
		WarmPoolSize: o.warmPoolSize,
	}, nodeagent.Deps{Runtime: rt, Cgroups: nodeagent.NewCgroupFS(cgroupRoot), Net: np})
	if err != nil {
		return errors.Join(err, closeNet(np))
	}
	if err := m.Start(sctx); err != nil {
		// Leftover containers may still be under the runsc root, so the
		// root stays.
		cctx, ccancel := context.WithTimeout(context.Background(), closeTimeout)
		defer ccancel()
		return errors.Join(err, m.Close(cctx), closeNet(np))
	}

	srv := grpc.NewServer(nodeagent.ServerOptions()...)
	gssv1.RegisterNodeServiceServer(srv, nodeagent.NewServer(m))
	served := make(chan error, 1)
	go func() { served <- srv.Serve(lis) }()
	slog.Info("node agent serving", "node", o.nodeID, "group", o.group,
		"listen", lis.Addr().String(), "advertise", advertise, "runsc", version,
		"cpu_millis", o.cpuMillis, "memory_bytes", o.memoryBytes, "max_sandboxes", o.maxSandboxes,
		"warm_pool_size", o.warmPoolSize, "checkpoint_dir", o.checkpointDir, "egress_cidr", o.egressCIDR.String())

	var serveErr error
	select {
	case <-ctx.Done():
		stop()
		slog.Info("node agent shutting down", "node", o.nodeID)
	case serveErr = <-served:
		serveErr = fmt.Errorf("serve: %w", serveErr)
	}
	err = errors.Join(serveErr, shutdown(srv, m, np, rt))
	if err == nil {
		slog.Info("node agent stopped", "node", o.nodeID)
	}
	return err
}

// checkRootfs fails unless dir holds the zygote and pyfork that every
// sandbox starts with.
func checkRootfs(dir string) error {
	for _, f := range []string{"gss/zygote.py", "gss/pyfork"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			return fmt.Errorf("the sandbox rootfs at %s is missing or incomplete (build it with `make rootfs`): %w", dir, err)
		}
	}
	return nil
}

// shutdown stops the node in order: no new RPCs, the sandboxes destroyed,
// the network policy closed, the runsc root removed.
//
// GracefulStop refuses new RPCs at once and then waits for the ones in
// flight. Those end while Manager.Close runs: it waits for Creates in flight
// and ends every Exec of a sandbox it destroys. An RPC still open
// drainTimeout after that is cancelled.
func shutdown(srv *grpc.Server, m *nodeagent.Manager, np *netpolicy.Manager, rt *runsc.CLI) error {
	drained := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(drained)
	}()
	var errs []error
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	if err := m.Close(ctx); err != nil {
		errs = append(errs, fmt.Errorf("destroy sandboxes: %w", err))
	}
	cancel()
	select {
	case <-drained:
	case <-time.After(drainTimeout):
		srv.Stop()
		<-drained
	}
	if err := closeNet(np); err != nil {
		errs = append(errs, err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()
	// RemoveRoot only when no container is left: a sandbox whose teardown
	// failed may still be running from it.
	ids, err := rt.List(ctx)
	switch {
	case err != nil:
		errs = append(errs, fmt.Errorf("runsc root %s left in place: %w", rt.Root, err))
	case len(ids) > 0:
		errs = append(errs, fmt.Errorf("runsc root %s left in place: containers %q are still there", rt.Root, ids))
	default:
		if err := rt.RemoveRoot(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// closeNet closes the network policy, bounded by closeTimeout.
func closeNet(np *netpolicy.Manager) error {
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()
	return np.Close(ctx)
}
