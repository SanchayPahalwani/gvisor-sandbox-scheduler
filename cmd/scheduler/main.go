// Command scheduler runs the scheduler (spec section 8): one gRPC server
// with SandboxService for clients and ClusterService for node agents. It
// places sandboxes, proxies Exec to the nodes, detects dead nodes and
// restarts their sandboxes elsewhere. Its state is in memory and is rebuilt
// from heartbeats after a restart (spec D18).
//
// SIGINT or SIGTERM stops it: failure detection and the rescheduler first,
// then no new RPCs and up to drainTimeout for the ones in flight. A second
// signal ends the process at once.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/cluster"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/gateway"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/sched"
	"google.golang.org/grpc"
)

// drainTimeout is how long a shutdown waits for RPCs in flight (Exec
// streams, creates) before it cancels them.
const drainTimeout = 5 * time.Second

// options are the parsed and checked flags.
type options struct {
	listen                string
	nodeTimeout           time.Duration
	heartbeatInterval     time.Duration
	weights               sched.Weights
	sampleSize            int
	rescheduleConcurrency int
	policy                string // binpack or spread
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
		slog.Error("scheduler failed", "err", err)
		os.Exit(1)
	}
}

// parseFlags parses and checks the command line. It prints any error, with
// the usage for a flag the flag package rejects, to out.
func parseFlags(args []string, out io.Writer) (*options, error) {
	fs := flag.NewFlagSet("scheduler", flag.ContinueOnError)
	fs.SetOutput(out)
	o := &options{}
	d := sched.DefaultWeights
	fs.StringVar(&o.listen, "listen", ":7000", "gRPC listen address")
	fs.DurationVar(&o.nodeTimeout, "node-timeout", 3*time.Second, "a node without a heartbeat for this long is dead (spec D19)")
	fs.DurationVar(&o.heartbeatInterval, "heartbeat-interval", time.Second, "heartbeat interval told to nodes at registration")
	fs.Float64Var(&o.weights.BinPack, "w-binpack", d.BinPack, "weight of the BinPack score (binpack policy)")
	fs.Float64Var(&o.weights.WarmPool, "w-warm", d.WarmPool, "weight of the WarmPool score (binpack policy)")
	fs.Float64Var(&o.weights.GroupAffinity, "w-affinity", d.GroupAffinity, "weight of the GroupAffinity score (binpack policy)")
	fs.Float64Var(&o.weights.NodeHealth, "w-health", d.NodeHealth, "weight of the NodeHealth score (binpack policy)")
	fs.IntVar(&o.sampleSize, "sample-size", 0, "stop scoring after this many feasible nodes (0: score all, spec D22)")
	fs.IntVar(&o.rescheduleConcurrency, "reschedule-concurrency", gateway.DefaultConfig().RescheduleConcurrency, "most reschedules in flight at once")
	fs.StringVar(&o.policy, "policy", "binpack", "placement policy: binpack (the default policy, spec D20) or spread (least allocated; the group-loss test, spec D35)")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	fail := func(format string, a ...any) (*options, error) {
		err := fmt.Errorf(format, a...)
		fmt.Fprintf(out, "scheduler: %v\n", err)
		return nil, err
	}
	if fs.NArg() > 0 {
		return fail("unexpected arguments %q", fs.Args())
	}
	if _, _, err := net.SplitHostPort(o.listen); err != nil {
		return fail("--listen %q: %v", o.listen, err)
	}
	if o.nodeTimeout <= 0 || o.heartbeatInterval <= 0 {
		return fail("--node-timeout and --heartbeat-interval must be positive, got %v and %v", o.nodeTimeout, o.heartbeatInterval)
	}
	if o.heartbeatInterval >= o.nodeTimeout {
		return fail("--heartbeat-interval %v must be shorter than --node-timeout %v, or every node is declared dead between heartbeats", o.heartbeatInterval, o.nodeTimeout)
	}
	for _, w := range []struct {
		flag string
		v    float64
	}{{"--w-binpack", o.weights.BinPack}, {"--w-warm", o.weights.WarmPool}, {"--w-affinity", o.weights.GroupAffinity}, {"--w-health", o.weights.NodeHealth}} {
		if w.v < 0 {
			return fail("%s must not be negative, got %v", w.flag, w.v)
		}
	}
	if o.sampleSize < 0 {
		return fail("--sample-size must not be negative, got %d", o.sampleSize)
	}
	if o.rescheduleConcurrency <= 0 {
		return fail("--reschedule-concurrency must be positive, got %d", o.rescheduleConcurrency)
	}
	if o.policy != "binpack" && o.policy != "spread" {
		return fail("--policy %q: want binpack or spread", o.policy)
	}
	return o, nil
}

// newPolicy returns the placement policy the options name.
func newPolicy(o *options) *sched.Policy {
	var p *sched.Policy
	if o.policy == "spread" {
		p = sched.SpreadPolicy()
	} else {
		p = sched.DefaultPolicy(o.weights)
	}
	p.SampleSize = o.sampleSize
	return p
}

// run serves until ctx ends (a signal) or the server fails. stop restores
// the default signal handling, so that a second signal ends the process.
func run(ctx context.Context, stop context.CancelFunc, o *options) error {
	lis, err := net.Listen("tcp", o.listen)
	if err != nil {
		stop()
		return err
	}
	ccfg := cluster.DefaultConfig()
	ccfg.NodeTimeout = o.nodeTimeout
	state := cluster.New(ccfg, time.Now)
	gcfg := gateway.DefaultConfig()
	gcfg.HeartbeatInterval = o.heartbeatInterval
	gcfg.RescheduleConcurrency = o.rescheduleConcurrency
	nodes := gateway.NewGRPCNodes()
	if c, ok := nodes.(io.Closer); ok {
		defer c.Close()
	}
	gw := gateway.New(state, newPolicy(o), nodes, gcfg)

	srv := grpc.NewServer(gateway.ServerOptions()...)
	gssv1.RegisterSandboxServiceServer(srv, gw)
	gssv1.RegisterClusterServiceServer(srv, gw)

	runCtx, cancelRun := context.WithCancel(context.Background())
	ran := make(chan struct{})
	go func() {
		gw.Run(runCtx)
		close(ran)
	}()
	served := make(chan error, 1)
	go func() { served <- srv.Serve(lis) }()
	slog.Info("scheduler serving", "listen", lis.Addr().String(), "policy", o.policy,
		"node_timeout", o.nodeTimeout, "heartbeat_interval", o.heartbeatInterval,
		"w_binpack", o.weights.BinPack, "w_warm", o.weights.WarmPool, "w_affinity", o.weights.GroupAffinity,
		"w_health", o.weights.NodeHealth, "sample_size", o.sampleSize, "reschedule_concurrency", o.rescheduleConcurrency)

	var serveErr error
	select {
	case <-ctx.Done():
		slog.Info("scheduler shutting down")
	case serveErr = <-served:
		serveErr = fmt.Errorf("serve: %w", serveErr)
	}
	stop()
	// Failure detection and rescheduling stop first: while the server
	// drains it takes no new RPCs, heartbeats included, so a detector still
	// running would declare healthy nodes dead and move their sandboxes.
	cancelRun()
	<-ran
	drained := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(drainTimeout):
		srv.Stop()
		<-drained
	}
	if serveErr != nil {
		return serveErr
	}
	slog.Info("scheduler stopped")
	return nil
}
