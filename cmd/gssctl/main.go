// Command gssctl is the command-line client of the scheduler's SandboxService.
// It creates sandboxes, runs code in them, and lists the
// sandboxes and nodes the scheduler knows, and shows how a sandbox was placed.
//
// run and exec exit with the program's exit code. gssctl's own failures exit
// 1, among them an Exec that ended without the program's exit being seen
// (spec D34: the stream ended with a gRPC status, which is printed); a bad
// command line exits 2.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/telemetry"
)

const usage = `usage: gssctl [--scheduler ADDR] <command> [flags]

The scheduler address is --scheduler, else $GSS_SCHEDULER, else 127.0.0.1:7000.

commands:
  run [spec flags] [--timeout 30s] [--keep] (-c CODE | -f FILE | -- ARGV...)
                     create a sandbox, run the program in it with its output
                     copied through, destroy the sandbox (unless --keep), and
                     exit with the program's exit code
  create [spec flags]
                     create a sandbox and print its ID
  exec ID [--timeout 30s] (-c CODE | -f FILE | -- ARGV...)
                     run a program in an existing sandbox
  destroy ID         destroy a sandbox
  ps                 list the sandboxes
  nodes              list the nodes
  explain ID         show how a sandbox was placed: the nodes considered and
                     feasible, and the top candidates with per-plugin scores

spec flags:
  --cpu 500m         CPU: millicores (500m) or cores (0.5, 2)
  --mem 256Mi        memory: Ki, Mi, Gi, Ti or bytes; at least 192Mi
  --allow HOST:PORT  allow egress to host:port, ip:port or cidr:port (repeatable)
  --group G          run on a node of group G
  --affinity KEY     prefer the group where sandboxes with KEY run
  --mode auto        start path: auto, cold, restore or warm
  --ttl 10m          destroy the sandbox after this long (10s to 60m)

-c CODE runs Python code in a fork of the primed interpreter, -f FILE runs
the code in FILE the same way, and -- ARGV... runs ARGV as a new process.
Run "gssctl <command> -h" for a command's flags.

With OTEL_EXPORTER_OTLP_ENDPOINT set, gssctl exports a trace of each call.
`

const (
	defaultScheduler = "127.0.0.1:7000"
	// maxMsgSize bounds a gRPC message each way: code sent with -f, and the
	// output chunks of an Exec.
	maxMsgSize = 20 << 20
	// The program's time limit: the scheduler's default and most.
	defaultExecTimeout = 30 * time.Second
	maxExecTimeout     = 10 * time.Minute
	// maxCodeBytes is the most Python code -c or -f sends; the message limit has room
	// for the rest of the request.
	maxCodeBytes = 16 << 20
)

// Deadlines of the calls. They keep a hung call from hanging gssctl; none is near
// what a healthy call takes. An Exec's deadline is its --timeout plus execSlack,
// which covers the scheduler's wait for the sandbox to be ready.
const (
	createTimeout  = time.Minute
	execSlack      = 30 * time.Second
	destroyTimeout = 30 * time.Second
	queryTimeout   = 10 * time.Second
	// flushTimeout bounds the export, at exit, of what telemetry still holds.
	flushTimeout = 2 * time.Second
)

// Exit codes of gssctl's own failures; run and exec otherwise exit with the program's.
const (
	exitFail        = 1
	exitUsage       = 2
	exitInterrupted = 130 // a signal ended run or exec before the program's exit was seen
)

// signals are caught while gssctl runs, so that none of them kills it between
// creating a sandbox and destroying it. SIGINT, SIGTERM and SIGHUP (the terminal
// went away) end its context. SIGPIPE is caught only so that a write to a closed
// stdout or stderr fails with EPIPE instead of killing the process; that write
// error ends the run. It does not end the context: once SIGPIPE is caught, a write
// to a broken gRPC connection raises it too, and that failure is the RPC's status
// to report (spec D34), not an interruption.
var signals = []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGPIPE}

func main() { os.Exit(gssctl(os.Args[1:])) }

// gssctl runs the command line args with the process's own outputs, environment
// and signals, and returns the exit code.
func gssctl(args []string) int {
	ctx, stop := notifyContext(context.Background())
	defer stop()
	// Telemetry exports only while OTEL_EXPORTER_OTLP_ENDPOINT is set;
	// unset, Setup installs nothing and the flush is a no-op.
	shutdown, err := telemetry.Setup(ctx, "gssctl")
	if err != nil {
		fmt.Fprintf(os.Stderr, "gssctl: %v\n", err)
		return exitFail
	}
	defer func() {
		fctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
		defer cancel()
		if err := shutdown(fctx); err != nil {
			fmt.Fprintf(os.Stderr, "gssctl: telemetry flush failed: %v\n", err)
		}
	}()
	c := &cli{stdout: os.Stdout, stderr: os.Stderr, getenv: os.Getenv, now: time.Now, dial: dial}
	return c.main(ctx, args)
}

// notifyContext returns a context that a signal in signals other than SIGPIPE
// ends, and the function that stops catching them.
func notifyContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	ch := make(chan os.Signal, len(signals))
	signal.Notify(ch, signals...)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case s := <-ch:
				if s != syscall.SIGPIPE {
					cancel(fmt.Errorf("signal: %v", s))
				}
			case <-done:
				return
			}
		}
	}()
	return ctx, func() {
		signal.Stop(ch)
		close(done)
		cancel(nil)
	}
}

// dial connects to the scheduler with the otelgrpc client handler: with telemetry
// on, each call is the root span of its trace.
func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMsgSize), grpc.MaxCallRecvMsgSize(maxMsgSize)),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
}

// cli is one gssctl invocation, with its outputs, environment, clock and dialer.
type cli struct {
	stdout, stderr io.Writer
	getenv         func(string) string
	now            func() time.Time
	dial           func(addr string) (*grpc.ClientConn, error)

	scheduler string // set from the global flags
}

// usageError is a bad command line. printed is true when the flag package has
// already printed it.
type usageError struct {
	err     error
	printed bool
}

func (e *usageError) Error() string { return e.err.Error() }

func usagef(format string, a ...any) error {
	return &usageError{err: fmt.Errorf(format, a...)}
}

// main runs the command line args (without the program name) and returns the exit code.
func (c *cli) main(ctx context.Context, args []string) int {
	code, err := c.dispatch(ctx, args)
	var ue *usageError
	switch {
	case err == nil:
		return code
	case errors.Is(err, flag.ErrHelp):
		return 0
	case errors.As(err, &ue):
		if !ue.printed {
			fmt.Fprintf(c.stderr, "gssctl: %v\n", ue.err)
		}
		return exitUsage
	default:
		fmt.Fprintf(c.stderr, "gssctl: %v\n", err)
		return exitFail
	}
}

func (c *cli) dispatch(ctx context.Context, args []string) (int, error) {
	fs := flag.NewFlagSet("gssctl", flag.ContinueOnError)
	fs.SetOutput(c.stderr)
	fs.Usage = func() {} // printed below: to stdout for -h, to stderr after an error
	def := defaultScheduler
	if env := c.getenv("GSS_SCHEDULER"); env != "" {
		def = env
	}
	fs.StringVar(&c.scheduler, "scheduler", def, "address of the scheduler's SandboxService")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(c.stdout, usage)
			return 0, nil
		}
		fmt.Fprint(c.stderr, usage)
		return 0, &usageError{err: err, printed: true}
	}
	if fs.NArg() == 0 {
		fmt.Fprint(c.stderr, usage)
		return 0, &usageError{err: errors.New("no command"), printed: true}
	}
	cmd, rest := fs.Arg(0), fs.Args()[1:]
	switch cmd {
	case "run":
		return c.run(ctx, rest)
	case "create":
		return 0, c.create(ctx, rest)
	case "exec":
		return c.exec(ctx, rest)
	case "destroy":
		return 0, c.destroy(ctx, rest)
	case "ps":
		return 0, c.ps(ctx, rest)
	case "nodes":
		return 0, c.nodes(ctx, rest)
	case "explain":
		return 0, c.explain(ctx, rest)
	case "help":
		fmt.Fprint(c.stdout, usage)
		return 0, nil
	}
	return 0, usagef("unknown command %q (run gssctl -h for the commands)", cmd)
}

// connect dials the scheduler.
func (c *cli) connect() (*grpc.ClientConn, gssv1.SandboxServiceClient, error) {
	conn, err := c.dial(c.scheduler)
	if err != nil {
		return nil, nil, fmt.Errorf("scheduler %s: %w", c.scheduler, err)
	}
	return conn, gssv1.NewSandboxServiceClient(conn), nil
}

// describe renders an RPC error as "Code: message".
func describe(err error) string {
	if s, ok := status.FromError(err); ok {
		return fmt.Sprintf("%s: %s", s.Code(), s.Message())
	}
	return err.Error()
}
