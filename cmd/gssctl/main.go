// Command gssctl is the command-line client of the scheduler's SandboxService
// (spec section 11). It creates sandboxes, runs code in them, and lists the
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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
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
  --ttl 10m          destroy the sandbox after this long (at most 60m)

-c CODE runs Python code in a fork of the primed interpreter, -f FILE runs
the code in FILE the same way, and -- ARGV... runs ARGV as a new process.
Run "gssctl <command> -h" for a command's flags.
`

const (
	defaultScheduler = "127.0.0.1:7000"
	// maxMsgSize bounds a gRPC message each way: code sent with -f, and the
	// output chunks of an Exec.
	maxMsgSize = 20 << 20
	// defaultExecTimeout is the program's time limit, as the scheduler's default.
	defaultExecTimeout = 30 * time.Second
)

// Deadlines of the calls. They keep a hung call from hanging gssctl; none is near
// what a healthy call takes. An Exec's deadline is its --timeout plus execSlack,
// which covers the scheduler's wait for the sandbox to be ready.
const (
	createTimeout  = time.Minute
	execSlack      = 30 * time.Second
	destroyTimeout = 30 * time.Second
	queryTimeout   = 10 * time.Second
)

// Exit codes of gssctl's own failures; run and exec otherwise exit with the program's.
const (
	exitFail  = 1
	exitUsage = 2
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	c := &cli{stdout: os.Stdout, stderr: os.Stderr, getenv: os.Getenv, now: time.Now, dial: dial}
	code := c.main(ctx, os.Args[1:])
	stop()
	os.Exit(code)
}

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMsgSize), grpc.MaxCallRecvMsgSize(maxMsgSize)))
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
