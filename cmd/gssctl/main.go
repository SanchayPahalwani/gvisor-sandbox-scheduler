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
	"math"
	"os"
	"os/signal"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

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

// flagSet returns the flag set of one command.
func (c *cli) flagSet(cmd string) *flag.FlagSet {
	fs := flag.NewFlagSet("gssctl "+cmd, flag.ContinueOnError)
	fs.SetOutput(c.stderr)
	return fs
}

// parse parses args into fs. Errors the flag package printed are marked so.
func parse(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return &usageError{err: err, printed: true}
	}
	return nil
}

// noArgs fails when the command line has arguments left after the flags.
func noArgs(fs *flag.FlagSet) error {
	if fs.NArg() > 0 {
		return usagef("%s: unexpected argument %q", fs.Name(), fs.Arg(0))
	}
	return nil
}

// sandboxID takes the sandbox ID that must come first in args.
func sandboxID(cmd string, args []string) (string, []string, error) {
	if len(args) == 0 || args[0] == "" || strings.HasPrefix(args[0], "-") {
		return "", nil, usagef("gssctl %s: the sandbox ID comes first: gssctl %s ID", cmd, cmd)
	}
	return args[0], args[1:], nil
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

// stringList is a repeatable string flag.
type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

// specFlags are the flags that make a SandboxSpec.
type specFlags struct {
	cpu, mem              string
	allow                 stringList
	group, affinity, mode string
	ttl                   time.Duration
}

func addSpecFlags(fs *flag.FlagSet) *specFlags {
	f := &specFlags{}
	fs.StringVar(&f.cpu, "cpu", "500m", "CPU: millicores (500m) or cores (0.5, 2)")
	fs.StringVar(&f.mem, "mem", "256Mi", "memory: a number with Ki, Mi, Gi or Ti, or bytes; at least 192Mi")
	fs.Var(&f.allow, "allow", "allow egress to host:port, ip:port or cidr:port (repeatable; none: no network)")
	fs.StringVar(&f.group, "group", "", "run on a node of this group (default: any)")
	fs.StringVar(&f.affinity, "affinity", "", "prefer the node group where sandboxes with this key run")
	fs.StringVar(&f.mode, "mode", "auto", "start path: auto, cold, restore or warm")
	fs.DurationVar(&f.ttl, "ttl", gssv1.DefaultTTL, "destroy the sandbox after this long")
	return f
}

func (f *specFlags) spec() (*gssv1.SandboxSpec, error) {
	cpu, err := parseCPU(f.cpu)
	if err != nil {
		return nil, usagef("--cpu: %v", err)
	}
	mem, err := parseMem(f.mem)
	if err != nil {
		return nil, usagef("--mem: %v", err)
	}
	mode, err := parseMode(f.mode)
	if err != nil {
		return nil, usagef("--mode: %v", err)
	}
	s := &gssv1.SandboxSpec{
		Resources:   &gssv1.Resources{CpuMillis: cpu, MemoryBytes: mem},
		EgressAllow: f.allow, NodeGroup: f.group, AffinityKey: f.affinity,
		Ttl: durationpb.New(f.ttl), StartMode: mode,
	}
	if err := gssv1.ValidateSpec(s); err != nil {
		return nil, usagef("%v", err)
	}
	return s, nil
}

var (
	millisRE = regexp.MustCompile(`^([0-9]+)m$`)
	coresRE  = regexp.MustCompile(`^([0-9]+)(?:\.([0-9]+))?$`)
	memRE    = regexp.MustCompile(`^([0-9]+)(Ki|Mi|Gi|Ti)?$`)
)

// parseCPU reads millicores ("500m") or cores ("0.5", "2") as millicores.
func parseCPU(s string) (int64, error) {
	var n int64
	if m := millisRE.FindStringSubmatch(s); m != nil {
		v, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%q is out of range", s)
		}
		n = v
	} else if m := coresRE.FindStringSubmatch(s); m != nil {
		whole, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil || whole > math.MaxInt64/1000-1 {
			return 0, fmt.Errorf("%q is out of range", s)
		}
		frac := m[2]
		if len(frac) > 3 {
			return 0, fmt.Errorf("%q is finer than a millicore", s)
		}
		f, _ := strconv.ParseInt(frac+strings.Repeat("0", 3-len(frac)), 10, 64) // three digits
		n = whole*1000 + f
	} else {
		return 0, fmt.Errorf("%q is not millicores (500m) or cores (0.5, 2)", s)
	}
	if n <= 0 {
		return 0, fmt.Errorf("%q must be more than zero", s)
	}
	return n, nil
}

// parseMem reads a byte count with an optional binary suffix: Ki, Mi, Gi or Ti.
func parseMem(s string) (int64, error) {
	m := memRE.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("%q is not a number with Ki, Mi, Gi or Ti, or bytes", s)
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is out of range", s)
	}
	shift := map[string]uint{"": 0, "Ki": 10, "Mi": 20, "Gi": 30, "Ti": 40}[m[2]]
	if n > math.MaxInt64>>shift {
		return 0, fmt.Errorf("%q is out of range", s)
	}
	if n == 0 {
		return 0, fmt.Errorf("%q must be more than zero", s)
	}
	return n << shift, nil
}

func parseMode(s string) (gssv1.StartMode, error) {
	switch s {
	case "auto":
		return gssv1.StartMode_START_MODE_AUTO, nil
	case "cold":
		return gssv1.StartMode_START_MODE_COLD, nil
	case "restore":
		return gssv1.StartMode_START_MODE_RESTORE, nil
	case "warm":
		return gssv1.StartMode_START_MODE_WARM, nil
	}
	return 0, fmt.Errorf("unknown start mode %q (want auto, cold, restore or warm)", s)
}

func modeName(m gssv1.StartMode) string {
	return strings.ToLower(strings.TrimPrefix(m.String(), "START_MODE_"))
}

// cmdFlags are the flags that say what to run.
type cmdFlags struct {
	code, file string
	timeout    time.Duration
}

func addCmdFlags(fs *flag.FlagSet) *cmdFlags {
	f := &cmdFlags{}
	fs.StringVar(&f.code, "c", "", "Python code to run in a fork of the primed interpreter")
	fs.StringVar(&f.file, "f", "", "file of Python code to run the same way")
	fs.DurationVar(&f.timeout, "timeout", defaultExecTimeout, "the program's time limit (at most 10m)")
	return f
}

// request builds the ExecRequest from -c, -f or the ARGV after "--". args is what
// fs parsed.
func (f *cmdFlags) request(fs *flag.FlagSet, args []string) (*gssv1.ExecRequest, error) {
	argv := fs.Args()
	if len(argv) > 0 {
		// The flag package stops at "--" or at the first argument that is not a
		// flag; only the first is a program.
		if n := len(args) - len(argv); n == 0 || args[n-1] != "--" {
			return nil, usagef("%s: unexpected argument %q (a program's arguments go after --)", fs.Name(), argv[0])
		}
	}
	given := 0
	for _, set := range []bool{f.code != "", f.file != "", len(argv) > 0} {
		if set {
			given++
		}
	}
	switch {
	case given == 0:
		return nil, usagef("%s: give the program: -c CODE, -f FILE or -- ARGV...", fs.Name())
	case given > 1:
		return nil, usagef("%s: give only one of -c CODE, -f FILE and -- ARGV...", fs.Name())
	}
	if f.timeout <= 0 {
		return nil, usagef("%s: --timeout must be positive, got %v", fs.Name(), f.timeout)
	}
	req := &gssv1.ExecRequest{Timeout: durationpb.New(f.timeout)}
	switch {
	case f.code != "":
		req.Command = &gssv1.ExecRequest_PythonCode{PythonCode: f.code}
	case f.file != "":
		b, err := os.ReadFile(f.file)
		if err != nil {
			return nil, err
		}
		req.Command = &gssv1.ExecRequest_PythonCode{PythonCode: string(b)}
	default:
		req.Command = &gssv1.ExecRequest_Argv{Argv: &gssv1.Argv{Args: argv}}
	}
	return req, nil
}

// execStream runs req, copying the program's output to stdout and stderr as it
// arrives, and returns its exit status and when it arrived. Per spec D34 a stream
// that ends without an exit event did not see the program exit: that is an error,
// the stream's status if it ended with one.
func (c *cli) execStream(ctx context.Context, sc gssv1.SandboxServiceClient, req *gssv1.ExecRequest) (*gssv1.ExitStatus, time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, req.GetTimeout().AsDuration()+execSlack)
	defer cancel()
	stream, err := sc.Exec(ctx, req)
	if err != nil {
		return nil, time.Time{}, err
	}
	var exit *gssv1.ExitStatus
	var exitAt time.Time
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if exit != nil {
				break // the result is in; how the stream closed after it does not change it
			}
			return nil, time.Time{}, err
		}
		switch e := ev.GetEvent().(type) {
		case *gssv1.ExecEvent_Stdout:
			c.stdout.Write(e.Stdout)
		case *gssv1.ExecEvent_Stderr:
			c.stderr.Write(e.Stderr)
		case *gssv1.ExecEvent_Exit:
			if exit == nil {
				exit, exitAt = e.Exit, c.now()
			}
		}
	}
	if exit == nil {
		return nil, time.Time{}, errors.New("the exec stream ended without the program's exit")
	}
	return exit, exitAt, nil
}

// exitNotes says on stderr why a program ended when it did not exit by itself.
func (c *cli) exitNotes(exit *gssv1.ExitStatus, timeout time.Duration) {
	if exit.GetTimedOut() {
		fmt.Fprintf(c.stderr, "gssctl: the program timed out after %v and was killed (exit code %d)\n", timeout, exit.GetCode())
	}
	if exit.GetOomKilled() {
		fmt.Fprintf(c.stderr, "gssctl: the program ran out of memory and was killed (exit code %d)\n", exit.GetCode())
	}
}

func createSandbox(ctx context.Context, sc gssv1.SandboxServiceClient, spec *gssv1.SandboxSpec) (*gssv1.Sandbox, error) {
	ctx, cancel := context.WithTimeout(ctx, createTimeout)
	defer cancel()
	sb, err := sc.CreateSandbox(ctx, &gssv1.CreateSandboxRequest{Spec: spec})
	if err != nil {
		return nil, fmt.Errorf("create: %s", describe(err))
	}
	return sb, nil
}

// destroySandbox destroys whatever incarnation of id is live.
func destroySandbox(ctx context.Context, sc gssv1.SandboxServiceClient, id string) error {
	ctx, cancel := context.WithTimeout(ctx, destroyTimeout)
	defer cancel()
	if _, err := sc.DestroySandbox(ctx, &gssv1.DestroySandboxRequest{SandboxId: id}); err != nil {
		return fmt.Errorf("destroy %s: %s", id, describe(err))
	}
	return nil
}

func (c *cli) run(ctx context.Context, args []string) (int, error) {
	fs := c.flagSet("run")
	sf := addSpecFlags(fs)
	cf := addCmdFlags(fs)
	keep := fs.Bool("keep", false, "keep the sandbox afterwards (it lives until its TTL or gssctl destroy)")
	if err := parse(fs, args); err != nil {
		return 0, err
	}
	spec, err := sf.spec()
	if err != nil {
		return 0, err
	}
	req, err := cf.request(fs, args)
	if err != nil {
		return 0, err
	}
	conn, sc, err := c.connect()
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	sent := c.now()
	sb, err := createSandbox(ctx, sc, spec)
	if err != nil {
		return 0, err
	}
	req.SandboxId = sb.GetId()
	exit, exitAt, execErr := c.execStream(ctx, sc, req)
	if !*keep {
		// Also after Ctrl-C: the sandbox would otherwise live until its TTL.
		if err := destroySandbox(context.WithoutCancel(ctx), sc, sb.GetId()); err != nil {
			fmt.Fprintf(c.stderr, "gssctl: %v; it is removed when its TTL ends\n", err)
		}
	}
	if execErr != nil {
		return 0, fmt.Errorf("exec in sandbox %s on node %s: %s", sb.GetId(), sb.GetNodeId(), describe(execErr))
	}
	c.exitNotes(exit, cf.timeout)
	ready := "-"
	if sb.GetStartLatency() != nil {
		ready = msString(sb.GetStartLatency().AsDuration())
	}
	fmt.Fprintf(c.stderr, "sandbox %s node=%s group=%s start=%s ready=%s total=%s\n",
		sb.GetId(), sb.GetNodeId(), sb.GetNodeGroup(), modeName(sb.GetStartedAs()), ready, msString(exitAt.Sub(sent)))
	return int(exit.GetCode()), nil
}

// msString renders d in whole milliseconds, "85ms".
func msString(d time.Duration) string {
	return fmt.Sprintf("%dms", d.Round(time.Millisecond).Milliseconds())
}

func (c *cli) create(ctx context.Context, args []string) error {
	fs := c.flagSet("create")
	sf := addSpecFlags(fs)
	if err := parse(fs, args); err != nil {
		return err
	}
	if err := noArgs(fs); err != nil {
		return err
	}
	spec, err := sf.spec()
	if err != nil {
		return err
	}
	conn, sc, err := c.connect()
	if err != nil {
		return err
	}
	defer conn.Close()
	sb, err := createSandbox(ctx, sc, spec)
	if err != nil {
		return err
	}
	fmt.Fprintln(c.stdout, sb.GetId())
	return nil
}

func (c *cli) exec(ctx context.Context, args []string) (int, error) {
	id, rest, err := sandboxID("exec", args)
	if err != nil {
		return 0, err
	}
	fs := c.flagSet("exec")
	cf := addCmdFlags(fs)
	if err := parse(fs, rest); err != nil {
		return 0, err
	}
	req, err := cf.request(fs, rest)
	if err != nil {
		return 0, err
	}
	req.SandboxId = id
	conn, sc, err := c.connect()
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	exit, _, err := c.execStream(ctx, sc, req)
	if err != nil {
		return 0, fmt.Errorf("exec in sandbox %s: %s", id, describe(err))
	}
	c.exitNotes(exit, cf.timeout)
	return int(exit.GetCode()), nil
}

func (c *cli) destroy(ctx context.Context, args []string) error {
	id, rest, err := sandboxID("destroy", args)
	if err != nil {
		return err
	}
	fs := c.flagSet("destroy")
	if err := parse(fs, rest); err != nil {
		return err
	}
	if err := noArgs(fs); err != nil {
		return err
	}
	conn, sc, err := c.connect()
	if err != nil {
		return err
	}
	defer conn.Close()
	return destroySandbox(ctx, sc, id)
}

// table writes tab-separated rows as aligned columns.
func (c *cli) table(rows [][]string) error {
	tw := tabwriter.NewWriter(c.stdout, 0, 8, 2, ' ', 0)
	for _, r := range rows {
		fmt.Fprintln(tw, strings.Join(r, "\t"))
	}
	return tw.Flush()
}

func cpuString(m int64) string {
	if m == 0 {
		return "0"
	}
	return fmt.Sprintf("%dm", m)
}

func memString(b int64) string {
	switch {
	case b == 0:
		return "0"
	case b%(1<<20) == 0:
		return fmt.Sprintf("%dMi", b>>20)
	case b%(1<<10) == 0:
		return fmt.Sprintf("%dKi", b>>10)
	}
	return strconv.FormatInt(b, 10)
}

func (c *cli) query(ctx context.Context, cmd string, args []string, fn func(context.Context, gssv1.SandboxServiceClient) error) error {
	fs := c.flagSet(cmd)
	if err := parse(fs, args); err != nil {
		return err
	}
	if err := noArgs(fs); err != nil {
		return err
	}
	conn, sc, err := c.connect()
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	return fn(ctx, sc)
}

func (c *cli) ps(ctx context.Context, args []string) error {
	return c.query(ctx, "ps", args, func(ctx context.Context, sc gssv1.SandboxServiceClient) error {
		resp, err := sc.ListSandboxes(ctx, &gssv1.ListSandboxesRequest{})
		if err != nil {
			return fmt.Errorf("list sandboxes: %s", describe(err))
		}
		sbs := slices.Clone(resp.GetSandboxes())
		sort.SliceStable(sbs, func(i, j int) bool {
			a, b := sbs[i].GetCreatedAt().AsTime(), sbs[j].GetCreatedAt().AsTime()
			if !a.Equal(b) {
				return a.Before(b)
			}
			return sbs[i].GetId() < sbs[j].GetId()
		})
		rows := [][]string{{"ID", "STATE", "NODE", "GROUP", "START", "READY", "CPU", "MEM", "EXPIRES"}}
		for _, sb := range sbs {
			start, ready, expires := "-", "-", "-"
			if sb.GetStartedAs() != gssv1.StartMode_START_MODE_AUTO {
				start = modeName(sb.GetStartedAs())
			}
			if sb.GetStartLatency() != nil {
				ready = msString(sb.GetStartLatency().AsDuration())
			}
			if sb.GetExpiresAt() != nil {
				if left := sb.GetExpiresAt().AsTime().Sub(c.now()).Round(time.Second); left > 0 {
					expires = "in " + left.String()
				} else {
					expires = "expired"
				}
			}
			r := sb.GetSpec().GetResources()
			rows = append(rows, []string{
				sb.GetId(), strings.TrimPrefix(sb.GetState().String(), "SANDBOX_STATE_"), dash(sb.GetNodeId()), dash(sb.GetNodeGroup()),
				start, ready, cpuString(r.GetCpuMillis()), memString(r.GetMemoryBytes()), expires,
			})
		}
		return c.table(rows)
	})
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func (c *cli) nodes(ctx context.Context, args []string) error {
	return c.query(ctx, "nodes", args, func(ctx context.Context, sc gssv1.SandboxServiceClient) error {
		resp, err := sc.ListNodes(ctx, &gssv1.ListNodesRequest{})
		if err != nil {
			return fmt.Errorf("list nodes: %s", describe(err))
		}
		ns := slices.Clone(resp.GetNodes())
		sort.SliceStable(ns, func(i, j int) bool { return ns[i].GetInfo().GetId() < ns[j].GetInfo().GetId() })
		// scripts/cluster.sh finds the NODE, STATE and WARM columns by their headers.
		rows := [][]string{{"NODE", "GROUP", "STATE", "ADDRESS", "CPU", "MEM", "SANDBOXES", "WARM", "HEALTH", "HEARTBEAT"}}
		for _, n := range ns {
			info, alloc := n.GetInfo(), n.GetAllocated()
			hb := "-"
			if n.GetLastHeartbeat() != nil {
				hb = c.now().Sub(n.GetLastHeartbeat().AsTime()).Round(100*time.Millisecond).String() + " ago"
			}
			rows = append(rows, []string{
				info.GetId(), info.GetGroup(), strings.TrimPrefix(n.GetState().String(), "NODE_STATE_"), info.GetAddress(),
				cpuString(alloc.GetCpuMillis()) + "/" + cpuString(info.GetCapacity().GetCpuMillis()),
				memString(alloc.GetMemoryBytes()) + "/" + memString(info.GetCapacity().GetMemoryBytes()),
				fmt.Sprintf("%d/%d", n.GetSandboxes(), info.GetMaxSandboxes()),
				strconv.Itoa(int(n.GetWarmReady())), fmt.Sprintf("%.2f", n.GetHealth()), hb,
			})
		}
		return c.table(rows)
	})
}

func (c *cli) explain(ctx context.Context, args []string) error {
	id, rest, err := sandboxID("explain", args)
	if err != nil {
		return err
	}
	return c.query(ctx, "explain", rest, func(ctx context.Context, sc gssv1.SandboxServiceClient) error {
		sb, err := sc.GetSandbox(ctx, &gssv1.GetSandboxRequest{SandboxId: id})
		if err != nil {
			return fmt.Errorf("get sandbox %s: %s", id, describe(err))
		}
		state := strings.TrimPrefix(sb.GetState().String(), "SANDBOX_STATE_")
		if sb.GetNodeId() != "" {
			fmt.Fprintf(c.stdout, "sandbox %s: %s on node %s (group %s), started as %s, incarnation %d\n",
				sb.GetId(), state, sb.GetNodeId(), sb.GetNodeGroup(), modeName(sb.GetStartedAs()), sb.GetIncarnation())
		} else {
			fmt.Fprintf(c.stdout, "sandbox %s: %s, not on a node\n", sb.GetId(), state)
		}
		if sb.GetMessage() != "" {
			fmt.Fprintf(c.stdout, "message: %s\n", sb.GetMessage())
		}
		p := sb.GetPlacement()
		if p == nil {
			fmt.Fprintln(c.stdout, "no placement recorded")
			return nil
		}
		fmt.Fprintf(c.stdout, "nodes considered: %d, feasible: %d\n", p.GetNodesTotal(), p.GetNodesFeasible())
		if len(p.GetTop()) == 0 {
			fmt.Fprintln(c.stdout, "no candidates")
			return nil
		}
		var plugins []string
		for _, s := range p.GetTop() {
			for name := range s.GetPlugins() {
				if !slices.Contains(plugins, name) {
					plugins = append(plugins, name)
				}
			}
		}
		sort.Strings(plugins)
		fmt.Fprintln(c.stdout, "top candidates (* = the node it runs on):")
		rows := [][]string{append([]string{"", "NODE", "TOTAL"}, plugins...)}
		for _, s := range p.GetTop() {
			mark := ""
			if s.GetNodeId() == sb.GetNodeId() {
				mark = "*"
			}
			row := []string{mark, s.GetNodeId(), fmt.Sprintf("%.3f", s.GetTotal())}
			for _, name := range plugins {
				if v, ok := s.GetPlugins()[name]; ok {
					row = append(row, fmt.Sprintf("%.3f", v))
				} else {
					row = append(row, "-")
				}
			}
			rows = append(rows, row)
		}
		return c.table(rows)
	})
}
