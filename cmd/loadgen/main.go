// Command loadgen drives the scheduler's SandboxService and writes benchmark results
// as JSON under results/.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bench"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/telemetry"
)

// flushTimeout bounds the export of what telemetry still holds at exit.
const flushTimeout = 5 * time.Second

const usage = `usage: loadgen <subcommand> [flags]

subcommands:
  start       start latency per start path (cold, restore, warm)
  throughput  closed-loop create, Exec, destroy against the whole cluster
  density     live sandboxes on one node against start and Exec latency
  grouploss   local failure-domain test: kill every node of one group under load

common flags: --scheduler 127.0.0.1:7000, --out results/<subcommand>.json
run "loadgen <subcommand> -h" for its flags
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// Telemetry is exported only while OTEL_EXPORTER_OTLP_ENDPOINT is set;
	// the benchmark targets refuse to run with it set (D25).
	shutdown, err := telemetry.Setup(ctx, "gss-loadgen")
	if err == nil {
		err = run(ctx, os.Args, os.Stdout, os.Stderr)
		fctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
		if ferr := shutdown(fctx); ferr != nil {
			fmt.Fprintln(os.Stderr, "loadgen: telemetry flush failed:", ferr)
		}
		cancel()
	}
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

// run dispatches args (os.Args) to a subcommand. Results go to stdout, progress to stderr.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) < 2 {
		fmt.Fprint(stderr, usage)
		return errors.New("no subcommand")
	}
	var err error
	switch args[1] {
	case "start":
		err = startCmd(ctx, args, stdout, stderr)
	case "throughput":
		err = throughputCmd(ctx, args, stdout, stderr)
	case "density":
		err = densityCmd(ctx, args, stdout, stderr)
	case "grouploss":
		err = groupLossCmd(ctx, args, stdout, stderr)
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return nil
	default:
		fmt.Fprint(stderr, usage)
		return fmt.Errorf("unknown subcommand %q", args[1])
	}
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	return err
}

// commonFlags are the flags every subcommand has.
type commonFlags struct {
	scheduler string
	out       string
}

func newFlagSet(sub string, stderr io.Writer) (*flag.FlagSet, *commonFlags) {
	fs := flag.NewFlagSet("loadgen "+sub, flag.ContinueOnError)
	fs.SetOutput(stderr)
	cf := &commonFlags{}
	fs.StringVar(&cf.scheduler, "scheduler", "127.0.0.1:7000", "address of the scheduler's SandboxService")
	fs.StringVar(&cf.out, "out", "results/"+sub+".json", "write the results as JSON to this file")
	return fs, cf
}

func parseFlags(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("%s: unexpected arguments %q", fs.Name(), fs.Args())
	}
	return nil
}

// measure connects to the scheduler, runs fn, records the command line and writes the
// results file.
func (cf *commonFlags) measure(args []string, fn func(gssv1.SandboxServiceClient) (*bench.Run, error)) (*bench.Run, error) {
	conn, err := dial(cf.scheduler)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	r, err := fn(gssv1.NewSandboxServiceClient(conn))
	if err != nil {
		return nil, err
	}
	r.Command = commandLine(args)
	if err := r.Write(cf.out); err != nil {
		return nil, err
	}
	return r, nil
}

func newRun(name string, params map[string]any) *bench.Run {
	return &bench.Run{
		Name:      name,
		StartedAt: time.Now().UTC(),
		Params:    params,
		Series:    []bench.Series{},
		Scalars:   map[string]float64{},
	}
}

// commandLine joins args into the command line that produced a run. Arguments that a
// shell would split or expand are single-quoted so the line can be run again as is.
func commandLine(args []string) string {
	out := make([]string, len(args))
	for i, a := range args {
		if a == "" || strings.ContainsAny(a, " \t\n'\"\\$`*?[]{}()<>|&;#~!") {
			a = "'" + strings.ReplaceAll(a, "'", `'\''`) + "'"
		}
		out[i] = a
	}
	return strings.Join(out, " ")
}

// errorTally counts failures and keeps the first few messages for the run's notes.
type errorTally struct {
	n     int
	first []string
}

const maxErrorNotes = 5

func (t *errorTally) add(what string, err error) {
	t.n++
	if len(t.first) < maxErrorNotes {
		t.first = append(t.first, fmt.Sprintf("error: %s: %v", what, err))
	}
}

func (t *errorTally) notes() []string {
	out := append([]string(nil), t.first...)
	if t.n > len(t.first) {
		out = append(out, fmt.Sprintf("%d more errors are not listed", t.n-len(t.first)))
	}
	return out
}

func modeName(m gssv1.StartMode) string {
	return strings.ToLower(strings.TrimPrefix(m.String(), "START_MODE_"))
}

func parseMode(s string) (gssv1.StartMode, error) {
	switch s {
	case "cold":
		return gssv1.StartMode_START_MODE_COLD, nil
	case "restore":
		return gssv1.StartMode_START_MODE_RESTORE, nil
	case "warm":
		return gssv1.StartMode_START_MODE_WARM, nil
	}
	return 0, fmt.Errorf("unknown start mode %q (want cold, restore or warm)", s)
}
