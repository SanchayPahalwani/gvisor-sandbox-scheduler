package main

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bench"
)

type startConfig struct {
	N        int // cycles per mode; the first is a warm-up and is not recorded
	Modes    []gssv1.StartMode
	Group    string        // node_group constraint of every sandbox
	WarmWait time.Duration // how long to wait for a warm sandbox before a warm cycle
}

func newStartConfig(n int, modes []gssv1.StartMode, group string) startConfig {
	return startConfig{N: n, Modes: modes, Group: group, WarmWait: 20 * time.Second}
}

func startCmd(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, cf := newFlagSet("start", stderr)
	n := fs.Int("n", 30, "cycles per mode, the first of them a warm-up that is not recorded")
	modes := fs.String("modes", "cold,restore,warm", "start paths to measure, in order")
	group := fs.String("group", "a", "node group every sandbox is constrained to")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if *n < 2 {
		return fmt.Errorf("--n %d: need at least 2 (the first cycle of each mode is a warm-up)", *n)
	}
	var list []gssv1.StartMode
	for _, s := range strings.Split(*modes, ",") {
		m, err := parseMode(strings.TrimSpace(s))
		if err != nil {
			return err
		}
		if slices.Contains(list, m) {
			return fmt.Errorf("--modes lists %s twice", s)
		}
		list = append(list, m)
	}
	cfg := newStartConfig(*n, list, *group)
	r, err := cf.measure(args, func(c gssv1.SandboxServiceClient) (*bench.Run, error) {
		return runStart(ctx, c, cfg, stderr)
	})
	if err != nil {
		return err
	}
	return printStart(stdout, r, cfg, cf.out)
}

// runStart measures, for each mode in turn, N sequential create -> Exec -> destroy cycles
// with the start path forced.
func runStart(ctx context.Context, c gssv1.SandboxServiceClient, cfg startConfig, log io.Writer) (*bench.Run, error) {
	nodes, err := listNodes(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	if !slices.ContainsFunc(nodes, func(n *gssv1.Node) bool { return inGroup(n, cfg.Group) && ready(n) }) {
		return nil, fmt.Errorf("no READY node in group %q", cfg.Group)
	}
	var modeNames []string
	for _, m := range cfg.Modes {
		modeNames = append(modeNames, modeName(m))
	}
	r := newRun("start", map[string]any{
		"n":                cfg.N,
		"modes":            strings.Join(modeNames, ","),
		"group":            cfg.Group,
		"cpu_millis":       gssv1.DefaultCPUMillis,
		"memory_mib":       gssv1.DefaultMemoryBytes >> 20,
		"code":             helloCode,
		"warm_wait":        cfg.WarmWait.String(),
		"warm_retry_pause": staleWarmPause.String(),
		"warm_retries_max": staleWarmRetries,
	})
	began := time.Now()
	var tally errorTally
	retries := 0
	for _, mode := range cfg.Modes {
		name := modeName(mode)
		spec := func() *gssv1.SandboxSpec {
			return &gssv1.SandboxSpec{
				Resources: &gssv1.Resources{CpuMillis: gssv1.DefaultCPUMillis, MemoryBytes: gssv1.DefaultMemoryBytes},
				NodeGroup: cfg.Group,
				StartMode: mode,
			}
		}
		var readyMS, createMS, firstMS []float64
		failed := 0
		for i := range cfg.N {
			if mode == gssv1.StartMode_START_MODE_WARM {
				if err := waitWarm(ctx, c, cfg.Group, cfg.WarmWait); err != nil {
					if ctx.Err() != nil {
						return nil, ctx.Err()
					}
					tally.add(fmt.Sprintf("%s cycle %d", name, i), err)
					failed++
					continue
				}
			}
			res, err := runCycle(ctx, c, spec, helloCode)
			retries += res.retries
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if err != nil {
				tally.add(fmt.Sprintf("%s cycle %d", name, i), err)
				failed++
				continue
			}
			if i == 0 {
				continue // warm-up
			}
			readyMS = append(readyMS, res.readyMS)
			createMS = append(createMS, res.createMS)
			firstMS = append(firstMS, res.firstResultMS)
		}
		fmt.Fprintf(log, "start %s: %d cycles, %d failed\n", name, cfg.N, failed)
		r.Series = append(r.Series,
			bench.Summarize(name+"_ready_ms", "ms", readyMS),
			bench.Summarize(name+"_create_ms", "ms", createMS),
			bench.Summarize(name+"_first_result_ms", "ms", firstMS))
	}
	r.Seconds = time.Since(began).Seconds()
	r.Errors = tally.n
	r.Scalars["warm_retries"] = float64(retries)
	r.Notes = append([]string{
		"the first cycle of each mode is a warm-up and is not recorded",
		"<mode>_ready_ms is the start_latency in the CreateSandbox response: node agent receives Create -> sandbox ready",
		"<mode>_create_ms is the client-side CreateSandbox round trip",
		`<mode>_first_result_ms is CreateSandbox sent -> exit event of the Exec of print("hello")`,
		"a cycle with any failed call (create, Exec, destroy) or a start path other than the forced one counts in errors and gives no sample",
		"warm_retries counts forced-warm creates retried after FailedPrecondition (stale warm count); a retried attempt is not a sample and its successor's clock starts at its own Create",
	}, tally.notes()...)
	return r, nil
}

func inGroup(n *gssv1.Node, group string) bool {
	return group == "" || n.GetInfo().GetGroup() == group
}

func ready(n *gssv1.Node) bool { return n.GetState() == gssv1.NodeState_NODE_STATE_READY }

// waitWarm polls ListNodes until a READY node in group reports a warm sandbox, for at
// most limit.
func waitWarm(ctx context.Context, c gssv1.SandboxServiceClient, group string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		nodes, err := listNodes(ctx, c)
		if err == nil && slices.ContainsFunc(nodes, func(n *gssv1.Node) bool {
			return inGroup(n, group) && ready(n) && n.GetWarmReady() > 0
		}) {
			return nil
		}
		if !time.Now().Before(deadline) {
			if err != nil {
				return fmt.Errorf("no node in group %q reported a warm sandbox within %v (last ListNodes: %w)", group, limit, err)
			}
			return fmt.Errorf("no node in group %q reported a warm sandbox within %v", group, limit)
		}
		if err := sleepCtx(ctx, 100*time.Millisecond); err != nil {
			return err
		}
	}
}

func printStart(w io.Writer, r *bench.Run, cfg startConfig, out string) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "MODE\tN\tREADY P50\tREADY P95\tCREATE P50\tCREATE P95\tFIRST RESULT P50\tFIRST RESULT P95")
	for _, m := range cfg.Modes {
		name := modeName(m)
		get := func(kind string) bench.Series {
			for _, s := range r.Series {
				if s.Name == name+"_"+kind+"_ms" {
					return s
				}
			}
			return bench.Series{}
		}
		rd, cr, fr := get("ready"), get("create"), get("first_result")
		fmt.Fprintf(tw, "%s\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\n", name, rd.Count, rd.P50, rd.P95, cr.P50, cr.P95, fr.P50, fr.P95)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprintf(w, "milliseconds; errors: %d; warm retries: %.0f; wrote %s\n", r.Errors, r.Scalars["warm_retries"], out)
	return nil
}
