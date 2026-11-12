package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bench"
)

// Size of every density sandbox: the smallest the scheduler accepts (spec D31).
const (
	densityCPUMillis = 250
	densityMemBytes  = gssv1.MinMemoryBytes
	densityTTL       = 30 * time.Minute
)

type densityConfig struct {
	Steps   []int // live sandboxes to hold at each step, increasing
	PerStep int   // restore starts and Execs measured at each step
	// MemAvailableMiB reads the node machine's available memory.
	MemAvailableMiB func() (float64, error)
}

func densityCmd(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, cf := newFlagSet("density", stderr)
	steps := fs.String("steps", "5,10,20,30,40,50,60", "live sandboxes to hold at each step, increasing")
	perStep := fs.Int("per-step", 10, "restore starts and Execs measured at each step")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	cfg := densityConfig{PerStep: *perStep, MemAvailableMiB: readMemAvailableMiB}
	for _, s := range strings.Split(*steps, ",") {
		v, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || v < 1 || (len(cfg.Steps) > 0 && v <= cfg.Steps[len(cfg.Steps)-1]) {
			return fmt.Errorf("--steps %q: want increasing positive integers", *steps)
		}
		cfg.Steps = append(cfg.Steps, v)
	}
	if cfg.PerStep < 1 {
		return fmt.Errorf("--per-step must be at least 1")
	}
	r, err := cf.measure(args, func(c gssv1.SandboxServiceClient) (*bench.Run, error) {
		return runDensity(ctx, c, cfg, stderr)
	})
	if err != nil {
		return err
	}
	return printDensity(stdout, r, cf.out)
}

func densitySpec() *gssv1.SandboxSpec {
	return &gssv1.SandboxSpec{
		Resources: &gssv1.Resources{CpuMillis: densityCPUMillis, MemoryBytes: densityMemBytes},
		Ttl:       durationpb.New(densityTTL),
		StartMode: gssv1.StartMode_START_MODE_RESTORE,
	}
}

// runDensity raises the number of live sandboxes on the cluster's only node step by
// step and measures restore starts and Execs at each step. It stops after the first
// step with a failure.
func runDensity(ctx context.Context, c gssv1.SandboxServiceClient, cfg densityConfig, log io.Writer) (*bench.Run, error) {
	nodes, err := listNodes(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	if len(nodes) != 1 {
		return nil, fmt.Errorf("density needs a cluster with exactly one node and the scheduler lists %d; "+
			"make bench-density starts one (GSS_NODES=\"a1:a:7101\")", len(nodes))
	}
	if !ready(nodes[0]) {
		return nil, fmt.Errorf("node %s is %v, not READY", nodes[0].GetInfo().GetId(), nodes[0].GetState())
	}
	r := newRun("density", map[string]any{
		"steps":      cfg.Steps,
		"per_step":   cfg.PerStep,
		"node":       nodes[0].GetInfo().GetId(),
		"start_mode": "restore",
		"cpu_millis": densityCPUMillis,
		"memory_mib": densityMemBytes >> 20,
		"ttl":        densityTTL.String(),
		"code":       helloCode,
	})
	began := time.Now()
	rng := rand.New(rand.NewPCG(1, 1)) // fixed seed: which live sandbox each Exec hits
	var (
		kept    []string // IDs of the sandboxes held live
		tally   errorTally
		memNote string
		stopped string
	)
	destroyKept := func() int {
		failed := 0
		for _, id := range kept {
			if err := destroySandbox(cleanupCtx(ctx), c, id); err != nil {
				tally.add("destroy "+id, err)
				failed++
			}
		}
		kept = nil
		return failed
	}
	for _, step := range cfg.Steps {
		failed := 0
		var firstErr string
		fail := func(what string, err error) {
			failed++
			if firstErr == "" {
				firstErr = fmt.Sprintf("%s: %v", what, err)
			}
			tally.add(fmt.Sprintf("step %d %s", step, what), err)
		}
		for len(kept) < step {
			sb, err := createSandbox(ctx, c, densitySpec())
			if ctx.Err() != nil {
				destroyKept()
				return nil, ctx.Err()
			}
			if err != nil {
				fail(fmt.Sprintf("create of live sandbox %d", len(kept)+1), err)
				break
			}
			kept = append(kept, sb.GetId())
		}
		row := map[string]any{"step": step, "live": len(kept)}
		if mem, err := cfg.MemAvailableMiB(); err == nil {
			row["node_mem_available_mib"] = mem
		} else if memNote == "" {
			memNote = fmt.Sprintf("node_mem_available_mib is not recorded: %v", err)
		}
		var startMS, execMS []float64
		for range cfg.PerStep {
			res, err := runCycle(ctx, c, densitySpec, helloCode)
			if ctx.Err() != nil {
				destroyKept()
				return nil, ctx.Err()
			}
			if err != nil {
				fail("restore start", err)
				continue
			}
			startMS = append(startMS, res.readyMS)
		}
		for range cfg.PerStep {
			if len(kept) == 0 {
				break
			}
			id := kept[rng.IntN(len(kept))]
			sent := time.Now()
			exitAt, err := execCode(ctx, c, id, helloCode, execTimeout)
			if ctx.Err() != nil {
				destroyKept()
				return nil, ctx.Err()
			}
			if err != nil {
				fail("exec in "+id, err)
				continue
			}
			execMS = append(execMS, ms(exitAt.Sub(sent)))
		}
		st, ex := bench.Summarize("start", "ms", startMS), bench.Summarize("exec", "ms", execMS)
		row["start_p50_ms"], row["start_p95_ms"] = st.P50, st.P95
		row["exec_p50_ms"], row["exec_p95_ms"] = ex.P50, ex.P95
		row["failed"] = failed
		r.Rows = append(r.Rows, row)
		fmt.Fprintf(log, "density step %d: live %d, start p95 %.1f ms, exec p95 %.1f ms, failed %d\n", step, len(kept), st.P95, ex.P95, failed)
		if failed > 0 {
			stopped = fmt.Sprintf("the ramp stopped at step %d (%d live) after %d failures; first: %s", step, len(kept), failed, firstErr)
			break
		}
	}
	if stopped == "" {
		stopped = fmt.Sprintf("the ramp reached the last step (%d) without a failure", cfg.Steps[len(cfg.Steps)-1])
	}
	destroyKept()
	r.Seconds = time.Since(began).Seconds()
	r.Errors = tally.n

	baseline, maxLive, over := densityVerdict(r.Rows)
	r.Scalars["baseline_start_p95_ms"] = baseline
	r.Scalars["max_live_within_2x"] = float64(maxLive)
	r.Notes = []string{
		stopped,
		"start_* is the start_latency in the CreateSandbox response of a forced restore (node agent receives Create -> ready), from per_step create -> Exec -> destroy cycles run while the step's sandboxes are live",
		`exec_* is Exec sent -> exit event of print("hello") in a live sandbox picked at random`,
		"live is the number of sandboxes this run created and kept at that step; failed counts failed creates, restore starts and Execs at that step",
		"max_live_within_2x is the largest live count of a step with no failures and start_p95_ms <= 2 x baseline_start_p95_ms (the first step's)",
		"node_mem_available_mib is MemAvailable from /proc/meminfo on the machine loadgen runs on (the VM that runs the node), read after the step's sandboxes were created",
	}
	if len(over) > 0 {
		r.Notes = append(r.Notes, fmt.Sprintf("start_p95_ms was above 2 x baseline at steps %v", over))
	}
	if baseline == 0 {
		r.Notes = append(r.Notes, "the first step has no successful restore start, so there is no baseline and max_live_within_2x is 0")
	}
	if memNote != "" {
		r.Notes = append(r.Notes, memNote)
	}
	r.Notes = append(r.Notes, tally.notes()...)
	return r, nil
}

// densityVerdict returns the first step's start p95, the largest live count of a step
// with no failures and start p95 within 2x of it, and the failure-free steps that
// went over 2x.
func densityVerdict(rows []map[string]any) (baseline float64, maxLive int, over []int) {
	if len(rows) == 0 || rows[0]["failed"] != 0 {
		return 0, 0, nil
	}
	baseline = rows[0]["start_p95_ms"].(float64)
	if baseline == 0 {
		return 0, 0, nil
	}
	for _, row := range rows {
		if row["failed"] != 0 {
			continue
		}
		if row["start_p95_ms"].(float64) > 2*baseline {
			over = append(over, row["step"].(int))
			continue
		}
		maxLive = max(maxLive, row["live"].(int))
	}
	return baseline, maxLive, over
}

func readMemAvailableMiB() (float64, error) {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	return parseMemAvailableMiB(b)
}

// parseMemAvailableMiB returns the MemAvailable line of /proc/meminfo in whole MiB.
func parseMemAvailableMiB(data []byte) (float64, error) {
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) == 3 && f[0] == "MemAvailable:" && f[2] == "kB" {
			kb, err := strconv.ParseInt(f[1], 10, 64)
			if err != nil {
				return 0, fmt.Errorf("MemAvailable: %w", err)
			}
			return float64(kb / 1024), nil
		}
	}
	return 0, errors.New("no MemAvailable line in /proc/meminfo")
}

func printDensity(w io.Writer, r *bench.Run, out string) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "STEP\tLIVE\tSTART P50\tSTART P95\tEXEC P50\tEXEC P95\tFAILED\tMEM AVAILABLE MIB")
	for _, row := range r.Rows {
		mem := "-"
		if v, ok := row["node_mem_available_mib"].(float64); ok {
			mem = fmt.Sprintf("%.0f", v)
		}
		fmt.Fprintf(tw, "%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%d\t%s\n", row["step"], row["live"],
			row["start_p50_ms"], row["start_p95_ms"], row["exec_p50_ms"], row["exec_p95_ms"], row["failed"], mem)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprintf(w, "milliseconds; baseline start p95 %.1f; max live within 2x: %.0f\n%s\nwrote %s\n",
		r.Scalars["baseline_start_p95_ms"], r.Scalars["max_live_within_2x"], r.Notes[0], out)
	return nil
}
