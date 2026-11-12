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
	type keptSandbox struct {
		id  string
		inc int32 // incarnation at creation
	}
	var (
		kept    []keptSandbox
		steps   []densityStep
		tally   errorTally
		memNote string
		stopped string
	)
	destroyKept := func() {
		for _, k := range kept {
			if err := destroySandbox(cleanupCtx(ctx), c, k.id); err != nil {
				tally.add("destroy "+k.id, err)
			}
		}
		kept = nil
	}
	for _, step := range cfg.Steps {
		st := densityStep{step: step}
		var firstErr string
		fail := func(what string, err error) {
			st.failed++
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
			kept = append(kept, keptSandbox{id: sb.GetId(), inc: sb.GetIncarnation()})
		}
		st.live = len(kept)
		if mem, err := cfg.MemAvailableMiB(); err == nil {
			st.memMiB, st.memOK = mem, true
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
			if res.cleanupErr != nil {
				tally.add(fmt.Sprintf("step %d restore start", step), res.cleanupErr)
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
			id := kept[rng.IntN(len(kept))].id
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
		// A kept sandbox that the scheduler restarted (its sentry OOM-killed, say) answers
		// Execs again once the new incarnation is READY, so Exec success does not show the
		// loss. Each kept sandbox must still be READY at the incarnation it was created at;
		// one whose GetSandbox fails cannot be confirmed, which fails the step as well.
		for _, k := range kept {
			sb, err := getSandbox(ctx, c, k.id)
			if ctx.Err() != nil {
				destroyKept()
				return nil, ctx.Err()
			}
			switch {
			case err != nil:
				st.unconfirmed++
				fail("check of live sandbox "+k.id, err)
			case sb.GetState() != gssv1.SandboxState_SANDBOX_STATE_READY || sb.GetIncarnation() != k.inc:
				st.lost++
				fail("live sandbox "+k.id+" lost", fmt.Errorf("%s at incarnation %d, created at incarnation %d", sb.GetState(), sb.GetIncarnation(), k.inc))
			}
		}
		st.start, st.exec = bench.Summarize("start", "ms", startMS), bench.Summarize("exec", "ms", execMS)
		steps = append(steps, st)
		r.Rows = append(r.Rows, st.row())
		fmt.Fprintf(log, "density step %d: live %d, start p95 %.1f ms, exec p95 %.1f ms, lost %d, unconfirmed %d, failed %d\n",
			step, st.live, st.start.P95, st.exec.P95, st.lost, st.unconfirmed, st.failed)
		if st.failed > 0 {
			stopped = fmt.Sprintf("the ramp stopped at step %d (%d live) after %d failures", step, st.live, st.failed)
			if st.lost > 0 {
				stopped += fmt.Sprintf(", %d of them live sandboxes lost (not READY at the incarnation they were created at)", st.lost)
			}
			if st.unconfirmed > 0 {
				stopped += fmt.Sprintf(", %d of them live sandboxes not confirmed (GetSandbox failed)", st.unconfirmed)
			}
			stopped += "; first: " + firstErr
			break
		}
	}
	if stopped == "" {
		stopped = fmt.Sprintf("the ramp reached the last step (%d) without a failure", cfg.Steps[len(cfg.Steps)-1])
	}
	destroyKept()
	r.Seconds = time.Since(began).Seconds()
	r.Errors = tally.n

	baseline, maxLive, verdictNotes := densityVerdict(steps)
	r.Scalars["baseline_start_p95_ms"] = baseline
	r.Scalars["max_live_within_2x"] = float64(maxLive)
	r.Notes = []string{
		stopped,
		"start_* is the start_latency in the CreateSandbox response of a forced restore (node agent receives Create -> ready), from per_step create -> Exec -> destroy cycles run while the step's sandboxes are live",
		`exec_* is Exec sent -> exit event of print("hello") in a live sandbox picked at random`,
		"live is the number of sandboxes this run created and kept at that step",
		"lost counts kept sandboxes that GetSandbox, after the step's measurements, did not show READY at the incarnation they were created at; a restarted sandbox answers Execs again, so Exec success alone would hide it",
		"unconfirmed counts kept sandboxes whose GetSandbox call failed after the step's measurements, so their state is not known",
		"failed counts failed creates, restore starts and Execs at that step, and each lost or unconfirmed sandbox",
		"max_live_within_2x is the largest live count of a step with no failures and start_p95_ms <= 2 x baseline_start_p95_ms (the first step's)",
		"node_mem_available_mib is MemAvailable from /proc/meminfo on the machine loadgen runs on (the VM that runs the node), read after the step's sandboxes were created",
	}
	r.Notes = append(r.Notes, verdictNotes...)
	if memNote != "" {
		r.Notes = append(r.Notes, memNote)
	}
	r.Notes = append(r.Notes, tally.notes()...)
	return r, nil
}

// densityStep is one step of the ramp.
type densityStep struct {
	step, live, lost, failed int
	unconfirmed              int
	start, exec              bench.Series // restore-start ready latency; Exec latency
	memMiB                   float64
	memOK                    bool
}

func (s densityStep) row() map[string]any {
	row := map[string]any{
		"step": s.step, "live": s.live, "lost": s.lost, "unconfirmed": s.unconfirmed, "failed": s.failed,
		"start_p50_ms": s.start.P50, "start_p95_ms": s.start.P95,
		"exec_p50_ms": s.exec.P50, "exec_p95_ms": s.exec.P95,
	}
	if s.memOK {
		row["node_mem_available_mib"] = s.memMiB
	}
	return row
}

// densityVerdict returns the first step's start p95 (the baseline), the largest live
// count of a step with no failures and start p95 within 2x of it, and notes on how
// the verdict came out.
func densityVerdict(steps []densityStep) (baseline float64, maxLive int, notes []string) {
	if len(steps) == 0 {
		return 0, 0, nil
	}
	first := steps[0]
	if first.start.Count == 0 {
		return 0, 0, []string{"the first step has no successful restore start, so there is no baseline: baseline_start_p95_ms and max_live_within_2x are 0"}
	}
	baseline = first.start.P95
	if first.failed > 0 {
		return baseline, 0, []string{fmt.Sprintf("the first step had %d failures; its restore starts give the baseline, but no step qualifies, so max_live_within_2x is 0", first.failed)}
	}
	var over []int
	for _, s := range steps {
		if s.failed > 0 || s.start.Count == 0 {
			continue
		}
		if s.start.P95 > 2*baseline {
			over = append(over, s.step)
			continue
		}
		maxLive = max(maxLive, s.live)
	}
	if len(over) > 0 {
		notes = append(notes, fmt.Sprintf("start_p95_ms was above 2 x baseline at steps %v", over))
	}
	return baseline, maxLive, notes
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
	fmt.Fprintln(tw, "STEP\tLIVE\tSTART P50\tSTART P95\tEXEC P50\tEXEC P95\tLOST\tUNCONFIRMED\tFAILED\tMEM AVAILABLE MIB")
	for _, row := range r.Rows {
		mem := "-"
		if v, ok := row["node_mem_available_mib"].(float64); ok {
			mem = fmt.Sprintf("%.0f", v)
		}
		fmt.Fprintf(tw, "%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%d\t%d\t%d\t%s\n", row["step"], row["live"],
			row["start_p50_ms"], row["start_p95_ms"], row["exec_p50_ms"], row["exec_p95_ms"], row["lost"], row["unconfirmed"], row["failed"], mem)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprintf(w, "milliseconds; baseline start p95 %.1f; max live within 2x: %.0f\n%s\nwrote %s\n",
		r.Scalars["baseline_start_p95_ms"], r.Scalars["max_live_within_2x"], r.Notes[0], out)
	return nil
}
