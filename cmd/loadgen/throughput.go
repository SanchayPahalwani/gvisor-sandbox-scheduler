package main

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bench"
)

type throughputConfig struct {
	Workers  int
	Duration time.Duration
}

// failurePause keeps a worker whose cycle failed from retrying in a tight loop.
const failurePause = 100 * time.Millisecond

func throughputCmd(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, cf := newFlagSet("throughput", stderr)
	workers := fs.Int("workers", 8, "closed-loop workers")
	duration := fs.Duration("duration", 60*time.Second, "how long workers start new cycles")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if *workers < 1 || *duration <= 0 {
		return fmt.Errorf("--workers must be at least 1 and --duration positive")
	}
	cfg := throughputConfig{Workers: *workers, Duration: *duration}
	r, err := cf.measure(args, func(c gssv1.SandboxServiceClient) (*bench.Run, error) {
		return runThroughput(ctx, c, cfg, stderr)
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "%d workers for %v: %.2f sandboxes/s over %.1f s, errors %d\n",
		cfg.Workers, cfg.Duration, r.Scalars["sandboxes_per_sec"], r.Seconds, r.Errors)
	fmt.Fprintf(stdout, "start paths: warm %.3f, restore %.3f, cold %.3f\n",
		r.Scalars["share_warm"], r.Scalars["share_restore"], r.Scalars["share_cold"])
	for _, s := range r.Series {
		fmt.Fprintf(stdout, "%s: n %d, p50 %.1f, p95 %.1f, p99 %.1f\n", s.Name, s.Count, s.P50, s.P95, s.P99)
	}
	fmt.Fprintf(stdout, "wrote %s\n", cf.out)
	return nil
}

// runThroughput runs Workers closed loops of create (AUTO) -> Exec -> destroy. Workers
// start new cycles until Duration has passed; cycles in flight then finish, and the
// rate is completed cycles over the whole wall time including them.
func runThroughput(ctx context.Context, c gssv1.SandboxServiceClient, cfg throughputConfig, log io.Writer) (*bench.Run, error) {
	nodes, err := listNodes(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	if !slices.ContainsFunc(nodes, ready) {
		return nil, fmt.Errorf("no READY node")
	}
	r := newRun("throughput", map[string]any{
		"workers":    cfg.Workers,
		"duration":   cfg.Duration.String(),
		"start_mode": "auto",
		"cpu_millis": gssv1.DefaultCPUMillis,
		"memory_mib": gssv1.DefaultMemoryBytes >> 20,
		"code":       helloCode,
	})
	spec := func() *gssv1.SandboxSpec {
		return &gssv1.SandboxSpec{Resources: &gssv1.Resources{CpuMillis: gssv1.DefaultCPUMillis, MemoryBytes: gssv1.DefaultMemoryBytes}}
	}
	var (
		mu       sync.Mutex
		firstMS  []float64
		readyMS  []float64
		started  = map[gssv1.StartMode]int{}
		tally    errorTally
		wg       sync.WaitGroup
		began    = time.Now()
		stopFrom = began.Add(cfg.Duration)
	)
	for w := range cfg.Workers {
		wg.Go(func() {
			for time.Now().Before(stopFrom) && ctx.Err() == nil {
				res, err := runCycle(ctx, c, spec, helloCode)
				if ctx.Err() != nil {
					return
				}
				mu.Lock()
				if res.cleanupErr != nil {
					tally.add(fmt.Sprintf("worker %d", w), res.cleanupErr)
				}
				if err != nil {
					tally.add(fmt.Sprintf("worker %d", w), err)
				} else {
					firstMS = append(firstMS, res.firstResultMS)
					readyMS = append(readyMS, res.readyMS)
					started[res.sb.GetStartedAs()]++
				}
				mu.Unlock()
				if err != nil {
					_ = sleepCtx(ctx, failurePause)
				}
			}
		})
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	elapsed := time.Since(began)
	r.Seconds = elapsed.Seconds()
	r.Errors = tally.n
	ok := len(firstMS)
	share := func(m gssv1.StartMode) float64 {
		if ok == 0 {
			return 0
		}
		return float64(started[m]) / float64(ok)
	}
	r.Scalars["sandboxes_per_sec"] = float64(ok) / elapsed.Seconds()
	r.Scalars["share_warm"] = share(gssv1.StartMode_START_MODE_WARM)
	r.Scalars["share_restore"] = share(gssv1.StartMode_START_MODE_RESTORE)
	r.Scalars["share_cold"] = share(gssv1.StartMode_START_MODE_COLD)
	r.Series = append(r.Series, bench.Summarize("first_result_ms", "ms", firstMS), bench.Summarize("ready_ms", "ms", readyMS))
	r.Notes = []string{
		"sandboxes_per_sec is completed create -> Exec -> destroy cycles divided by the wall time from the first create to the end of the last cycle",
		"share_* is the fraction of completed cycles whose sandbox started on that path (started_as)",
		`first_result_ms is CreateSandbox sent -> exit event of print("hello"); ready_ms is the start_latency in the response`,
		"a cycle with any failed call counts in errors and in no other figure; a destroy that fails after such a failure counts as one more error",
	}
	if other := ok - started[gssv1.StartMode_START_MODE_WARM] - started[gssv1.StartMode_START_MODE_RESTORE] - started[gssv1.StartMode_START_MODE_COLD]; other > 0 {
		r.Notes = append(r.Notes, fmt.Sprintf("%d completed cycles reported no start path, so the shares sum to less than 1", other))
	}
	r.Notes = append(r.Notes, tally.notes()...)
	fmt.Fprintf(log, "throughput: %d cycles completed, %d failed\n", ok, tally.n)
	return r, nil
}
