// Command schedsim runs the placement policy against synthetic nodes.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"text/tabwriter"

	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/sched"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/sim"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "schedsim:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := sim.DefaultConfig()
	out := flag.String("out", "", "write results as JSON to this file")
	flag.IntVar(&cfg.Nodes, "nodes", cfg.Nodes, "number of synthetic nodes")
	flag.IntVar(&cfg.Groups, "groups", cfg.Groups, "number of node groups")
	flag.Int64Var(&cfg.Seed, "seed", cfg.Seed, "random seed")
	flag.Float64Var(&cfg.FillFraction, "fill", cfg.FillFraction, "fraction of fleet CPU requested in the fill scenario")
	flag.IntVar(&cfg.ChurnSteps, "churn", cfg.ChurnSteps, "remove-one-add-one steps in the churn scenario")
	flag.Parse()
	if cfg.Nodes <= 0 || cfg.Groups <= 0 || cfg.FillFraction <= 0 || cfg.FillFraction > 1 || cfg.ChurnSteps < 0 {
		return fmt.Errorf("invalid flags: nodes and groups must be positive, fill in (0,1], churn >= 0")
	}

	sampled := sched.DefaultPolicy(sched.DefaultWeights)
	sampled.SampleSize = 100
	policies := []*sched.Policy{
		sched.DefaultPolicy(sched.DefaultWeights),
		sampled,
		sched.SpreadPolicy(),
		sched.FirstFitPolicy(),
		sched.RandomPolicy(cfg.Seed),
	}
	var results []sim.Result
	for _, p := range policies {
		results = append(results, sim.Run(cfg, p)...)
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "POLICY\tSAMPLE\tSCENARIO\tPLACED\tFAILED\tDECISIONS/S\tNODES USED\tLOWER BOUND\tPACKING EFF\tEMPTY CPU")
	for _, r := range results {
		fmt.Fprintf(tw, "%s\t%d\t%s\t%d\t%d\t%.0f\t%d\t%d\t%.3f\t%.3f\n",
			r.Policy, r.SampleSize, r.Scenario, r.Placements, r.Failed, r.DecisionsPerSec,
			r.NodesUsed, r.LowerBound, r.PackingEfficiency, r.EmptyCPUFraction)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	notes := []string{
		"packing_efficiency is requested/capacity of the binding resource on nodes that hold at least one sandbox",
		fmt.Sprintf("the request mix is CPU-bound on this fleet (about %.1f GiB per requested CPU against %d GiB per node CPU)",
			sim.MixMemPerCPU(), sim.NodeMemPerCPU),
	}
	if *out == "" {
		return nil
	}
	doc := struct {
		Config  sim.Config   `json:"config"`
		Go      string       `json:"go"`
		GOOS    string       `json:"goos"`
		GOARCH  string       `json:"goarch"`
		CPUs    int          `json:"cpus"`
		Notes   []string     `json:"notes"`
		Results []sim.Result `json:"results"`
	}{cfg, runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), notes, results}
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		return err
	}
	return os.WriteFile(*out, append(b, '\n'), 0o644)
}
