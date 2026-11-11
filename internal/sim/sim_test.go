package sim

import (
	"math"
	"testing"

	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/sched"
)

func TestFleetComposition(t *testing.T) {
	nodes := Fleet(DefaultConfig())
	if len(nodes) != 1000 {
		t.Fatalf("nodes = %d", len(nodes))
	}
	bySize := map[int64]int{}
	byGroup := map[string]int{}
	ids := map[string]bool{}
	for _, n := range nodes {
		bySize[n.Capacity.CPUMillis]++
		byGroup[n.Group]++
		ids[n.ID] = true
		if n.MaxSandboxes != 1000 || n.Health != 1 || n.WarmReady != 1 || !n.Ready {
			t.Fatalf("node defaults wrong: %+v", n)
		}
		if n.Capacity.MemoryBytes != n.Capacity.CPUMillis/1000*4*gib {
			t.Fatalf("memory %d for cpu %d", n.Capacity.MemoryBytes, n.Capacity.CPUMillis)
		}
	}
	if bySize[8000] != 200 || bySize[16000] != 500 || bySize[32000] != 300 {
		t.Fatalf("sizes = %v", bySize)
	}
	for _, g := range []string{"g0", "g1", "g2", "g3"} {
		if byGroup[g] != 250 {
			t.Fatalf("group %s = %d", g, byGroup[g])
		}
	}
	if len(ids) != 1000 {
		t.Fatalf("ids not unique: %d", len(ids))
	}
}

func TestLowerBound(t *testing.T) {
	nodes := []*sched.NodeInfo{
		{Capacity: sched.Resources{CPUMillis: 8000, MemoryBytes: 32 * gib}},
		{Capacity: sched.Resources{CPUMillis: 16000, MemoryBytes: 64 * gib}},
		{Capacity: sched.Resources{CPUMillis: 32000, MemoryBytes: 128 * gib}},
	}
	if got := LowerBound(nodes, 33000, gib); got != 2 {
		t.Fatalf("33 CPU -> %d, want 2", got)
	}
	if got := LowerBound(nodes, 0, 0); got != 0 {
		t.Fatalf("zero demand -> %d", got)
	}
	if got := LowerBound(nodes, 1000, 130*gib); got != 2 {
		t.Fatalf("memory-bound -> %d, want 2", got)
	}
}

func smallConfig() Config {
	c := DefaultConfig()
	c.Nodes = 200
	c.ChurnSteps = 2000
	return c
}

func TestRunDeterministic(t *testing.T) {
	cfg := smallConfig()
	a := Run(cfg, sched.DefaultPolicy(sched.DefaultWeights))
	b := Run(cfg, sched.DefaultPolicy(sched.DefaultWeights))
	if len(a) != 2 || len(b) != 2 {
		t.Fatalf("results = %d, %d", len(a), len(b))
	}
	for i := range a {
		x, y := a[i], b[i]
		x.Seconds, x.DecisionsPerSec, y.Seconds, y.DecisionsPerSec = 0, 0, 0, 0
		if x != y {
			t.Fatalf("run differs:\n%+v\n%+v", x, y)
		}
	}
	if a[0].Scenario != "fill" || a[1].Scenario != "churn" {
		t.Fatalf("scenarios %q %q", a[0].Scenario, a[1].Scenario)
	}
}

func TestBinPackBeatsSpread(t *testing.T) {
	cfg := smallConfig()
	def := Run(cfg, sched.DefaultPolicy(sched.DefaultWeights))[1]
	spr := Run(cfg, sched.SpreadPolicy())[1]
	if def.Scenario != "churn" || spr.Scenario != "churn" {
		t.Fatal("expected churn results")
	}
	if def.PackingEfficiency <= spr.PackingEfficiency {
		t.Fatalf("efficiency default %v <= spread %v", def.PackingEfficiency, spr.PackingEfficiency)
	}
	if def.EmptyCPUFraction <= spr.EmptyCPUFraction {
		t.Fatalf("empty fraction default %v <= spread %v", def.EmptyCPUFraction, spr.EmptyCPUFraction)
	}
	for _, r := range []Result{def, spr} {
		if r.PackingEfficiency <= 0 || r.PackingEfficiency > 1 {
			t.Fatalf("%s efficiency %v", r.Policy, r.PackingEfficiency)
		}
	}
}

func TestEmptyCPUFraction(t *testing.T) {
	var nodes []*sched.NodeInfo
	for i := 0; i < 4; i++ {
		nodes = append(nodes, &sched.NodeInfo{Capacity: sched.Resources{CPUMillis: 8000, MemoryBytes: 32 * gib}})
	}
	nodes[0].Sandboxes = 1
	nodes[0].Allocated = sched.Resources{CPUMillis: 4000, MemoryBytes: 4 * gib}
	s := &state{nodes: nodes, live: []liveSandbox{{node: 0, res: nodes[0].Allocated}}}
	var r Result
	s.finish(&r, 0)
	if r.EmptyCPUFraction != 0.75 {
		t.Fatalf("empty fraction = %v", r.EmptyCPUFraction)
	}
	if r.PackingEfficiency != 0.5 || r.NodesUsed != 1 {
		t.Fatalf("efficiency %v used %d", r.PackingEfficiency, r.NodesUsed)
	}
}

func TestFillReachesTarget(t *testing.T) {
	cfg := smallConfig()
	r := run(cfg, sched.DefaultPolicy(sched.DefaultWeights))
	if r.fill.Failed != 0 {
		t.Fatalf("failed = %d", r.fill.Failed)
	}
	var fleetCPU int64
	for _, n := range r.nodes {
		fleetCPU += n.Capacity.CPUMillis
	}
	ratio := float64(r.fillCPU) / float64(fleetCPU)
	if ratio < 0.70 || ratio > 0.71 {
		t.Fatalf("requested/fleet = %.4f", ratio)
	}
}

func TestAccountingConsistent(t *testing.T) {
	cfg := smallConfig()
	r := run(cfg, sched.DefaultPolicy(sched.DefaultWeights))
	var sum, live sched.Resources
	count := 0
	for _, n := range r.nodes {
		sum.CPUMillis += n.Allocated.CPUMillis
		sum.MemoryBytes += n.Allocated.MemoryBytes
		count += n.Sandboxes
		if n.Allocated.CPUMillis > n.Capacity.CPUMillis || n.Allocated.MemoryBytes > n.Capacity.MemoryBytes || n.Sandboxes > n.MaxSandboxes {
			t.Fatalf("node %s over capacity: %+v", n.ID, n)
		}
		if n.Allocated.CPUMillis < 0 || n.Sandboxes < 0 {
			t.Fatalf("node %s negative: %+v", n.ID, n)
		}
	}
	for _, s := range r.live {
		live.CPUMillis += s.res.CPUMillis
		live.MemoryBytes += s.res.MemoryBytes
	}
	if sum != live || count != len(r.live) {
		t.Fatalf("allocated %+v (%d) != live %+v (%d)", sum, count, live, len(r.live))
	}
	if math.IsNaN(r.churn.CPUUtilUsed) {
		t.Fatal("nan util")
	}
}
