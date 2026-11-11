// Package sim runs the placement policy against synthetic fleets. It is the
// engine behind cmd/schedsim.
package sim

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/sched"
)

const gib = int64(1) << 30

// Config describes one simulation.
type Config struct {
	Nodes        int     `json:"nodes"`
	Groups       int     `json:"groups"`
	Seed         int64   `json:"seed"`
	FillFraction float64 `json:"fill_fraction"` // share of fleet CPU requested before churn
	ChurnSteps   int     `json:"churn_steps"`
}

// DefaultConfig is the configuration of the published results.
func DefaultConfig() Config {
	return Config{Nodes: 1000, Groups: 4, Seed: 42, FillFraction: 0.70, ChurnSteps: 50000}
}

// Result is one policy in one scenario.
type Result struct {
	Policy          string  `json:"policy"`
	SampleSize      int     `json:"sample_size"`
	Scenario        string  `json:"scenario"` // "fill" or "churn"
	Placements      int     `json:"placements"`
	Failed          int     `json:"failed"`
	Seconds         float64 `json:"seconds"` // time spent inside Place only
	DecisionsPerSec float64 `json:"decisions_per_sec"`
	NodesUsed       int     `json:"nodes_used"`
	LowerBound      int     `json:"lower_bound"`
	// PackingEfficiency is the utilisation of the binding resource on nodes in
	// use: max(CPUUtilUsed, MemUtilUsed), each requested / capacity summed
	// over nodes that hold at least one sandbox.
	PackingEfficiency float64 `json:"packing_efficiency"`
	CPUUtilUsed       float64 `json:"cpu_util_used_nodes"`
	MemUtilUsed       float64 `json:"mem_util_used_nodes"`
	// EmptyCPUFraction is the CPU capacity of nodes holding no sandbox over
	// fleet CPU capacity: the share of the fleet that could be switched off.
	EmptyCPUFraction float64 `json:"empty_cpu_fraction"`
}

// Fleet builds the synthetic nodes: 20% 8 CPU/32 GiB, 50% 16 CPU/64 GiB,
// 30% 32 CPU/128 GiB. Groups g0..gN-1 are assigned round-robin by index. Sizes
// are shuffled with the config seed so input order does not favour one size.
func Fleet(cfg Config) []*sched.NodeInfo {
	groups := max(cfg.Groups, 1)
	cpus := make([]int64, cfg.Nodes)
	for i := range cpus {
		switch {
		case i < cfg.Nodes*2/10:
			cpus[i] = 8
		case i < cfg.Nodes*7/10:
			cpus[i] = 16
		default:
			cpus[i] = 32
		}
	}
	r := rand.New(rand.NewSource(cfg.Seed ^ 0x5eed))
	r.Shuffle(len(cpus), func(i, j int) { cpus[i], cpus[j] = cpus[j], cpus[i] })
	nodes := make([]*sched.NodeInfo, cfg.Nodes)
	for i := range nodes {
		nodes[i] = &sched.NodeInfo{
			ID:           fmt.Sprintf("n%04d", i),
			Group:        fmt.Sprintf("g%d", i%groups),
			Ready:        true,
			Capacity:     sched.Resources{CPUMillis: cpus[i] * 1000, MemoryBytes: cpus[i] * NodeMemPerCPU * gib},
			MaxSandboxes: 1000,
			WarmReady:    1,
			Health:       1,
		}
	}
	return nodes
}

// LowerBound assumes the largest nodes are used first, so on a mixed fleet it
// is a floor on node count and not a fair yardstick for a utilisation-based
// policy.
//
// It is the fewest nodes, largest first, whose capacity covers both
// totals. Nodes are tried largest-by-CPU first and largest-by-memory first and
// the smaller count wins. It returns len(nodes) when the fleet cannot cover the
// demand.
func LowerBound(nodes []*sched.NodeInfo, cpu, mem int64) int {
	if cpu <= 0 && mem <= 0 {
		return 0
	}
	best := len(nodes)
	for _, byMem := range []bool{false, true} {
		order := append([]*sched.NodeInfo(nil), nodes...)
		sort.SliceStable(order, func(i, j int) bool {
			if byMem {
				return order[i].Capacity.MemoryBytes > order[j].Capacity.MemoryBytes
			}
			return order[i].Capacity.CPUMillis > order[j].Capacity.CPUMillis
		})
		var c, m int64
		for i, n := range order {
			c += n.Capacity.CPUMillis
			m += n.Capacity.MemoryBytes
			if c >= cpu && m >= mem {
				best = min(best, i+1)
				break
			}
		}
	}
	return best
}

var sizes = []struct {
	res    sched.Resources
	weight int // percent
}{
	{sched.Resources{CPUMillis: 250, MemoryBytes: 256 << 20}, 40},
	{sched.Resources{CPUMillis: 500, MemoryBytes: 512 << 20}, 30},
	{sched.Resources{CPUMillis: 1000, MemoryBytes: gib}, 20},
	{sched.Resources{CPUMillis: 2000, MemoryBytes: 4 * gib}, 10},
}

// MixMemPerCPU is the mean memory in GiB per requested CPU of the request mix.
func MixMemPerCPU() float64 {
	var cpu, mem float64
	for _, z := range sizes {
		cpu += float64(z.weight) * float64(z.res.CPUMillis) / 1000
		mem += float64(z.weight) * float64(z.res.MemoryBytes) / float64(gib)
	}
	return mem / cpu
}

// NodeMemPerCPU is the memory in GiB per CPU of every synthetic node.
const NodeMemPerCPU = 4

func drawSize(r *rand.Rand) sched.Resources {
	v := r.Intn(100)
	for _, s := range sizes {
		if v < s.weight {
			return s.res
		}
		v -= s.weight
	}
	return sizes[len(sizes)-1].res
}

type liveSandbox struct {
	node int
	res  sched.Resources
}

type state struct {
	nodes    []*sched.NodeInfo
	index    map[string]int
	live     []liveSandbox
	fill     Result
	churn    Result
	fillCPU  int64 // requested CPU at the end of fill
	fleetCPU int64
}

// Run runs the fill scenario and then the churn scenario against a fresh
// fleet and returns one Result for each. Pass a Policy that no other run uses.
func Run(cfg Config, p *sched.Policy) []Result {
	s := run(cfg, p)
	return []Result{s.fill, s.churn}
}

func run(cfg Config, p *sched.Policy) *state {
	nodes := Fleet(cfg)
	s := &state{nodes: nodes, index: make(map[string]int, len(nodes))}
	for i, n := range nodes {
		s.index[n.ID] = i
		s.fleetCPU += n.Capacity.CPUMillis
	}
	r := rand.New(rand.NewSource(cfg.Seed))

	// Fill: place until requested CPU reaches the target.
	target := int64(cfg.FillFraction * float64(s.fleetCPU))
	var requested int64
	var elapsed time.Duration
	res := Result{Policy: p.Name, SampleSize: p.SampleSize, Scenario: "fill"}
	for attempts := 0; requested < target && attempts < 10*len(nodes)+100000; attempts++ {
		req := drawSize(r)
		if s.place(p, req, &res, &elapsed) {
			requested += req.CPUMillis
		}
	}
	s.fillCPU = requested
	s.finish(&res, elapsed)
	s.fill = res

	// Churn: remove one random live sandbox, then place one new request.
	elapsed = 0
	res = Result{Policy: p.Name, SampleSize: p.SampleSize, Scenario: "churn"}
	for i := 0; i < cfg.ChurnSteps; i++ {
		if len(s.live) > 0 {
			k := r.Intn(len(s.live))
			sb := s.live[k]
			s.live[k] = s.live[len(s.live)-1]
			s.live = s.live[:len(s.live)-1]
			n := s.nodes[sb.node]
			n.Allocated.CPUMillis -= sb.res.CPUMillis
			n.Allocated.MemoryBytes -= sb.res.MemoryBytes
			n.Sandboxes--
		}
		s.place(p, drawSize(r), &res, &elapsed)
	}
	s.finish(&res, elapsed)
	s.churn = res
	return s
}

// place asks the policy for a node and applies the decision itself.
func (s *state) place(p *sched.Policy, res sched.Resources, out *Result, elapsed *time.Duration) bool {
	req := &sched.Request{Resources: res, WantsWarm: true}
	t0 := time.Now()
	d, err := p.Place(req, s.nodes)
	*elapsed += time.Since(t0)
	if err != nil {
		out.Failed++
		return false
	}
	idx := s.index[d.NodeID]
	n := s.nodes[idx]
	n.Allocated.CPUMillis += res.CPUMillis
	n.Allocated.MemoryBytes += res.MemoryBytes
	n.Sandboxes++
	s.live = append(s.live, liveSandbox{node: idx, res: res})
	out.Placements++
	return true
}

func (s *state) finish(out *Result, elapsed time.Duration) {
	out.Seconds = elapsed.Seconds()
	if out.Seconds > 0 {
		out.DecisionsPerSec = float64(out.Placements+out.Failed) / out.Seconds
	}
	var reqCPU, reqMem, capCPU, capMem, allocCPU, allocMem, emptyCPU, fleetCPU int64
	for _, sb := range s.live {
		reqCPU += sb.res.CPUMillis
		reqMem += sb.res.MemoryBytes
	}
	for _, n := range s.nodes {
		fleetCPU += n.Capacity.CPUMillis
		if n.Sandboxes == 0 {
			emptyCPU += n.Capacity.CPUMillis
			continue
		}
		out.NodesUsed++
		capCPU += n.Capacity.CPUMillis
		capMem += n.Capacity.MemoryBytes
		allocCPU += n.Allocated.CPUMillis
		allocMem += n.Allocated.MemoryBytes
	}
	out.LowerBound = LowerBound(s.nodes, reqCPU, reqMem)
	if out.NodesUsed > 0 {
		out.CPUUtilUsed = float64(allocCPU) / float64(capCPU)
		out.MemUtilUsed = float64(allocMem) / float64(capMem)
		out.PackingEfficiency = max(out.CPUUtilUsed, out.MemUtilUsed)
	}
	if fleetCPU > 0 {
		out.EmptyCPUFraction = float64(emptyCPU) / float64(fleetCPU)
	}
}
