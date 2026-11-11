package sched

import "math/rand/v2"

type binPackScorer struct{}

func (binPackScorer) Name() string { return "BinPack" }
func (binPackScorer) Score(req *Request, n *NodeInfo) float64 {
	var cpu, mem float64
	if n.Capacity.CPUMillis > 0 {
		cpu = float64(n.Allocated.CPUMillis+req.Resources.CPUMillis) / float64(n.Capacity.CPUMillis)
	}
	if n.Capacity.MemoryBytes > 0 {
		mem = float64(n.Allocated.MemoryBytes+req.Resources.MemoryBytes) / float64(n.Capacity.MemoryBytes)
	}
	return 100 * (cpu + mem) / 2
}

type warmPoolScorer struct{}

func (warmPoolScorer) Name() string { return "WarmPool" }
func (warmPoolScorer) Score(req *Request, n *NodeInfo) float64 {
	if req.WantsWarm && n.WarmReady > 0 {
		return 100
	}
	return 0
}

type groupAffinityScorer struct{}

func (groupAffinityScorer) Name() string { return "GroupAffinity" }
func (groupAffinityScorer) Score(req *Request, n *NodeInfo) float64 {
	if req.AffinityKey == "" {
		return 0
	}
	sum := 0
	for _, c := range req.AffinityByGroup {
		sum += c
	}
	if sum <= 0 {
		return 0
	}
	return 100 * float64(req.AffinityByGroup[n.Group]) / float64(sum)
}

type nodeHealthScorer struct{}

func (nodeHealthScorer) Name() string                          { return "NodeHealth" }
func (nodeHealthScorer) Score(_ *Request, n *NodeInfo) float64 { return 100 * n.Health }

type leastAllocatedScorer struct{}

func (leastAllocatedScorer) Name() string { return "LeastAllocated" }
func (leastAllocatedScorer) Score(req *Request, n *NodeInfo) float64 {
	return 100 - binPackScorer{}.Score(req, n)
}

// randomScorer is stateful; Policy.Place serialises calls to it.
type randomScorer struct{ r *rand.Rand }

func (*randomScorer) Name() string                        { return "Random" }
func (s *randomScorer) Score(*Request, *NodeInfo) float64 { return s.r.Float64() * 100 }

// BinPack scores fuller nodes higher.
func BinPack() ScorePlugin { return binPackScorer{} }

// WarmPool scores nodes holding a usable warm sandbox.
func WarmPool() ScorePlugin { return warmPoolScorer{} }

// GroupAffinity scores nodes in groups already running the affinity key.
func GroupAffinity() ScorePlugin { return groupAffinityScorer{} }

// NodeHealth scores by the node's health.
func NodeHealth() ScorePlugin { return nodeHealthScorer{} }

// LeastAllocated scores emptier nodes higher.
func LeastAllocated() ScorePlugin { return leastAllocatedScorer{} }
