// Package sched is the placement policy: a filter stage and a score stage in
// the kube-scheduler shape. It is pure: no I/O, no clocks, no goroutines.
package sched

import (
	"fmt"
	"sort"
	"strings"
)

// Resources is a CPU and memory amount.
type Resources struct{ CPUMillis, MemoryBytes int64 }

// NodeInfo is the scheduler's view of one node.
type NodeInfo struct {
	ID, Group           string
	Ready               bool
	Capacity, Allocated Resources
	MaxSandboxes        int
	Sandboxes           int
	WarmReady           int
	Health              float64 // 0..1
}

// Request describes one placement.
type Request struct {
	Resources       Resources
	NodeGroup       string // hard constraint, "" = any
	AffinityKey     string
	AffinityByGroup map[string]int  // live sandboxes with the same key, per node group
	WantsWarm       bool            // no egress and start mode AUTO or WARM
	Exclude         map[string]bool // node IDs to skip
}

// FilterPlugin rejects nodes that cannot take the request.
type FilterPlugin interface {
	Name() string
	Filter(req *Request, n *NodeInfo) bool
}

// ScorePlugin rates a feasible node from 0 to 100.
type ScorePlugin interface {
	Name() string
	Score(req *Request, n *NodeInfo) float64
}

// Weighted is a score plugin with its weight.
type Weighted struct {
	Plugin ScorePlugin
	Weight float64
}

// NodeScore is one candidate's total and its weighted per-plugin scores.
type NodeScore struct {
	NodeID  string
	Total   float64
	Plugins map[string]float64
}

// Decision is the result of a placement.
type Decision struct {
	NodeID   string
	Total    int // nodes considered
	Feasible int
	Top      []NodeScore // best 3, best first
}

// NoFitError reports that no node passed the filters.
type NoFitError struct {
	Total    int
	Rejected map[string]int // filter name -> nodes it rejected first
}

func (e *NoFitError) Error() string {
	if e.Total == 0 {
		return "no nodes registered"
	}
	names := make([]string, 0, len(e.Rejected))
	for name, c := range e.Rejected {
		if c > 0 {
			names = append(names, name)
		}
	}
	sort.Slice(names, func(i, j int) bool {
		if e.Rejected[names[i]] != e.Rejected[names[j]] {
			return e.Rejected[names[i]] > e.Rejected[names[j]]
		}
		return names[i] < names[j]
	})
	if len(names) == 0 {
		return "no node fits"
	}
	var b strings.Builder
	b.WriteString("no node fits: ")
	for i, name := range names {
		if i == 0 {
			fmt.Fprintf(&b, "%d rejected by %s", e.Rejected[name], name)
		} else {
			fmt.Fprintf(&b, ", %d by %s", e.Rejected[name], name)
		}
	}
	return b.String()
}
