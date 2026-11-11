package sched

import (
	"math/rand/v2"
	"sync"
)

// Policy is a filter chain plus weighted scorers. Place may be called from
// several goroutines; calls are serialised because the sampling offset and
// scratch buffers live in the Policy.
type Policy struct {
	Name       string
	Filters    []FilterPlugin
	Scorers    []Weighted
	SampleSize int

	firstFit bool // no scorers: earliest scanned feasible node wins
	mu       sync.Mutex
	offset   int
	cur      []float64
	slots    [3][]float64
	cand     [3]candidate
	rejects  [8]int
}

type candidate struct {
	n      *NodeInfo
	total  float64
	scores []float64
}

// Place picks the best feasible node. It does not allocate per node.
func (p *Policy) Place(req *Request, nodes []*NodeInfo) (*Decision, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	total := len(nodes)
	if total == 0 {
		return nil, &NoFitError{}
	}
	ns := len(p.Scorers)
	if cap(p.cur) < ns {
		p.cur = make([]float64, ns)
		for i := range p.slots {
			p.slots[i] = make([]float64, ns)
		}
	}
	p.cur = p.cur[:ns]
	for i := range p.slots {
		p.slots[i] = p.slots[i][:ns]
	}
	rejects := p.rejects[:0]
	if len(p.Filters) <= len(p.rejects) {
		rejects = p.rejects[:len(p.Filters)]
		for i := range rejects {
			rejects[i] = 0
		}
	} else {
		rejects = make([]int, len(p.Filters))
	}

	start := p.offset % total
	nTop, feasible, scanned := 0, 0, 0
scan:
	for i := 0; i < total; i++ {
		n := nodes[(start+i)%total]
		scanned++
		for fi, f := range p.Filters {
			if !f.Filter(req, n) {
				rejects[fi]++
				continue scan
			}
		}
		feasible++
		sum := 0.0
		for si, s := range p.Scorers {
			v := s.Weight * s.Plugin.Score(req, n)
			p.cur[si] = v
			sum += v
		}
		nTop = p.insert(nTop, n, sum)
		if p.SampleSize > 0 && feasible >= p.SampleSize {
			break
		}
	}
	if p.SampleSize > 0 {
		p.offset = (start + scanned) % total
	}

	if feasible == 0 {
		rej := make(map[string]int)
		for fi, f := range p.Filters {
			if rejects[fi] > 0 {
				rej[f.Name()] = rejects[fi]
			}
		}
		return nil, &NoFitError{Total: scanned, Rejected: rej}
	}

	d := &Decision{
		NodeID:   p.cand[0].n.ID,
		Total:    scanned,
		Feasible: feasible,
		Top:      make([]NodeScore, nTop),
	}
	for i := 0; i < nTop; i++ {
		c := p.cand[i]
		m := make(map[string]float64, ns)
		for si, s := range p.Scorers {
			m[s.Plugin.Name()] = c.scores[si]
		}
		d.Top[i] = NodeScore{NodeID: c.n.ID, Total: c.total, Plugins: m}
	}
	return d, nil
}

// insert places the node whose scores are in p.cur into the top-3 list.
// For first-fit every total ties and the earliest scanned node wins.
func (p *Policy) insert(nTop int, n *NodeInfo, total float64) int {
	pos := nTop
	for pos > 0 && p.better(total, n, &p.cand[pos-1]) {
		pos--
	}
	if pos >= len(p.cand) {
		return nTop
	}
	// Reuse the slice of the entry that falls off the end, or an unused slot.
	var buf []float64
	if nTop == len(p.cand) {
		buf = p.cand[nTop-1].scores
	} else {
		buf = p.slots[nTop]
	}
	for i := min(nTop, len(p.cand)-1); i > pos; i-- {
		p.cand[i] = p.cand[i-1]
	}
	copy(buf, p.cur)
	p.cand[pos] = candidate{n: n, total: total, scores: buf}
	if nTop < len(p.cand) {
		nTop++
	}
	return nTop
}

func (p *Policy) better(total float64, n *NodeInfo, o *candidate) bool {
	if total != o.total {
		return total > o.total
	}
	if p.firstFit {
		return false
	}
	return n.ID < o.n.ID
}

func baseFilters() []FilterPlugin {
	return []FilterPlugin{Excluded(), NodeReady(), NodeGroup(), Capacity()}
}

// Weights are the DefaultPolicy scorer weights.
type Weights struct{ BinPack, WarmPool, GroupAffinity, NodeHealth float64 }

// DefaultWeights follows spec 8.3.
var DefaultWeights = Weights{1, 1, 0.5, 1}

// DefaultPolicy packs, prefers warm sandboxes, group affinity and healthy
// nodes. A scorer with weight 0 stays in the list and contributes 0.
func DefaultPolicy(w Weights) *Policy {
	p := &Policy{Name: "default", Filters: baseFilters()}
	for _, s := range []Weighted{
		{BinPack(), w.BinPack},
		{WarmPool(), w.WarmPool},
		{GroupAffinity(), w.GroupAffinity},
		{NodeHealth(), w.NodeHealth},
	} {
		p.Scorers = append(p.Scorers, s)
	}
	return p
}

// SpreadPolicy picks the least allocated node.
func SpreadPolicy() *Policy {
	return &Policy{Name: "spread", Filters: baseFilters(),
		Scorers: []Weighted{{LeastAllocated(), 1}}}
}

// FirstFitPolicy picks the first feasible node in input order.
func FirstFitPolicy() *Policy {
	return &Policy{Name: "firstfit", Filters: baseFilters(), firstFit: true}
}

// RandomPolicy picks a pseudo-random feasible node, reproducible per seed.
func RandomPolicy(seed int64) *Policy {
	r := rand.New(rand.NewPCG(uint64(seed), 0x9e3779b97f4a7c15))
	return &Policy{Name: "random", Filters: baseFilters(),
		Scorers: []Weighted{{&randomScorer{r: r}, 1}}}
}
