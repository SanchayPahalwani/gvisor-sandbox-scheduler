package sched

import (
	"errors"
	"math"
	"reflect"
	"strconv"
	"testing"
)

func place(t *testing.T, p *Policy, r *Request, nodes ...*NodeInfo) *Decision {
	t.Helper()
	d, err := p.Place(r, nodes)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestPlacePacksFullerNode(t *testing.T) {
	n1, n2 := node("n1", "a"), node("n2", "a")
	n2.Allocated = Resources{2000, 4 * gib}
	d := place(t, DefaultPolicy(DefaultWeights), req(500, gib), n1, n2)
	if d.NodeID != "n2" {
		t.Fatalf("got %s", d.NodeID)
	}
}

func TestPlaceWarmBeatsPacking(t *testing.T) {
	warm, full := node("warm", "a"), node("full", "a")
	warm.WarmReady = 2
	full.Allocated = Resources{3600, 7 * gib}
	r := req(100, gib/10)
	r.WantsWarm = true
	d := place(t, DefaultPolicy(DefaultWeights), r, full, warm)
	if d.NodeID != "warm" {
		t.Fatalf("got %s", d.NodeID)
	}
}

func TestPlaceAllWarmFallsBackToPacking(t *testing.T) {
	n1, n2 := node("n1", "a"), node("n2", "a")
	n1.WarmReady, n2.WarmReady = 1, 1
	n2.Allocated = Resources{2000, 4 * gib}
	r := req(500, gib)
	r.WantsWarm = true
	if d := place(t, DefaultPolicy(DefaultWeights), r, n1, n2); d.NodeID != "n2" {
		t.Fatalf("got %s", d.NodeID)
	}
}

func TestPlaceUnhealthyNodeLoses(t *testing.T) {
	a, b := node("a", "g"), node("b", "g")
	a.Health = 0.3
	if d := place(t, DefaultPolicy(DefaultWeights), req(100, gib), a, b); d.NodeID != "b" {
		t.Fatalf("got %s", d.NodeID)
	}
}

func TestPlaceAffinity(t *testing.T) {
	a, b := node("a1", "a"), node("b1", "b")
	r := req(100, gib)
	r.AffinityKey = "k"
	r.AffinityByGroup = map[string]int{"b": 2}
	if d := place(t, DefaultPolicy(DefaultWeights), r, a, b); d.NodeID != "b1" {
		t.Fatalf("got %s", d.NodeID)
	}
}

func TestPlaceGroupConstraint(t *testing.T) {
	a, b := node("a1", "a"), node("b1", "b")
	a.Allocated = Resources{3000, 6 * gib} // packs better
	r := req(100, gib)
	r.NodeGroup = "b"
	if d := place(t, DefaultPolicy(DefaultWeights), r, a, b); d.NodeID != "b1" {
		t.Fatalf("got %s", d.NodeID)
	}
}

func TestPlaceTieBreak(t *testing.T) {
	if d := place(t, DefaultPolicy(DefaultWeights), req(100, gib), node("n2", "a"), node("n1", "a")); d.NodeID != "n1" {
		t.Fatalf("got %s", d.NodeID)
	}
}

func TestPlaceNoFit(t *testing.T) {
	s1, s2, wrong := node("s1", "a"), node("s2", "a"), node("w", "b")
	s1.Allocated = Resources{3900, 0}
	s2.Allocated = Resources{3900, 0}
	r := req(500, gib)
	r.NodeGroup = "a"
	_, err := DefaultPolicy(DefaultWeights).Place(r, []*NodeInfo{s1, s2, wrong})
	var nf *NoFitError
	if !errors.As(err, &nf) {
		t.Fatalf("err = %v", err)
	}
	if nf.Total != 3 || !reflect.DeepEqual(nf.Rejected, map[string]int{"Capacity": 2, "NodeGroup": 1}) {
		t.Fatalf("got %+v", nf)
	}
	if want := "no node fits: 2 rejected by Capacity, 1 by NodeGroup"; err.Error() != want {
		t.Fatalf("got %q", err.Error())
	}
}

func TestPlaceTooBigForAnyNode(t *testing.T) {
	_, err := DefaultPolicy(DefaultWeights).Place(req(8000, gib), []*NodeInfo{node("a", "g"), node("b", "g")})
	var nf *NoFitError
	if !errors.As(err, &nf) || nf.Rejected["Capacity"] != 2 || len(nf.Rejected) != 1 {
		t.Fatalf("err = %v", err)
	}
}

func TestPlaceEmptyCluster(t *testing.T) {
	_, err := DefaultPolicy(DefaultWeights).Place(req(1, 1), nil)
	var nf *NoFitError
	if !errors.As(err, &nf) || err.Error() != "no nodes registered" {
		t.Fatalf("err = %v", err)
	}
}

func TestPlaceExclude(t *testing.T) {
	r := req(100, gib)
	r.Exclude = map[string]bool{"n1": true}
	if d := place(t, DefaultPolicy(DefaultWeights), r, node("n1", "a"), node("n2", "a")); d.NodeID != "n2" {
		t.Fatalf("got %s", d.NodeID)
	}
	r.Exclude["n2"] = true
	_, err := DefaultPolicy(DefaultWeights).Place(r, []*NodeInfo{node("n1", "a"), node("n2", "a")})
	if err == nil || err.Error() != "no node fits: 2 rejected by Excluded" {
		t.Fatalf("err = %v", err)
	}
}

func TestDecisionTop(t *testing.T) {
	var nodes []*NodeInfo
	for i := 0; i < 5; i++ {
		n := node("n"+strconv.Itoa(i), "a")
		n.Allocated = Resources{int64(i) * 500, int64(i) * gib}
		nodes = append(nodes, n)
	}
	r := req(100, gib/4)
	r.AffinityKey = "k"
	r.AffinityByGroup = map[string]int{"a": 1, "b": 1} // nodes are in a: raw 50, weighted 25
	d := place(t, DefaultPolicy(DefaultWeights), r, nodes...)
	if d.Total != 5 || d.Feasible != 5 || len(d.Top) != 3 {
		t.Fatalf("got %+v", d)
	}
	if d.Top[0].NodeID != "n4" || d.Top[1].NodeID != "n3" || d.Top[2].NodeID != "n2" || d.NodeID != "n4" {
		t.Fatalf("order: %+v", d.Top)
	}
	for i, ns := range d.Top {
		if len(ns.Plugins) != 4 {
			t.Fatalf("plugins: %v", ns.Plugins)
		}
		for _, name := range []string{"BinPack", "WarmPool", "GroupAffinity", "NodeHealth"} {
			if _, ok := ns.Plugins[name]; !ok {
				t.Fatalf("missing plugin %s in %v", name, ns.Plugins)
			}
		}
		near(t, ns.Plugins["GroupAffinity"], 25)
		near(t, ns.Plugins["NodeHealth"], 100)
		sum := 0.0
		for _, v := range ns.Plugins {
			sum += v
		}
		near(t, sum, ns.Total)
		if i > 0 && ns.Total > d.Top[i-1].Total {
			t.Fatal("not sorted")
		}
	}
	d = place(t, DefaultPolicy(DefaultWeights), r, nodes[:2]...)
	if len(d.Top) != 2 {
		t.Fatalf("top = %d", len(d.Top))
	}
}

func TestSampling(t *testing.T) {
	var nodes []*NodeInfo
	for i := 0; i < 100; i++ {
		nodes = append(nodes, node("n"+strconv.Itoa(i), "a"))
	}
	p := DefaultPolicy(DefaultWeights)
	p.SampleSize = 10
	seen := map[string]bool{}
	firsts := map[string]bool{}
	for i := 0; i < 10; i++ {
		d := place(t, p, req(100, gib), nodes...)
		if d.Feasible != 10 {
			t.Fatalf("feasible = %d", d.Feasible)
		}
		firsts[d.NodeID] = true
		for _, ns := range d.Top {
			seen[ns.NodeID] = true
		}
	}
	if len(firsts) < 2 {
		t.Fatal("offset did not rotate")
	}
	if len(seen) < 10 {
		t.Fatalf("only %d distinct nodes in Top across calls", len(seen))
	}
	// Every node is scanned once in 10 calls: use first-fit so the winner is
	// the first node of each window and the windows tile the list.
	ff := FirstFitPolicy()
	ff.SampleSize = 10
	wins := map[string]bool{}
	for i := 0; i < 10; i++ {
		wins[place(t, ff, req(100, gib), nodes...).NodeID] = true
	}
	if len(wins) != 10 {
		t.Fatalf("windows = %d", len(wins))
	}
	for i := 0; i < 100; i += 10 {
		if !wins["n"+strconv.Itoa(i)] {
			t.Fatalf("window starting n%d never scanned", i)
		}
	}
}

func TestWeightsZeroDisablesPlugin(t *testing.T) {
	w := DefaultWeights
	w.WarmPool = 0
	p := DefaultPolicy(w)
	warm, full := node("warm", "a"), node("full", "a")
	warm.WarmReady = 2
	full.Allocated = Resources{3600, 7 * gib}
	r := req(100, gib/10)
	r.WantsWarm = true
	if d := place(t, p, r, warm, full); d.NodeID != "full" {
		t.Fatalf("got %s", d.NodeID)
	}
}

func TestFirstFitSpreadRandom(t *testing.T) {
	a, b := node("a", "g"), node("b", "g")
	b.Allocated = Resources{2000, 4 * gib}
	if d := place(t, FirstFitPolicy(), req(100, gib), b, a); d.NodeID != "b" {
		t.Fatalf("firstfit got %s", d.NodeID)
	}
	if d := place(t, SpreadPolicy(), req(100, gib), b, a); d.NodeID != "a" {
		t.Fatalf("spread got %s", d.NodeID)
	}
	var nodes []*NodeInfo
	for i := 0; i < 20; i++ {
		nodes = append(nodes, node("n"+strconv.Itoa(i), "g"))
	}
	run := func() []string {
		p := RandomPolicy(7)
		var out []string
		for i := 0; i < 10; i++ {
			out = append(out, place(t, p, req(1, 1), nodes...).NodeID)
		}
		return out
	}
	x, y := run(), run()
	if !reflect.DeepEqual(x, y) {
		t.Fatal("random policy not reproducible")
	}
}

func TestPlaceHugeRequestDoesNotOverflow(t *testing.T) {
	idle, busy := node("idle", "a"), node("busy", "a")
	busy.Allocated = Resources{1000, gib}
	for _, r := range []*Request{req(math.MaxInt64, gib), req(100, math.MaxInt64)} {
		_, err := DefaultPolicy(DefaultWeights).Place(r, []*NodeInfo{idle, busy})
		var nf *NoFitError
		if !errors.As(err, &nf) || nf.Rejected["Capacity"] != 2 {
			t.Fatalf("request %+v: err = %v", r.Resources, err)
		}
	}
}

func TestDefaultPolicyZeroWeightsTieBreaksByID(t *testing.T) {
	d := place(t, DefaultPolicy(Weights{}), req(100, gib), node("n2", "a"), node("n1", "a"))
	if d.NodeID != "n1" {
		t.Fatalf("got %s", d.NodeID)
	}
	if len(d.Top[0].Plugins) != 4 {
		t.Fatalf("plugins: %v", d.Top[0].Plugins)
	}
}
