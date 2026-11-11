package sched

import (
	"math"
	"testing"
)

const gib = int64(1 << 30)

func node(id, group string) *NodeInfo {
	return &NodeInfo{
		ID: id, Group: group, Ready: true,
		Capacity:     Resources{4000, 8 * gib},
		MaxSandboxes: 100, Health: 1,
	}
}

func req(cpu, mem int64) *Request { return &Request{Resources: Resources{cpu, mem}} }

func near(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCapacityFilter(t *testing.T) {
	f := Capacity()
	n := node("n", "a")
	n.Allocated = Resources{3000, 6 * gib}
	if !f.Filter(req(1000, 2*gib), n) {
		t.Error("exact fit rejected")
	}
	if f.Filter(req(1001, 2*gib), n) {
		t.Error("1 millicore over accepted")
	}
	if f.Filter(req(1000, 2*gib+1), n) {
		t.Error("1 byte over accepted")
	}
	n.Sandboxes = n.MaxSandboxes
	if f.Filter(req(1, 1), n) {
		t.Error("full sandbox count accepted")
	}
}

func TestNodeGroupFilter(t *testing.T) {
	f := NodeGroup()
	a, b := node("a1", "a"), node("b1", "b")
	if !f.Filter(&Request{}, a) || !f.Filter(&Request{}, b) {
		t.Error("empty group must pass all")
	}
	r := &Request{NodeGroup: "a"}
	if !f.Filter(r, a) || f.Filter(r, b) {
		t.Error("group a must pass only a")
	}
}

func TestNodeReadyFilter(t *testing.T) {
	n := node("n", "a")
	if !NodeReady().Filter(&Request{}, n) {
		t.Error("ready node rejected")
	}
	n.Ready = false
	if NodeReady().Filter(&Request{}, n) {
		t.Error("unready node accepted")
	}
}

func TestExcludedFilter(t *testing.T) {
	n := node("n1", "a")
	if !Excluded().Filter(&Request{}, n) {
		t.Error("nil exclude rejected node")
	}
	r := &Request{Exclude: map[string]bool{"n1": true}}
	if Excluded().Filter(r, n) {
		t.Error("excluded node accepted")
	}
	if !Excluded().Filter(r, node("n2", "a")) {
		t.Error("other node rejected")
	}
}

func TestBinPackScore(t *testing.T) {
	n := node("n", "a")
	n.Allocated = Resources{1000, 2 * gib}
	near(t, BinPack().Score(req(1000, 2*gib), n), 50)
}

func TestWarmPoolScore(t *testing.T) {
	n := node("n", "a")
	cases := []struct {
		wants bool
		warm  int
		want  float64
	}{{true, 1, 100}, {true, 0, 0}, {false, 1, 0}, {false, 0, 0}}
	for _, c := range cases {
		n.WarmReady = c.warm
		near(t, WarmPool().Score(&Request{WantsWarm: c.wants}, n), c.want)
	}
}

func TestGroupAffinityScore(t *testing.T) {
	r := &Request{AffinityKey: "k", AffinityByGroup: map[string]int{"a": 3, "b": 1}}
	s := GroupAffinity()
	near(t, s.Score(r, node("x", "a")), 75)
	near(t, s.Score(r, node("x", "b")), 25)
	near(t, s.Score(r, node("x", "c")), 0)
	r.AffinityKey = ""
	near(t, s.Score(r, node("x", "a")), 0)
	r = &Request{AffinityKey: "k", AffinityByGroup: map[string]int{}}
	near(t, s.Score(r, node("x", "a")), 0)
}

func TestNodeHealthScore(t *testing.T) {
	n := node("n", "a")
	n.Health = 0.4
	near(t, NodeHealth().Score(&Request{}, n), 40)
}
