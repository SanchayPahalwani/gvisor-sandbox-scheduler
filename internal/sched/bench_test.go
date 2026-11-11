package sched

import (
	"strconv"
	"testing"
)

func BenchmarkPlace1000(b *testing.B) {
	nodes := make([]*NodeInfo, 1000)
	for i := range nodes {
		n := node("node-"+strconv.Itoa(i), "g"+strconv.Itoa(i%4))
		n.Allocated = Resources{int64(i%30) * 100, int64(i%30) * gib / 4}
		n.WarmReady = i % 3
		n.Health = 1 - float64(i%10)/20
		nodes[i] = n
	}
	r := &Request{
		Resources: Resources{250, gib / 2}, WantsWarm: true, AffinityKey: "k",
		AffinityByGroup: map[string]int{"g1": 3, "g2": 1},
	}
	p := DefaultPolicy(DefaultWeights)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Place(r, nodes); err != nil {
			b.Fatal(err)
		}
	}
}
