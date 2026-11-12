package bench

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	hundred := make([]float64, 100)
	for i := range hundred {
		hundred[i] = float64(i + 1)
	}
	for _, tc := range []struct {
		p    float64
		want float64
	}{{50, 50}, {95, 95}, {99, 99}, {100, 100}, {1, 1}, {0, 1}} {
		if got := Percentile(hundred, tc.p); got != tc.want {
			t.Errorf("Percentile([1..100], %v) = %v, want %v", tc.p, got, tc.want)
		}
	}
	for _, p := range []float64{0, 50, 95, 99, 100} {
		if got := Percentile([]float64{5}, p); got != 5 {
			t.Errorf("Percentile([5], %v) = %v, want 5", p, got)
		}
	}
	if got := Percentile(nil, 50); got != 0 {
		t.Errorf("Percentile(empty, 50) = %v, want 0", got)
	}
	// Nearest rank on a size where p/100*n is not an integer: ceil(0.95*30) = 29.
	thirty := make([]float64, 30)
	for i := range thirty {
		thirty[i] = float64(i + 1)
	}
	if got := Percentile(thirty, 95); got != 29 {
		t.Errorf("Percentile([1..30], 95) = %v, want 29", got)
	}
}

func TestSummarize(t *testing.T) {
	in := []float64{30, 10, 20, 50, 40}
	orig := append([]float64(nil), in...)
	s := Summarize("x_ms", "ms", in)
	want := Series{Name: "x_ms", Unit: "ms", Count: 5, Min: 10, Mean: 30, P50: 30, P95: 50, P99: 50, Max: 50}
	if s != want {
		t.Errorf("Summarize = %+v, want %+v", s, want)
	}
	if !reflect.DeepEqual(in, orig) {
		t.Errorf("Summarize modified its input: %v, was %v", in, orig)
	}
	empty := Summarize("e", "ms", nil)
	if empty != (Series{Name: "e", Unit: "ms"}) {
		t.Errorf("Summarize(empty) = %+v, want zero values with name and unit", empty)
	}
}

func TestRunWrite(t *testing.T) {
	r := &Run{
		Name:      "start",
		Command:   "bin/loadgen start --n 3",
		StartedAt: time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC),
		Seconds:   12.5,
		Params:    map[string]any{"n": 3.0, "modes": "cold,warm"},
		Series:    []Series{Summarize("cold_ready_ms", "ms", []float64{1, 2, 3})},
		Scalars:   map[string]float64{"sandboxes_per_sec": 4.25},
		Rows:      []map[string]any{{"live": 5.0, "start_p95_ms": 12.0}},
		Errors:    1,
		Notes:     []string{"one note"},
	}
	path := filepath.Join(t.TempDir(), "nested", "dir", "start.json")
	if err := r.Write(path); err != nil {
		t.Fatalf("Write: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var back Run
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(&back, r) {
		t.Errorf("round trip mismatch:\n got %+v\nwant %+v", back, *r)
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	series := raw["series"].([]any)[0].(map[string]any)
	for _, k := range []string{"name", "unit", "count", "min", "mean", "p50", "p95", "p99", "max"} {
		if _, ok := series[k]; !ok {
			t.Errorf("series JSON has no %q key: %v", k, series)
		}
	}
	for _, k := range []string{"name", "command", "started_at", "seconds", "params", "series", "scalars", "rows", "errors", "notes"} {
		if _, ok := raw[k]; !ok {
			t.Errorf("run JSON has no %q key", k)
		}
	}
	if b[0] != '{' || b[1] != '\n' || b[len(b)-1] != '\n' {
		t.Errorf("want indented JSON ending in a newline, got %q...", b[:min(len(b), 20)])
	}
}
