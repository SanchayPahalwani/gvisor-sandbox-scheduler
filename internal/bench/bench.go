// Package bench holds the result format the load generator writes under results/
// and the statistics it reports.
package bench

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"
)

// Series summarizes one set of samples.
type Series struct {
	Name  string  `json:"name"`
	Unit  string  `json:"unit"` // "ms"
	Count int     `json:"count"`
	Min   float64 `json:"min"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

// Run is one benchmark run as written to a results file.
type Run struct {
	Name      string             `json:"name"`
	Command   string             `json:"command"` // the full command line that produced it
	StartedAt time.Time          `json:"started_at"`
	Seconds   float64            `json:"seconds"`
	Params    map[string]any     `json:"params"`
	Series    []Series           `json:"series"`
	Scalars   map[string]float64 `json:"scalars"`
	Rows      []map[string]any   `json:"rows,omitempty"` // per-step tables (density)
	Errors    int                `json:"errors"`
	Notes     []string           `json:"notes,omitempty"`
}

// Percentile returns the nearest-rank percentile p (0..100) of sorted, which must be
// in ascending order: sorted[ceil(p/100*n)-1], clamped to the slice. It returns 0 for
// an empty slice.
func Percentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	// p*n/100 is exact for integer p and n; the tolerance keeps a rounding error just
	// above an integer from moving the rank up by one.
	rank := int(math.Ceil(p*float64(n)/100 - 1e-9))
	return sorted[min(max(rank, 1), n)-1]
}

// Summarize returns the statistics of samples without modifying it.
func Summarize(name, unit string, samples []float64) Series {
	s := Series{Name: name, Unit: unit, Count: len(samples)}
	if len(samples) == 0 {
		return s
	}
	sorted := slices.Clone(samples)
	slices.Sort(sorted)
	sum := 0.0
	for _, v := range sorted {
		sum += v
	}
	s.Min, s.Max = sorted[0], sorted[len(sorted)-1]
	s.Mean = sum / float64(len(sorted))
	s.P50 = Percentile(sorted, 50)
	s.P95 = Percentile(sorted, 95)
	s.P99 = Percentile(sorted, 99)
	return s
}

// Write stores r as indented JSON at path, creating parent directories.
func (r *Run) Write(path string) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s results: %w", r.Name, err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}
