package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite testdata/results.golden.md")

const fixtures = "testdata/fixtures"

func TestRenderGolden(t *testing.T) {
	in, err := Load(fixtures)
	if err != nil {
		t.Fatal(err)
	}
	got := Render(in)
	golden := filepath.Join("testdata", "results.golden.md")
	if *update {
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Errorf("rendered output differs from %s (run with -update after checking the diff)\n%s", golden, got)
	}
	for _, banned := range []string{"multi-cloud", "multi-region", "multi-zone", "multicloud"} {
		if strings.Contains(strings.ToLower(got), banned) {
			t.Errorf("output contains %q", banned)
		}
	}
	for _, want := range []string{
		"Local failure-domain test: both node groups are processes in one VM on one laptop. It says nothing about networks, regions or providers.",
		"spread policy",
		"Primed set (the node agent's default, `bundle.DefaultPrime`): json,",
		"Sandbox sizes are listed under each table",
		"192 MiB",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q", want)
		}
	}
}

func TestRenderMissingFiles(t *testing.T) {
	dir := t.TempDir()
	b, err := os.ReadFile(filepath.Join(fixtures, "hardware.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "hardware.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "docs", "results.md")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--results", dir, "--out", out, "--readme", filepath.Join(dir, "none.md")}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit code %d: %s", code, stderr.String())
	}
	md, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	sections := strings.Split(string(md), "\n## ")
	checked := 0
	for _, sec := range sections {
		name, _, _ := strings.Cut(sec, "\n")
		switch name {
		case "Start latency", "Throughput", "Sandboxes per node", "Placement simulation", "Local failure-domain test":
			checked++
			if !strings.Contains(sec, "not run") {
				t.Errorf("section %q does not say not run:\n%s", name, sec)
			}
		}
	}
	if checked != 5 {
		t.Errorf("found %d of the 5 benchmark sections", checked)
	}
	if _, err := os.Stat(filepath.Join(dir, "none.md")); err == nil {
		t.Error("a missing README was created")
	}
}

// TestSummaryLink: the README block links to docs/results.md only when some
// results file exists. With none, docs/results.md says nothing but "not run" and
// is not committed, so a link to it would be dead.
func TestSummaryLink(t *testing.T) {
	const link = "[docs/results.md](docs/results.md)"
	if s := Summary(&Inputs{}); strings.Contains(s, link) {
		t.Errorf("summary with no results files links to docs/results.md:\n%s", s)
	}
	in, err := Load(fixtures)
	if err != nil {
		t.Fatal(err)
	}
	if s := Summary(in); !strings.Contains(s, link) {
		t.Errorf("summary with results files lacks the link to docs/results.md:\n%s", s)
	}
	// One file is enough for the link: the full tables then have something in them.
	if s := Summary(&Inputs{Sim: in.Sim}); !strings.Contains(s, link) {
		t.Errorf("summary with only schedsim.json lacks the link to docs/results.md:\n%s", s)
	}
}

const readmeIn = "# Title\n\nintro\n\n<!-- results:begin -->\nold table\n<!-- results:end -->\n\nrest\n"

func TestReadmeMarkers(t *testing.T) {
	dir := t.TempDir()
	readme := filepath.Join(dir, "README.md")
	if err := os.WriteFile(readme, []byte(readmeIn), 0o644); err != nil {
		t.Fatal(err)
	}
	args := []string{"--results", fixtures, "--out", filepath.Join(dir, "results.md"), "--readme", readme}
	var so, se bytes.Buffer
	if code := run(args, &so, &se); code != 0 {
		t.Fatalf("exit %d: %s", code, se.String())
	}
	first, _ := os.ReadFile(readme)
	s := string(first)
	if strings.Contains(s, "old table") || !strings.Contains(s, "| Metric | Value | Source |") {
		t.Errorf("block not replaced:\n%s", s)
	}
	if !strings.HasPrefix(s, "# Title\n\nintro\n\n<!-- results:begin -->\n") || !strings.HasSuffix(s, "<!-- results:end -->\n\nrest\n") {
		t.Errorf("text outside the markers changed:\n%s", s)
	}
	if code := run(args, &so, &se); code != 0 {
		t.Fatalf("second run exit %d", code)
	}
	second, _ := os.ReadFile(readme)
	if string(second) != s {
		t.Error("second run changed the README")
	}

	// No markers: left alone.
	plain := filepath.Join(dir, "plain.md")
	if err := os.WriteFile(plain, []byte("no markers\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	args[5] = plain
	if code := run(args, &so, &se); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if b, _ := os.ReadFile(plain); string(b) != "no markers\n" {
		t.Errorf("README without markers changed: %q", b)
	}
}

func TestNoHandTypedNumbers(t *testing.T) {
	in, err := Load(fixtures)
	if err != nil {
		t.Fatal(err)
	}
	md := Render(in)
	_, rest, _ := strings.Cut(md, "\n## Start latency\n")
	section, _, _ := strings.Cut(rest, "\n## ")
	var table []string
	for _, l := range strings.Split(section, "\n") {
		if strings.HasPrefix(l, "|") {
			table = append(table, l)
		}
	}
	if len(table) != 5 { // header, separator, cold, restore, warm
		t.Fatalf("start-latency table has %d lines:\n%s", len(table), section)
	}
	raw, err := os.ReadFile(filepath.Join(fixtures, "start.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Series []struct {
			Name  string
			Count int
			P50   float64
			P95   float64
		}
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	get := func(name string) (c struct {
		Name  string
		Count int
		P50   float64
		P95   float64
	}) {
		for _, s := range doc.Series {
			if s.Name == name {
				return s
			}
		}
		t.Fatalf("series %s not in start.json", name)
		return
	}
	// Each cell must be the exact JSON field it is labelled with, formatted.
	for i, mode := range []string{"cold", "restore", "warm"} {
		ready, first := get(mode+"_ready_ms"), get(mode+"_first_result_ms")
		want := []string{mode, fmtMS(ready.P50), fmtMS(ready.P95), fmtMS(first.P50), fmtMS(first.P95), strconv.Itoa(ready.Count)}
		var cells []string
		for _, c := range strings.Split(strings.Trim(table[2+i], "| "), "|") {
			cells = append(cells, strings.TrimSpace(c))
		}
		if strings.Join(cells, "/") != strings.Join(want, "/") {
			t.Errorf("row %s: got %v, want %v", mode, cells, want)
		}
	}
}

func TestBadValues(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	zero := func(n string) string {
		return `{"name":"` + n + `","unit":"ms","count":0,"min":0,"mean":0,"p50":0,"p95":0,"p99":0,"max":0}`
	}
	write("start.json", `{"name":"start","command":"c","errors":7,"params":{},"scalars":{},"series":[`+
		zero("cold_ready_ms")+","+zero("cold_first_result_ms")+`]}`)
	write("throughput.json", `{"name":"throughput","command":"c","errors":3,"params":{},"scalars":{},"series":[`+zero("ready_ms")+`]}`)
	write("density.json", `{"name":"density","command":"c","errors":1,"params":{},"scalars":{},"series":[],
		"rows":[{"step":1,"live":5,"start_n":0,"start_p50_ms":0,"start_p95_ms":0,"exec_n":4,"exec_p50_ms":4.5,"exec_p95_ms":6.5,"failed":5},
		{"step":2,"live":6,"start_n":3,"start_p50_ms":0,"start_p95_ms":2.5,"exec_p50_ms":0,"exec_p95_ms":0,"failed":0}]}`)
	for _, f := range []string{"grouploss.json", "grouploss-retry.json"} {
		write(f, `{"name":"grouploss","command":"c","errors":0,"params":{"sched_policy":"spread"},"scalars":{"detection_ms":-1,"recovery_ms":-1}}`)
	}
	in, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	md := Render(in)
	if strings.Contains(md, "-1.0") || strings.Contains(md, "-1 ms") || strings.Contains(md, "<nil>") {
		t.Errorf("output shows a placeholder as a result:\n%s", md)
	}
	for _, want := range []string{"no successful run (7 errors)", "no successful run (3 errors)", "not detected", "not recovered", "| 1 | 5 | - | - | 4.5 | 6.5 | 5 |", "| 2 | 6 | 0.0 | 2.5 | - | - | 0 |", "Errors: 7"} {
		if !strings.Contains(md, want) {
			t.Errorf("output lacks %q:\n%s", want, md)
		}
	}
	sum := Summary(in)
	if strings.Contains(sum, "-1.0") || strings.Contains(sum, "-1 ms") || !strings.Contains(sum, "not recovered") || !strings.Contains(sum, "no successful run") {
		t.Errorf("summary:\n%s", sum)
	}
}

func TestFormats(t *testing.T) {
	for _, c := range []struct {
		got, want string
	}{
		{fmtMS(14.24), "14.2"}, {fmtMS(99.96), "100"}, {fmtMS(100.4), "100"}, {fmtMS(1820.6), "1821"},
		{fmtRate(512340.2), "512340"}, {fmtRate(31.07), "31"}, {fmtRate(10), "10"}, {fmtRate(9.96), "10"},
		{fmtRate(2.43), "2.4"}, {fmtRate(0), "0.0"}, {fmtFrac(0.9123), "0.912"},
	} {
		if c.got != c.want {
			t.Errorf("got %q want %q", c.got, c.want)
		}
	}
}

func TestHardwareWithoutVM(t *testing.T) {
	dir := t.TempDir()
	hw := `{"captured_at":"2026-11-12T12:38:02Z","host":{"model_name":"MacBook Pro","chip":"Apple M3 Pro","performance_cores":6,"efficiency_cores":6,"memory_gib":18,"os":"macOS 26.5.2"},"vm":null,"vm_note":"the Lima VM gss was not running, so the VM part was not captured"}`
	if err := os.WriteFile(filepath.Join(dir, "hardware.json"), []byte(hw), 0o644); err != nil {
		t.Fatal(err)
	}
	in, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	md := Render(in)
	if !strings.Contains(md, "VM: not captured (the Lima VM gss was not running") {
		t.Errorf("hardware line lacks the VM note:\n%s", md)
	}
}
