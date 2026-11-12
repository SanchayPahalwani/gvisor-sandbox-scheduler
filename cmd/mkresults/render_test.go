package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"regexp"
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
		"Primed set: numpy, pandas and the standard library",
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

// numbers collects every number in a JSON document.
func numbers(v any, out *[]float64) {
	switch x := v.(type) {
	case float64:
		*out = append(*out, x)
	case []any:
		for _, e := range x {
			numbers(e, out)
		}
	case map[string]any:
		for _, e := range x {
			numbers(e, out)
		}
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
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	var nums []float64
	numbers(doc, &nums)
	allowed := map[string]bool{}
	for _, n := range nums {
		allowed[fmtMS(n)] = true
		allowed[strconv.FormatFloat(n, 'f', -1, 64)] = true
	}
	tok := regexp.MustCompile(`\d+(?:\.\d+)?`)
	for _, line := range table[2:] {
		for _, m := range tok.FindAllString(line, -1) {
			if len(strings.ReplaceAll(m, ".", "")) >= 3 && !allowed[m] {
				t.Errorf("number %q in %q is not in start.json after formatting", m, line)
			}
		}
	}
}

func TestFormats(t *testing.T) {
	for _, c := range []struct {
		got, want string
	}{
		{fmtMS(14.24), "14.2"}, {fmtMS(99.96), "100"}, {fmtMS(100.4), "100"}, {fmtMS(1820.6), "1821"},
		{fmtRate(512340.2), "512340"}, {fmtFrac(0.9123), "0.912"},
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
