package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bench"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bundle"
)

const notRun = "not run"

// Inputs are the parsed files under results/. A nil field is a file that does not exist.
type Inputs struct {
	Hardware       *Hardware
	Start          *bench.Run
	Throughput     *bench.Run
	Density        *bench.Run
	GroupLoss      *bench.Run
	GroupLossRetry *bench.Run
	Sim            *SimDoc
}

// Hardware is results/hardware.json as written by scripts/hardware.sh.
type Hardware struct {
	CapturedAt string  `json:"captured_at"`
	Host       *Host   `json:"host"`
	VM         *VMInfo `json:"vm"`
	Machine    *VMInfo `json:"machine"` // Linux with no VM
	Note       string  `json:"vm_note"`
}

type Host struct {
	Model            string  `json:"model_name"`
	Chip             string  `json:"chip"`
	PerformanceCores int     `json:"performance_cores"`
	EfficiencyCores  int     `json:"efficiency_cores"`
	MemoryGiB        float64 `json:"memory_gib"`
	OS               string  `json:"os"`
}

type VMInfo struct {
	Kernel         string `json:"kernel"`
	Distro         string `json:"distro"`
	VCPUs          int    `json:"vcpus"`
	MemoryMiB      int    `json:"memory_mib"`
	Runsc          string `json:"runsc"`
	Go             string `json:"go"`
	Virtualization string `json:"virtualization"`
	NodesAre       string `json:"nodes_are"`
}

// SimDoc is results/schedsim.json as written by cmd/schedsim.
type SimDoc struct {
	Command   string    `json:"command"`
	StartedAt time.Time `json:"started_at"`
	Config    struct {
		Nodes        int     `json:"nodes"`
		Groups       int     `json:"groups"`
		Seed         int64   `json:"seed"`
		FillFraction float64 `json:"fill_fraction"`
		ChurnSteps   int     `json:"churn_steps"`
	} `json:"config"`
	Go      string      `json:"go"`
	GOOS    string      `json:"goos"`
	GOARCH  string      `json:"goarch"`
	CPUs    int         `json:"cpus"`
	Notes   []string    `json:"notes"`
	Results []SimResult `json:"results"`
}

type SimResult struct {
	Policy            string  `json:"policy"`
	SampleSize        int     `json:"sample_size"`
	Scenario          string  `json:"scenario"`
	DecisionsPerSec   float64 `json:"decisions_per_sec"`
	NodesUsed         int     `json:"nodes_used"`
	LowerBound        int     `json:"lower_bound"`
	PackingEfficiency float64 `json:"packing_efficiency"`
	EmptyCPUFraction  float64 `json:"empty_cpu_fraction"`
}

func loadJSON[T any](dir, name string) (*T, error) {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Join(dir, name), err)
	}
	return &v, nil
}

// Load reads every results file in dir; a missing file leaves its field nil.
func Load(dir string) (*Inputs, error) {
	var in Inputs
	var err error
	if in.Hardware, err = loadJSON[Hardware](dir, "hardware.json"); err != nil {
		return nil, err
	}
	if in.Start, err = loadJSON[bench.Run](dir, "start.json"); err != nil {
		return nil, err
	}
	if in.Throughput, err = loadJSON[bench.Run](dir, "throughput.json"); err != nil {
		return nil, err
	}
	if in.Density, err = loadJSON[bench.Run](dir, "density.json"); err != nil {
		return nil, err
	}
	if in.GroupLoss, err = loadJSON[bench.Run](dir, "grouploss.json"); err != nil {
		return nil, err
	}
	if in.GroupLossRetry, err = loadJSON[bench.Run](dir, "grouploss-retry.json"); err != nil {
		return nil, err
	}
	if in.Sim, err = loadJSON[SimDoc](dir, "schedsim.json"); err != nil {
		return nil, err
	}
	return &in, nil
}

// Number formats: milliseconds one decimal under 100 and none above, rates one
// decimal under 10 and none above, fractions three decimals.

func fmtMS(v float64) string {
	if v < 99.95 {
		return fmt.Sprintf("%.1f", v)
	}
	return fmt.Sprintf("%.0f", v)
}

func fmtRate(v float64) string {
	if v < 9.95 {
		return fmt.Sprintf("%.1f", v)
	}
	return fmt.Sprintf("%.0f", v)
}

func fmtFrac(v float64) string { return fmt.Sprintf("%.3f", v) }
func fmtInt(v float64) string  { return fmt.Sprintf("%.0f", v) }

func fmtDate(t time.Time) string {
	if t.IsZero() {
		return "date not recorded"
	}
	return t.UTC().Format("2006-01-02")
}

func series(r *bench.Run, name string) (bench.Series, bool) {
	for _, s := range r.Series {
		if s.Name == name {
			return s, true
		}
	}
	return bench.Series{}, false
}

func rowNum(row map[string]any, key string) (float64, bool) {
	v, ok := row[key].(float64)
	return v, ok
}

// scalar formats a scalar, or "-" when the run did not record it.
func scalar(r *bench.Run, key string, f func(float64) string) string {
	v, ok := r.Scalars[key]
	if !ok {
		return "-"
	}
	return f(v)
}

// param formats a parameter, or "-" when the run did not record it.
func param(r *bench.Run, key string) string {
	v, ok := r.Params[key]
	if !ok || v == nil {
		return "-"
	}
	return fmt.Sprint(v)
}

// fmtEvent formats detection and recovery times; loadgen writes -1 when the event
// did not happen.
func fmtEvent(v float64, none string) string {
	if v < 0 {
		return none
	}
	return fmtMS(v)
}

func noSamples(r *bench.Run) string {
	return fmt.Sprintf("no successful run (%d errors)", r.Errors)
}

// sizeLine states the sandbox size the run recorded.
func sizeLine(r *bench.Run) string {
	cpu, mem := "-", "-"
	if v, ok := r.Params["cpu_millis"].(float64); ok {
		cpu = fmtInt(v) + " mCPU"
	}
	if v, ok := r.Params["memory_mib"].(float64); ok {
		mem = fmtInt(v) + " MiB"
	}
	return "Sandbox size: " + cpu + ", " + mem
}

func paramString(r *bench.Run, key string) string {
	s, _ := r.Params[key].(string)
	return s
}

func table(header []string, rows [][]string) string {
	var b strings.Builder
	b.WriteString("| " + strings.Join(header, " | ") + " |\n")
	b.WriteString("|" + strings.Repeat(" --- |", len(header)) + "\n")
	for _, r := range rows {
		b.WriteString("| " + strings.Join(r, " | ") + " |\n")
	}
	return b.String()
}

func bullets(items []string) string {
	var b strings.Builder
	for _, it := range items {
		b.WriteString("- " + it + "\n")
	}
	return b.String()
}

// hardwareLine is the one-line description printed under every table.
func (h *Hardware) line() string {
	if h == nil {
		return "hardware " + notRun + " (results/hardware.json missing)"
	}
	var parts []string
	if h.Host != nil {
		parts = append(parts, fmt.Sprintf("%s, %s, %d performance + %d efficiency cores, %s GiB, %s",
			h.Host.Model, h.Host.Chip, h.Host.PerformanceCores, h.Host.EfficiencyCores, fmtG(h.Host.MemoryGiB), h.Host.OS))
	}
	vm, label := h.VM, "VM"
	if vm == nil && h.Machine != nil {
		vm, label = h.Machine, "machine"
	}
	if vm != nil {
		parts = append(parts, fmt.Sprintf("%s: %d vCPUs, %d MiB, Linux %s, %s, %s, %s, virtualization %s",
			label, vm.VCPUs, vm.MemoryMiB, vm.Kernel, vm.Distro, vm.Runsc, vm.Go, vm.Virtualization))
	} else {
		note := h.Note
		if note == "" {
			note = "no note"
		}
		parts = append(parts, "VM: not captured ("+note+")")
	}
	return strings.Join(parts, "; ")
}

func fmtG(v float64) string {
	if v == float64(int64(v)) {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.1f", v)
}

func (h *Hardware) vcpus() (int, bool) {
	if h == nil {
		return 0, false
	}
	if h.VM != nil && h.VM.VCPUs > 0 {
		return h.VM.VCPUs, true
	}
	if h.Machine != nil && h.Machine.VCPUs > 0 {
		return h.Machine.VCPUs, true
	}
	return 0, false
}

func footer(hw *Hardware, cmds []string, dates []string, extra ...string) string {
	var items []string
	for i, c := range cmds {
		label := "Command"
		if len(cmds) > 1 {
			label = fmt.Sprintf("Command %d", i+1)
		}
		items = append(items, fmt.Sprintf("%s: `%s`", label, c))
	}
	items = append(items, extra...)
	items = append(items, "Date: "+strings.Join(dates, ", "), "Hardware: "+hw.line())
	return bullets(items)
}

func runFooter(hw *Hardware, r *bench.Run, extra ...string) string {
	extra = append([]string{sizeLine(r), fmt.Sprintf("Errors: %d", r.Errors)}, extra...)
	return footer(hw, []string{r.Command}, []string{fmtDate(r.StartedAt)}, extra...)
}

// Render returns the content of docs/results.md.
func Render(in *Inputs) string {
	var b strings.Builder
	b.WriteString("# Results\n\n")
	b.WriteString("Generated by `cmd/mkresults` from the JSON files under `results/`. Nothing on this page is typed by hand.\n\n")
	section := func(title string, body string) {
		b.WriteString("## " + title + "\n\n" + body + "\n")
	}
	section("Hardware", renderHardware(in.Hardware))
	section("Start latency", renderStart(in))
	section("Throughput", renderThroughput(in))
	section("Sandboxes per node", renderDensity(in))
	section("Placement simulation", renderSim(in))
	section("Local failure-domain test", renderGroupLoss(in))
	section("How to read these numbers", renderCaveats(in))
	return strings.TrimRight(b.String(), "\n") + "\n"
}

func renderHardware(h *Hardware) string {
	if h == nil {
		return notRun + "\n"
	}
	var b strings.Builder
	var rows [][]string
	if h.Host != nil {
		rows = append(rows,
			[]string{"Host model", h.Host.Model},
			[]string{"Host chip", h.Host.Chip},
			[]string{"Host cores", fmt.Sprintf("%d performance, %d efficiency", h.Host.PerformanceCores, h.Host.EfficiencyCores)},
			[]string{"Host memory", fmtG(h.Host.MemoryGiB) + " GiB"},
			[]string{"Host OS", h.Host.OS})
	}
	vm, label := h.VM, "VM"
	if vm == nil && h.Machine != nil {
		vm, label = h.Machine, "Machine"
	}
	if vm != nil {
		rows = append(rows,
			[]string{label + " kernel", vm.Kernel},
			[]string{label + " distribution", vm.Distro},
			[]string{label + " vCPUs", fmt.Sprint(vm.VCPUs)},
			[]string{label + " memory", fmt.Sprintf("%d MiB", vm.MemoryMiB)},
			[]string{"runsc", vm.Runsc},
			[]string{"Go", vm.Go},
			[]string{"Virtualization", vm.Virtualization},
			[]string{"Nodes are", vm.NodesAre})
	} else {
		note := h.Note
		if note == "" {
			note = "no note"
		}
		rows = append(rows, []string{"VM", "not captured: " + note})
	}
	b.WriteString(table([]string{"Item", "Value"}, rows))
	b.WriteString("\n")
	date := h.CapturedAt
	if t, err := time.Parse(time.RFC3339, h.CapturedAt); err == nil {
		date = fmtDate(t)
	}
	b.WriteString(footer(h, []string{"scripts/hardware.sh"}, []string{date}))
	return b.String()
}

func renderStart(in *Inputs) string {
	r := in.Start
	if r == nil {
		return notRun + "\n"
	}
	var rows [][]string
	for _, mode := range []string{"cold", "restore", "warm"} {
		ready, ok1 := series(r, mode+"_ready_ms")
		first, ok2 := series(r, mode+"_first_result_ms")
		if !ok1 || !ok2 {
			rows = append(rows, []string{mode, notRun, notRun, notRun, notRun, notRun})
			continue
		}
		if ready.Count == 0 || first.Count == 0 {
			rows = append(rows, []string{mode, noSamples(r), "-", "-", "-", "0"})
			continue
		}
		rows = append(rows, []string{mode, fmtMS(ready.P50), fmtMS(ready.P95), fmtMS(first.P50), fmtMS(first.P95), fmt.Sprint(ready.Count)})
	}
	return primedSentence(r) + " Times are in milliseconds.\n\n" +
		table([]string{"Start path", "Ready p50", "Ready p95", "First result p50", "First result p95", "n"}, rows) + "\n" +
		runFooter(in.Hardware, r, "Warm retries: "+scalar(r, "warm_retries", fmtInt))
}

// primedSentence describes the primed set from the run's params, or names the default.
func primedSentence(r *bench.Run) string {
	if r != nil {
		if p := paramString(r, "primed_set"); p != "" {
			return "Primed set: " + strings.TrimRight(p, ".") + "."
		}
	}
	return "Primed set (the node agent's default, `bundle.DefaultPrime`): " + bundle.DefaultPrime + "."
}

func renderThroughput(in *Inputs) string {
	r := in.Throughput
	if r == nil {
		return notRun + "\n"
	}
	rows := [][]string{
		{"Workers", param(r, "workers")},
		{"Duration", param(r, "duration")},
		{"Sandboxes per second", scalar(r, "sandboxes_per_sec", fmtRate)},
		{"Share started warm", scalar(r, "share_warm", fmtFrac)},
		{"Share started by restore", scalar(r, "share_restore", fmtFrac)},
		{"Share started cold", scalar(r, "share_cold", fmtFrac)},
	}
	for _, d := range []struct{ label, name string }{{"Ready p50 / p95 (ms)", "ready_ms"}, {"First result p50 / p95 (ms)", "first_result_ms"}} {
		if s, ok := series(r, d.name); ok {
			if s.Count == 0 {
				rows = append(rows, []string{d.label, noSamples(r)})
			} else {
				rows = append(rows, []string{d.label, fmtMS(s.P50) + " / " + fmtMS(s.P95)})
			}
		}
	}
	return table([]string{"Metric", "Value"}, rows) + "\n" + runFooter(in.Hardware, r)
}

func renderDensity(in *Inputs) string {
	r := in.Density
	if r == nil {
		return notRun + "\n"
	}
	opt := func(row map[string]any, key string, f func(float64) string) string {
		if v, ok := rowNum(row, key); ok {
			return f(v)
		}
		return "-"
	}
	// start_n and exec_n are the step's sample counts; a count of 0 means no sample
	// (bench.Summarize returns zeros). Rows without counts fall back to both
	// percentiles being exactly 0.
	lat := func(row map[string]any, key, pair, count string) string {
		v, ok := rowNum(row, key)
		if !ok {
			return "-"
		}
		if n, ok := rowNum(row, count); ok {
			if n == 0 {
				return "-"
			}
			return fmtMS(v)
		}
		if w, ok := rowNum(row, pair); v == 0 && ok && w == 0 {
			return "-"
		}
		return fmtMS(v)
	}
	var rows [][]string
	for _, row := range r.Rows {
		rows = append(rows, []string{
			opt(row, "step", fmtInt), opt(row, "live", fmtInt),
			lat(row, "start_p50_ms", "start_p95_ms", "start_n"), lat(row, "start_p95_ms", "start_p50_ms", "start_n"),
			lat(row, "exec_p50_ms", "exec_p95_ms", "exec_n"), lat(row, "exec_p95_ms", "exec_p50_ms", "exec_n"),
			opt(row, "failed", fmtInt), opt(row, "lost", fmtInt), opt(row, "unconfirmed", fmtInt),
			opt(row, "node_mem_available_mib", fmtInt),
		})
	}
	var b strings.Builder
	base, haveBase := r.Scalars["baseline_start_p95_ms"]
	if haveBase && base > 0 {
		fmt.Fprintf(&b, "Highest step up to which no step had a failure and the restore-start p95 stayed within 2x the first step's (%s ms): %s live sandboxes.", fmtMS(base), scalar(r, "max_live_within_2x", fmtInt))
	} else {
		b.WriteString("No baseline: the first step had no successful restore start, so no step counts as within 2x.")
	}
	if len(r.Notes) > 0 {
		b.WriteString(" What stopped the ramp: " + strings.TrimRight(r.Notes[0], ".") + ".")
	}
	b.WriteString("\n\n")
	b.WriteString(table([]string{"Step", "Live", "Start p50 (ms)", "Start p95 (ms)", "Exec p50 (ms)", "Exec p95 (ms)", "Failed", "Lost", "Unconfirmed", "Node MemAvailable (MiB)"}, rows))
	b.WriteString("\n" + runFooter(in.Hardware, r))
	return b.String()
}

func simLabel(r SimResult) string {
	switch {
	case r.SampleSize > 0:
		return fmt.Sprintf("%s, sample %d", r.Policy, r.SampleSize)
	case r.Policy == "default":
		return "default, no sampling"
	}
	return r.Policy
}

func renderSim(in *Inputs) string {
	s := in.Sim
	if s == nil {
		return notRun + "\n"
	}
	var rows [][]string
	for _, r := range s.Results {
		rows = append(rows, []string{simLabel(r), r.Scenario, fmtRate(r.DecisionsPerSec), fmtFrac(r.PackingEfficiency),
			fmtFrac(r.EmptyCPUFraction), fmt.Sprint(r.NodesUsed), fmt.Sprint(r.LowerBound)})
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d synthetic nodes in %d groups, seed %d, fill to %s of fleet CPU, then %d churn steps. Run with %s on %s/%s, %d CPUs.\n\n",
		s.Config.Nodes, s.Config.Groups, s.Config.Seed, fmtFrac(s.Config.FillFraction), s.Config.ChurnSteps, s.Go, s.GOOS, s.GOARCH, s.CPUs)
	b.WriteString(table([]string{"Policy", "Scenario", "Decisions/s", "Packing efficiency", "Empty CPU fraction", "Nodes used", "Lower bound"}, rows))
	b.WriteString("\n")
	cmd := s.Command
	if cmd == "" {
		cmd = "make sim"
	}
	b.WriteString(footer(in.Hardware, []string{cmd}, []string{fmtDate(s.StartedAt)}))
	if len(s.Notes) > 0 {
		b.WriteString("\nNotes from the run:\n\n" + bullets(s.Notes))
	}
	return b.String()
}

func renderGroupLoss(in *Inputs) string {
	runs := []*bench.Run{in.GroupLoss, in.GroupLossRetry}
	if runs[0] == nil && runs[1] == nil {
		return notRun + "\n"
	}
	var b strings.Builder
	b.WriteString("Local failure-domain test: both node groups are processes in one VM on one laptop. It says nothing about networks, regions or providers.\n\n")
	policy := ""
	for _, r := range runs {
		if r != nil {
			policy = paramString(r, "sched_policy")
			break
		}
	}
	switch policy {
	case "spread":
		b.WriteString("The scheduler ran with the spread policy (spec D35), which splits the sessions evenly between the groups on every run. Under the default policy the split depends on when each node's warm pool refills and when its heartbeat refreshes the scheduler's warm count, so the number of sessions the test loses would vary from run to run.\n\n")
	case "":
		b.WriteString("The run did not record the scheduler's placement policy; spec D35 requires the spread policy for this test.\n\n")
	default:
		fmt.Fprintf(&b, "The run recorded placement policy %q; spec D35 requires the spread policy for this test.\n\n", policy)
	}
	type row struct {
		label string
		f     func(map[string]float64) string
	}
	pres := func(k string, f func(float64) string) func(map[string]float64) string {
		return func(s map[string]float64) string {
			if v, ok := s[k]; ok {
				return f(v)
			}
			return "-"
		}
	}
	ms := func(k string) func(map[string]float64) string {
		return pres(k, fmtMS)
	}
	event := func(k, none string) func(map[string]float64) string {
		return pres(k, func(v float64) string { return fmtEvent(v, none) })
	}
	n := func(k string) func(map[string]float64) string {
		return pres(k, fmtInt)
	}
	defs := []row{
		{"Sessions", n("sessions")},
		{"Sessions affected", n("affected_sessions")},
		{"Sessions rescheduled", n("rescheduled")},
		{"Detection time (ms)", event("detection_ms", "not detected")},
		{"Recovery time (ms)", event("recovery_ms", "not recovered")},
		{"Requests", n("requests_total")},
		{"Requests failed after the kill", n("requests_failed")},
		{"Requests failed before the kill", n("requests_failed_before_kill")},
		{"Requests failed on unaffected sessions", n("requests_failed_unaffected")},
		{"Requests spanning the kill", n("requests_spanning_kill")},
		{"Longest single Exec try (ms)", ms("max_blocked_exec_ms")},
		{"Longest request including retries (ms)", ms("max_request_with_retries_ms")},
	}
	var rows [][]string
	for _, d := range defs {
		line := []string{d.label}
		for _, r := range runs {
			if r == nil {
				line = append(line, notRun)
			} else {
				line = append(line, d.f(r.Scalars))
			}
		}
		rows = append(rows, line)
	}
	b.WriteString(table([]string{"Metric", "Without retry", "With retry"}, rows))
	b.WriteString("\n")
	var cmds, dates, extra []string
	for i, r := range runs {
		name := []string{"without retry", "with retry"}[i]
		if r == nil {
			continue
		}
		extra = append(extra, fmt.Sprintf("Sandbox size and errors, %s: %s; errors: %d", name, strings.TrimPrefix(sizeLine(r), "Sandbox size: "), r.Errors))
		cmds = append(cmds, r.Command)
		dates = append(dates, name+" "+fmtDate(r.StartedAt))
	}
	b.WriteString(footer(in.Hardware, cmds, dates, extra...))
	return b.String()
}

func renderCaveats(in *Inputs) string {
	vcpus := "its vCPUs"
	if n, ok := in.Hardware.vcpus(); ok {
		vcpus = fmt.Sprintf("its %d vCPUs", n)
	}
	return bullets([]string{
		"One laptop. Every number comes from one machine.",
		"Nodes are processes in one VM and share " + vcpus + ", so they compete for CPU with each other and with the load generator.",
		"Node capacities are declared with flags, not measured.",
		"gVisor runs on the systrap platform only; no other platform was tried.",
		"Sandbox sizes are listed under each table; the smallest allowed size is 192 MiB (spec D31).",
		primedSentence(in.Start) + " The start-latency figures say nothing about other Python code or other packages.",
		"Observability was off during the runs (spec D25): no OTLP export and no observability container.",
		"A single run each. There are no repeats and no confidence intervals, so small differences between rows are not evidence.",
	})
}

// Summary returns the README block: a table of the headline numbers.
func Summary(in *Inputs) string {
	var rows [][]string
	add := func(metric, value, source string) { rows = append(rows, []string{metric, value, "`" + source + "`"}) }
	for _, mode := range []string{"cold", "restore", "warm"} {
		v := notRun
		if in.Start != nil {
			if s, ok := series(in.Start, mode+"_ready_ms"); ok {
				v = fmtMS(s.P50) + " / " + fmtMS(s.P95) + " ms"
				if s.Count == 0 {
					v = noSamples(in.Start)
				}
			}
		}
		add("Start latency, "+mode+" (ready p50 / p95)", v, "start.json")
	}
	v := notRun
	if r := in.Throughput; r != nil {
		v = fmt.Sprintf("%s sandboxes/s (%s workers, %s)", scalar(r, "sandboxes_per_sec", fmtRate), param(r, "workers"), param(r, "duration"))
	}
	add("Throughput (create, Exec, destroy)", v, "throughput.json")
	v = notRun
	if r := in.Density; r != nil {
		v = scalar(r, "max_live_within_2x", fmtInt) + " live sandboxes, with restore-start p95 staying within 2x of the first step's"
	}
	add("Sandboxes per node", v, "density.json")
	v = notRun
	if s := in.Sim; s != nil {
		for _, r := range s.Results {
			if r.Policy == "default" && r.SampleSize == 0 && r.Scenario == "fill" {
				v = fmtRate(r.DecisionsPerSec) + " decisions/s"
			}
		}
	}
	add("Placement, default policy, fill scenario", v, "schedsim.json")
	v = notRun
	if a, c := in.GroupLoss, in.GroupLossRetry; a != nil || c != nil {
		part := func(r *bench.Run, label string) string {
			if r == nil {
				return label + " " + notRun
			}
			rec, ok := r.Scalars["recovery_ms"]
			switch {
			case !ok:
				return "- " + label
			case rec < 0:
				return "not recovered " + label
			}
			return fmtMS(rec) + " ms " + label
		}
		v = part(a, "without retry") + ", " + part(c, "with retry")
	}
	add("Group-loss recovery (local failure-domain test)", v, "grouploss.json, grouploss-retry.json")
	out := table([]string{"Metric", "Value", "Source"}, rows) + "\nHardware: " + in.Hardware.line() + "."
	if in.empty() {
		// docs/results.md then holds nothing but "not run" and is not committed:
		// no link to it.
		return out + "\n"
	}
	return out + " Full tables, commands and caveats: [docs/results.md](docs/results.md).\n"
}

// empty reports whether no results file was found.
func (in *Inputs) empty() bool {
	return in.Hardware == nil && in.Start == nil && in.Throughput == nil && in.Density == nil &&
		in.GroupLoss == nil && in.GroupLossRetry == nil && in.Sim == nil
}

const (
	markBegin = "<!-- results:begin -->"
	markEnd   = "<!-- results:end -->"
)

// ReplaceMarkers returns readme with the text between the markers replaced by block.
// ok is false when the markers are missing or out of order.
func ReplaceMarkers(readme, block string) (string, bool) {
	i := strings.Index(readme, markBegin)
	if i < 0 {
		return readme, false
	}
	j := strings.Index(readme[i:], markEnd)
	if j < 0 {
		return readme, false
	}
	j += i
	return readme[:i+len(markBegin)] + "\n" + strings.TrimRight(block, "\n") + "\n" + readme[j:], true
}
