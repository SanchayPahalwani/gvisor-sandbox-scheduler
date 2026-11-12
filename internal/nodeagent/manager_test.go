package nodeagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bundle"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// fakeClock is Deps.Now for tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// fakeCgroups records calls such as "Remove /gss/n1/sb-1_1_".
type fakeCgroups struct {
	mu    sync.Mutex
	calls []string
}

func (c *fakeCgroups) record(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, s)
}

func (c *fakeCgroups) Calls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.calls)
}

func (c *fakeCgroups) count(s string) int {
	n := 0
	for _, call := range c.Calls() {
		if call == s {
			n++
		}
	}
	return n
}

func (c *fakeCgroups) SetLimits(path string, cpu, mem int64) error {
	c.record(fmt.Sprintf("SetLimits %s %d %d", path, cpu, mem))
	return nil
}
func (c *fakeCgroups) OOMKills(path string) (uint64, error) { return 0, nil }
func (c *fakeCgroups) Remove(path string) error             { c.record("Remove " + path); return nil }
func (c *fakeCgroups) KillTree(path string) error           { c.record("KillTree " + path); return nil }
func (c *fakeCgroups) Ensure(path string) error             { c.record("Ensure " + path); return nil }

// fakeNet is a NetPolicy that records Setup and Teardown.
type fakeNet struct {
	mu    sync.Mutex
	calls []string
}

func (n *fakeNet) Setup(ctx context.Context, id string, allow []string) (string, []bundle.HostEntry, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls = append(n.calls, fmt.Sprintf("Setup %s %s", id, strings.Join(allow, ",")))
	return "/var/run/netns/gss-" + id, []bundle.HostEntry{{IP: "10.9.9.9", Name: "api.example"}}, nil
}

func (n *fakeNet) Teardown(ctx context.Context, id string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls = append(n.calls, "Teardown "+id)
	return nil
}

func (n *fakeNet) Calls() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.calls)
}

var t0 = time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)

type env struct {
	m     *Manager
	rt    *runsc.Fake
	cg    *fakeCgroups
	clock *fakeClock
	state string
}

// newEnv returns a manager on node n1 with a fake runtime, fake cgroups and a
// fake clock, not yet started. mod may change the config and deps.
func newEnv(t *testing.T, mod func(*Config, *Deps)) *env {
	t.Helper()
	e := &env{rt: runsc.NewFake(), cg: &fakeCgroups{}, clock: &fakeClock{t: t0}, state: t.TempDir()}
	cfg := Config{
		NodeID: "n1", Group: "g1",
		CPUMillis: 4000, MemoryBytes: 8 << 30, MaxSandboxes: 16,
		RootFS: "/var/lib/gss/rootfs/python", StateDir: e.state,
	}
	deps := Deps{Runtime: e.rt, Cgroups: e.cg, Now: e.clock.Now}
	if mod != nil {
		mod(&cfg, &deps)
	}
	m, err := NewManager(cfg, deps)
	if err != nil {
		t.Fatal(err)
	}
	// Fake pids are not host processes; tests that check liveness replace this.
	m.processAlive = func(int) bool { return true }
	m.reapEvery, m.liveEvery = 10*time.Millisecond, 10*time.Millisecond
	m.poolRetry = 10 * time.Millisecond
	e.m = m
	return e
}

// start starts the manager and closes it at cleanup.
func (e *env) start(t *testing.T) *env {
	t.Helper()
	if err := e.m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := e.m.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return e
}

func startEnv(t *testing.T, mod func(*Config, *Deps)) *env {
	t.Helper()
	return newEnv(t, mod).start(t)
}

func spec(cpu, mem int64) *gssv1.SandboxSpec {
	return &gssv1.SandboxSpec{Resources: &gssv1.Resources{CpuMillis: cpu, MemoryBytes: mem}}
}

func (e *env) create(id string, inc int32, s *gssv1.SandboxSpec) (*gssv1.NodeCreateResponse, error) {
	return e.m.Create(context.Background(), &gssv1.NodeCreateRequest{SandboxId: id, Spec: s, Incarnation: inc})
}

func (e *env) mustCreate(t *testing.T, id string, inc int32, s *gssv1.SandboxSpec) *gssv1.NodeCreateResponse {
	t.Helper()
	resp, err := e.create(id, inc, s)
	if err != nil {
		t.Fatalf("Create %s/%d: %v", id, inc, err)
	}
	return resp
}

func (e *env) bundleDir(rid string) string { return filepath.Join(e.state, "bundles", rid) }

// execDir is where the Execs of sandbox rid keep their pid files.
func (e *env) execDir(rid string) string { return filepath.Join(e.state, "execs", rid) }

// listed returns "id/incarnation" for every listed sandbox.
func (e *env) listed() []string {
	var out []string
	for _, s := range e.m.List() {
		out = append(out, fmt.Sprintf("%s/%d", s.GetId(), s.GetIncarnation()))
	}
	return out
}

// inTable reports whether the manager still has an entry for id. An entry
// leaves List when its teardown starts and the table when it is done.
func (e *env) inTable(id string) bool {
	e.m.mu.Lock()
	defer e.m.mu.Unlock()
	return e.m.sandboxes[id] != nil
}

func wantCode(t *testing.T, err error, want codes.Code, what string) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("%s: code %v (err %v), want %v", what, got, err, want)
	}
}

// eventually polls cond for up to 2 s.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func TestCreateCold(t *testing.T) {
	e := startEnv(t, nil)
	var stdin string
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		b, err := io.ReadAll(s.Stdin)
		stdin = string(b)
		if !slices.Equal(s.Args, []string{"/gss/pyfork"}) {
			return 1, fmt.Errorf("args %q", s.Args)
		}
		return 0, err
	}
	before := len(e.rt.Calls())
	resp := e.mustCreate(t, "sb-1", 1, spec(500, 256<<20))

	rid := "sb-1_1_"
	want := []string{
		"Create id=" + rid + " net=none",
		"Start id=" + rid + " net=none",
		"Exec id=" + rid + " net=none",  // the readiness probe
		"State id=" + rid + " net=none", // the sandbox pid, for the liveness check
	}
	if got := e.rt.Calls()[before:]; !slices.Equal(got, want) {
		t.Errorf("runtime calls = %q, want %q", got, want)
	}
	if want := `{"code_len":4,"timeout_s":0,"env":{}}` + "\npass"; stdin != want {
		t.Errorf("probe stdin = %q, want %q", stdin, want)
	}
	if resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("started_as = %v, want COLD", resp.GetStartedAs())
	}
	if resp.GetReadyLatency() == nil {
		t.Error("ready_latency not set")
	}
	if got := e.listed(); !slices.Equal(got, []string{"sb-1/1"}) {
		t.Errorf("List = %q", got)
	}
	if l := e.m.List()[0]; l.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("listed started_as = %v", l.GetStartedAs())
	}
	if !exists(filepath.Join(e.bundleDir(rid), "config.json")) {
		t.Errorf("no bundle at %s", e.bundleDir(rid))
	}
	if e.rt.Containers()[rid] != "running" {
		t.Errorf("containers = %v", e.rt.Containers())
	}
}

func TestCreateDefaults(t *testing.T) {
	e := startEnv(t, nil)
	e.mustCreate(t, "sb-1", 1, &gssv1.SandboxSpec{})
	e.mustCreate(t, "sb-2", 1, nil)
	for _, l := range e.m.List() {
		s := l.GetSpec()
		if s.GetResources().GetCpuMillis() != 500 || s.GetResources().GetMemoryBytes() != 256<<20 ||
			s.GetTtl().AsDuration() != 10*time.Minute {
			t.Errorf("%s spec = %v, want 500m / 256MiB / 10m", l.GetId(), s)
		}
	}
	var cfg struct {
		Linux struct {
			Resources struct {
				Memory struct{ Limit int64 }
			}
		}
	}
	raw, err := os.ReadFile(filepath.Join(e.bundleDir("sb-1_1_"), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Linux.Resources.Memory.Limit != 256<<20 {
		t.Errorf("bundle memory limit = %d", cfg.Linux.Resources.Memory.Limit)
	}
}

func TestCreateInvalid(t *testing.T) {
	e := startEnv(t, nil)
	before := len(e.rt.Calls())
	_, err := e.create("sb-1", 1, spec(50, 256<<20))
	wantCode(t, err, codes.InvalidArgument, "cpu 50")
	if !strings.Contains(err.Error(), "cpu_millis") {
		t.Errorf("error %v does not name cpu_millis", err)
	}
	for _, id := range []string{"a_b", "a.b", "", "warm", "img", "-a", strings.Repeat("a", 41)} {
		_, err := e.create(id, 1, nil)
		wantCode(t, err, codes.InvalidArgument, fmt.Sprintf("id %q", id))
	}
	_, err = e.create("sb-1", -1, nil)
	wantCode(t, err, codes.InvalidArgument, "negative incarnation")
	_, err = e.create("sb-1", 1, &gssv1.SandboxSpec{StartMode: 9})
	wantCode(t, err, codes.InvalidArgument, "unknown start mode")

	if got := e.m.List(); len(got) != 0 {
		t.Errorf("List = %v", got)
	}
	if got := e.rt.Calls()[before:]; len(got) != 0 {
		t.Errorf("runtime calls = %q", got)
	}
	// The longest valid ID works.
	e.mustCreate(t, strings.Repeat("a", 40), 1, nil)
}

func TestRunscIDPrefixFree(t *testing.T) {
	ids := []string{runscID("sb-1", 1), runscID("sb-1", 10), runscID("sb-10", 1), runscID("sb-1", 0), runscID("sb", 1)}
	if ids[0] != "sb-1_1_" {
		t.Errorf("runscID(sb-1, 1) = %q", ids[0])
	}
	for i, a := range ids {
		for j, b := range ids {
			if i != j && strings.HasPrefix(b, a) {
				t.Errorf("%q is a prefix of %q", a, b)
			}
		}
		// The pool's and the image builder's IDs.
		for _, b := range []string{"warm_0123abcd_", "img_0123456789abcdef_42_"} {
			if strings.HasPrefix(a, b) || strings.HasPrefix(b, a) {
				t.Errorf("%q and %q are prefixes", a, b)
			}
		}
	}
}

func TestCreateOverCapacity(t *testing.T) {
	e := startEnv(t, func(c *Config, _ *Deps) { c.CPUMillis = 1000 })
	e.mustCreate(t, "sb-1", 1, spec(500, 256<<20))
	e.mustCreate(t, "sb-2", 1, spec(500, 256<<20))
	_, err := e.create("sb-3", 1, spec(500, 256<<20))
	wantCode(t, err, codes.ResourceExhausted, "third 500m")

	m := startEnv(t, func(c *Config, _ *Deps) { c.MemoryBytes = 512 << 20 })
	m.mustCreate(t, "sb-1", 1, spec(500, 384<<20))
	_, err = m.create("sb-2", 1, spec(500, 256<<20))
	wantCode(t, err, codes.ResourceExhausted, "memory over capacity")
	if got := m.listed(); !slices.Equal(got, []string{"sb-1/1"}) {
		t.Errorf("List = %q", got)
	}
}

func TestCreateOverMaxSandboxes(t *testing.T) {
	e := startEnv(t, func(c *Config, _ *Deps) { c.MaxSandboxes = 2 })
	e.mustCreate(t, "sb-1", 1, nil)
	e.mustCreate(t, "sb-2", 1, nil)
	_, err := e.create("sb-3", 1, nil)
	wantCode(t, err, codes.ResourceExhausted, "third sandbox")
	if err := e.m.Destroy(context.Background(), "sb-1", 0); err != nil {
		t.Fatal(err)
	}
	e.mustCreate(t, "sb-3", 1, nil)
}

func TestCreateDuplicate(t *testing.T) {
	e := startEnv(t, nil)
	e.mustCreate(t, "sb-1", 1, nil)
	_, err := e.create("sb-1", 1, nil)
	wantCode(t, err, codes.AlreadyExists, "same incarnation")

	e.mustCreate(t, "sb-2", 2, nil)
	_, err = e.create("sb-2", 1, nil)
	wantCode(t, err, codes.AlreadyExists, "lower incarnation")

	if got := e.listed(); !slices.Equal(got, []string{"sb-1/1", "sb-2/2"}) {
		t.Errorf("List = %q", got)
	}
	if n := len(e.rt.Containers()); n != 2 {
		t.Errorf("%d containers, want 2", n)
	}
}

func TestCreateReplacesOlderIncarnation(t *testing.T) {
	e := startEnv(t, nil)
	e.mustCreate(t, "sb-1", 1, nil)
	e.mustCreate(t, "sb-1", 2, nil)
	if !slices.Contains(e.rt.Calls(), "Delete id=sb-1_1_ net=none") {
		t.Errorf("old incarnation not deleted: %q", e.rt.Calls())
	}
	if got := e.listed(); !slices.Equal(got, []string{"sb-1/2"}) {
		t.Errorf("List = %q", got)
	}
	if got := e.rt.Containers(); len(got) != 1 || got["sb-1_2_"] != "running" {
		t.Errorf("containers = %v", got)
	}
	if exists(e.bundleDir("sb-1_1_")) {
		t.Error("old bundle dir left")
	}
	if e.cg.count("Remove /gss/n1/sb-1_1_") != 1 {
		t.Errorf("cgroup calls = %q", e.cg.Calls())
	}
}

func TestDestroyIncarnation(t *testing.T) {
	e := startEnv(t, nil)
	ctx := context.Background()
	e.mustCreate(t, "sb-1", 2, nil)
	if err := e.m.Destroy(ctx, "sb-1", 1); err != nil {
		t.Fatal(err)
	}
	if got := e.listed(); !slices.Equal(got, []string{"sb-1/2"}) {
		t.Fatalf("after Destroy(sb-1, 1): List = %q", got)
	}
	if err := e.m.Destroy(ctx, "sb-1", 2); err != nil {
		t.Fatal(err)
	}
	if got := e.listed(); len(got) != 0 {
		t.Fatalf("after Destroy(sb-1, 2): List = %q", got)
	}

	e.mustCreate(t, "sb-1", 2, nil)
	if err := e.m.Destroy(ctx, "sb-1", 0); err != nil {
		t.Fatal(err)
	}
	if got := e.listed(); len(got) != 0 {
		t.Fatalf("after Destroy(sb-1, 0): List = %q", got)
	}
	if got := e.rt.Containers(); len(got) != 0 {
		t.Errorf("containers = %v", got)
	}
}

// checkRolledBack asserts that nothing of sandbox rid is left and that its
// capacity is free again (the node has room for exactly one sandbox).
func checkRolledBack(t *testing.T, e *env, rid string) {
	t.Helper()
	if !slices.Contains(e.rt.Calls(), "Delete id="+rid+" net=none") {
		t.Errorf("Delete not called: %q", e.rt.Calls())
	}
	if len(e.rt.Containers()) != 0 {
		t.Errorf("containers = %v", e.rt.Containers())
	}
	if exists(e.bundleDir(rid)) {
		t.Errorf("bundle dir %s left", e.bundleDir(rid))
	}
	if e.cg.count("Remove "+bundle.CgroupPath("n1", rid)) != 1 {
		t.Errorf("cgroup calls = %q", e.cg.Calls())
	}
	if got := e.m.List(); len(got) != 0 {
		t.Errorf("List = %v", got)
	}
	e.mustCreate(t, "sb-2", 1, spec(500, 256<<20)) // capacity is free again
}

func TestCreateRollbackOnStartFailure(t *testing.T) {
	e := startEnv(t, func(c *Config, _ *Deps) { c.CPUMillis = 500 })
	e.rt.Fail["Start"] = errors.New("boom")
	_, err := e.create("sb-1", 1, spec(500, 256<<20))
	wantCode(t, err, codes.Internal, "start failure")
	if !strings.Contains(err.Error(), "runsc.start") || !strings.Contains(err.Error(), "boom") {
		t.Errorf("error %v does not name the step and cause", err)
	}
	checkRolledBack(t, e, "sb-1_1_")
}

func TestCreateRollbackOnNotReady(t *testing.T) {
	e := startEnv(t, func(c *Config, _ *Deps) { c.CPUMillis = 500 })
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		fmt.Fprint(s.Stderr, "pyfork: connect: no zygote")
		return 125, nil
	}
	_, err := e.create("sb-1", 1, spec(500, 256<<20))
	wantCode(t, err, codes.Internal, "not ready")
	if !strings.Contains(err.Error(), "ready") || !strings.Contains(err.Error(), "no zygote") {
		t.Errorf("error %v does not mention ready and the probe output", err)
	}
	e.rt.ExecFunc = nil
	checkRolledBack(t, e, "sb-1_1_")
}

func TestCreateReadyTimeout(t *testing.T) {
	e := startEnv(t, func(c *Config, _ *Deps) { c.ReadyTimeout = 50 * time.Millisecond })
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		<-ctx.Done()
		return -1, ctx.Err()
	}
	start := time.Now()
	_, err := e.create("sb-1", 1, nil)
	wantCode(t, err, codes.Internal, "probe timeout")
	if d := time.Since(start); d > time.Second {
		t.Errorf("Create took %v with a 50ms ReadyTimeout", d)
	}
	if len(e.rt.Containers()) != 0 || len(e.m.List()) != 0 {
		t.Errorf("not rolled back: containers %v", e.rt.Containers())
	}
}

func TestEgressWithoutNetPolicy(t *testing.T) {
	e := startEnv(t, nil)
	before := len(e.rt.Calls())
	_, err := e.create("sb-1", 1, &gssv1.SandboxSpec{EgressAllow: []string{"api.example:443"}})
	wantCode(t, err, codes.FailedPrecondition, "egress with nil Net")
	if got := e.rt.Calls()[before:]; len(got) != 0 {
		t.Errorf("runtime calls = %q", got)
	}
	if len(e.m.List()) != 0 {
		t.Error("sandbox listed")
	}
}

func TestCreateEgress(t *testing.T) {
	n := &fakeNet{}
	e := startEnv(t, func(_ *Config, d *Deps) { d.Net = n })
	e.mustCreate(t, "sb-1", 1, &gssv1.SandboxSpec{EgressAllow: []string{"api.example:443", "10.0.0.0/8:80"}})
	if !slices.Contains(e.rt.Calls(), "Create id=sb-1_1_ net=sandbox") {
		t.Errorf("runtime calls = %q", e.rt.Calls())
	}
	raw, err := os.ReadFile(filepath.Join(e.bundleDir("sb-1_1_"), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"path": "/var/run/netns/gss-sb-1_1_"`) {
		t.Error("bundle does not join the prepared namespace")
	}
	hosts, err := os.ReadFile(filepath.Join(e.bundleDir("sb-1_1_"), "hosts"))
	if err != nil || !strings.Contains(string(hosts), "10.9.9.9 api.example") {
		t.Errorf("hosts = %q, %v", hosts, err)
	}
	if err := e.m.Destroy(context.Background(), "sb-1", 0); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(e.rt.Calls(), "Delete id=sb-1_1_ net=sandbox") {
		t.Errorf("runtime calls = %q", e.rt.Calls())
	}
	want := []string{"Setup sb-1_1_ api.example:443,10.0.0.0/8:80", "Teardown sb-1_1_"}
	if got := n.Calls(); !slices.Equal(got, want) {
		t.Errorf("net calls = %q, want %q", got, want)
	}

	// A failed start tears the network down again.
	e.rt.Fail["Start"] = errors.New("boom")
	_, err = e.create("sb-2", 1, &gssv1.SandboxSpec{EgressAllow: []string{"api.example:443"}})
	wantCode(t, err, codes.Internal, "egress start failure")
	if got := n.Calls(); !slices.Contains(got, "Teardown sb-2_1_") {
		t.Errorf("net calls = %q", got)
	}
}

func TestDestroyIdempotent(t *testing.T) {
	e := startEnv(t, nil)
	ctx := context.Background()
	e.mustCreate(t, "sb-1", 1, nil)
	for i := 0; i < 2; i++ {
		if err := e.m.Destroy(ctx, "sb-1", 0); err != nil {
			t.Fatalf("Destroy #%d: %v", i+1, err)
		}
	}
	if err := e.m.Destroy(ctx, "nope", 0); err != nil {
		t.Fatalf("Destroy unknown: %v", err)
	}
	if n := e.cg.count("Remove /gss/n1/sb-1_1_"); n != 1 {
		t.Errorf("cgroup Remove called %d times: %q", n, e.cg.Calls())
	}
	// Processes runsc no longer tracks are killed before the cgroup is removed.
	var teardown []string
	for _, call := range e.cg.Calls() {
		if strings.HasPrefix(call, "KillTree /gss/n1/sb-1_1_") || strings.HasPrefix(call, "Remove /gss/n1/sb-1_1_") {
			teardown = append(teardown, call)
		}
	}
	if got, want := teardown, []string{"KillTree /gss/n1/sb-1_1_", "Remove /gss/n1/sb-1_1_"}; !slices.Equal(got, want) {
		t.Errorf("teardown cgroup calls = %q, want %q", got, want)
	}
	if exists(e.bundleDir("sb-1_1_")) {
		t.Error("bundle dir left")
	}
	if len(e.rt.Containers()) != 0 || len(e.m.List()) != 0 {
		t.Errorf("containers %v, list %v", e.rt.Containers(), e.m.List())
	}
}

// A Create of a newer incarnation can replace the entry a Destroy looked up
// before the Destroy gets to it. Destroy then looks again, and removes the
// new incarnation too when the request covers it.
func TestDestroyFindsReplacingIncarnation(t *testing.T) {
	for _, tc := range []struct {
		incarnation int32
		want        []string
	}{
		{0, nil},                // any incarnation
		{2, nil},                // up to 2
		{1, []string{"sb-1/2"}}, // only 1: the replacement stays
	} {
		e := newEnv(t, nil)
		replaced := false
		e.m.destroyLookedUp = func(s *sandbox) {
			if s.incarnation == 1 && !replaced {
				replaced = true
				e.mustCreate(t, "sb-1", 2, nil) // runs between the lookup and the teardown
			}
		}
		e.start(t)
		e.mustCreate(t, "sb-1", 1, nil)
		if err := e.m.Destroy(context.Background(), "sb-1", tc.incarnation); err != nil {
			t.Fatalf("Destroy(sb-1, %d): %v", tc.incarnation, err)
		}
		if !replaced {
			t.Fatal("the hook did not run")
		}
		if got := e.listed(); !slices.Equal(got, tc.want) {
			t.Errorf("Destroy(sb-1, %d): List = %q, want %q", tc.incarnation, got, tc.want)
		}
	}
}

func TestDestroyDuringCreate(t *testing.T) {
	e := startEnv(t, nil)
	e.rt.Delay["Start"] = 200 * time.Millisecond
	created := make(chan error, 1)
	go func() {
		_, err := e.create("sb-1", 1, nil)
		created <- err
	}()
	eventually(t, "Create to reach Start", func() bool {
		return slices.Contains(e.rt.Calls(), "Start id=sb-1_1_ net=none")
	})
	if err := e.m.Destroy(context.Background(), "sb-1", 0); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-created:
		if err != nil {
			t.Errorf("Create: %v", err)
		}
	default:
		t.Fatal("Destroy returned before the Create finished")
	}
	if got := e.m.List(); len(got) != 0 {
		t.Errorf("List = %v", got)
	}
	if len(e.rt.Containers()) != 0 {
		t.Errorf("containers = %v", e.rt.Containers())
	}
}

func TestDestroyFailureIsRetried(t *testing.T) {
	e := newEnv(t, nil)
	e.m.liveEvery = time.Hour // only Destroy retries in this test
	e.start(t)
	e.mustCreate(t, "sb-1", 1, nil)
	e.rt.Fail["Delete"] = errors.New("delete failed")
	err := e.m.Destroy(context.Background(), "sb-1", 0)
	wantCode(t, err, codes.Internal, "failed delete")
	if len(e.m.List()) != 0 {
		t.Error("a sandbox being destroyed is still listed")
	}
	if e.cg.count("Remove /gss/n1/sb-1_1_") != 0 || !exists(e.bundleDir("sb-1_1_")) {
		t.Error("host side removed although the container may still run")
	}
	if err := e.m.Destroy(context.Background(), "sb-1", 0); err != nil {
		t.Fatalf("second Destroy: %v", err)
	}
	if len(e.rt.Containers()) != 0 || exists(e.bundleDir("sb-1_1_")) || e.cg.count("Remove /gss/n1/sb-1_1_") != 1 {
		t.Errorf("not cleaned up: containers %v, cgroups %q", e.rt.Containers(), e.cg.Calls())
	}
}

func TestLivenessRetriesFailedDestroy(t *testing.T) {
	e := startEnv(t, nil)
	e.mustCreate(t, "sb-1", 1, nil)
	e.rt.Fail["Delete"] = errors.New("delete failed")
	if err := e.m.Destroy(context.Background(), "sb-1", 0); err == nil {
		t.Fatal("Destroy succeeded with a failing Delete")
	}
	eventually(t, "the liveness loop to finish the destroy", func() bool {
		return len(e.rt.Containers()) == 0 && e.cg.count("Remove /gss/n1/sb-1_1_") == 1
	})
}

func TestReaper(t *testing.T) {
	e := startEnv(t, nil)
	e.mustCreate(t, "sb-1", 1, &gssv1.SandboxSpec{Ttl: durationpb.New(time.Minute)})
	e.mustCreate(t, "sb-2", 1, &gssv1.SandboxSpec{Ttl: durationpb.New(2 * time.Minute)})

	e.clock.Set(t0.Add(69 * time.Second))
	e.m.reapExpired()
	time.Sleep(50 * time.Millisecond) // several reaper ticks
	if got := e.listed(); !slices.Equal(got, []string{"sb-1/1", "sb-2/1"}) {
		t.Fatalf("at +69s: List = %q", got)
	}
	e.clock.Set(t0.Add(71 * time.Second))
	eventually(t, "the reaper to destroy sb-1", func() bool { return !e.inTable("sb-1") })
	if got := e.listed(); !slices.Equal(got, []string{"sb-2/1"}) {
		t.Errorf("at +71s: List = %q", got)
	}
	if _, ok := e.rt.Containers()["sb-1_1_"]; ok {
		t.Error("sb-1 container not deleted")
	}
}

func TestLivenessRemovesDeadSandbox(t *testing.T) {
	e := newEnv(t, nil)
	var dead atomic.Int64 // pid reported dead
	var mu sync.Mutex
	checked := map[int]bool{}
	e.m.processAlive = func(pid int) bool {
		mu.Lock()
		checked[pid] = true
		mu.Unlock()
		return int64(pid) != dead.Load()
	}
	e.start(t)
	e.mustCreate(t, "sb-1", 1, nil)
	e.mustCreate(t, "sb-2", 1, nil)
	st, err := e.rt.State(context.Background(), runsc.Ref{ID: "sb-1_1_"})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "the liveness loop to check sb-1's pid", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return checked[st.PID]
	})
	dead.Store(int64(st.PID))
	eventually(t, "the dead sandbox to be removed", func() bool { return !e.inTable("sb-1") })
	if got := e.listed(); !slices.Equal(got, []string{"sb-2/1"}) {
		t.Errorf("List = %q", got)
	}
	if _, ok := e.rt.Containers()["sb-1_1_"]; ok || exists(e.bundleDir("sb-1_1_")) || e.cg.count("Remove /gss/n1/sb-1_1_") != 1 {
		t.Errorf("sb-1 not cleaned up: containers %v, cgroups %q", e.rt.Containers(), e.cg.Calls())
	}
}

func TestStartRemovesLeftovers(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	for _, id := range []string{"old1", "old2"} {
		if err := e.rt.Create(ctx, runsc.Ref{ID: id}, "/nowhere", "/dev/null"); err != nil {
			t.Fatal(err)
		}
	}
	stale := e.bundleDir("old1_3_")
	if err := os.MkdirAll(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "config.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	staleExec := e.execDir("old1_3_")
	if err := os.MkdirAll(staleExec, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staleExec, "exec-1.pid"), []byte("7"), 0o644); err != nil {
		t.Fatal(err)
	}
	e.start(t)
	if got := e.rt.Containers(); len(got) != 0 {
		t.Errorf("containers after Start = %v", got)
	}
	ents, err := os.ReadDir(filepath.Join(e.state, "bundles"))
	if err != nil || len(ents) != 0 {
		t.Errorf("bundles after Start: %v, %v", ents, err)
	}
	if ents, err := os.ReadDir(filepath.Join(e.state, "execs")); err != nil || len(ents) != 0 {
		t.Errorf("exec dirs after Start: %v, %v", ents, err)
	}
	// The node's cgroup exists before any runsc create: a cgroup directory
	// runsc creates is one it removes again when that container is deleted.
	if got, want := e.cg.Calls(), []string{"KillTree /gss/n1", "Ensure /gss/n1"}; !slices.Equal(got, want) {
		t.Errorf("cgroup calls = %q, want %q", got, want)
	}
}

func TestStartRequired(t *testing.T) {
	e := newEnv(t, nil)
	_, err := e.create("sb-1", 1, nil)
	wantCode(t, err, codes.Unavailable, "Create before Start")
	e.start(t)
	if err := e.m.Start(context.Background()); err == nil {
		t.Error("second Start succeeded")
	}
}

// recordSpans makes a span recorder the global tracer provider for the test.
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return sr
}

// lastSpan returns the last ended span called name, or nil.
func lastSpan(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	var out sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.Name() == name {
			out = s
		}
	}
	return out
}

// checkParent checks that the last span called child is a child of the last
// span called parent.
func checkParent(t *testing.T, spans []sdktrace.ReadOnlySpan, child, parent string) {
	t.Helper()
	c, p := lastSpan(spans, child), lastSpan(spans, parent)
	if c == nil || p == nil {
		t.Errorf("missing span %q or %q in %d spans", child, parent, len(spans))
		return
	}
	if c.Parent().SpanID() != p.SpanContext().SpanID() {
		t.Errorf("span %s is not a child of %s", child, parent)
	}
}

func TestChooseMode(t *testing.T) {
	const (
		auto    = gssv1.StartMode_START_MODE_AUTO
		cold    = gssv1.StartMode_START_MODE_COLD
		restore = gssv1.StartMode_START_MODE_RESTORE
		warm    = gssv1.StartMode_START_MODE_WARM
	)
	for _, tc := range []struct {
		requested  gssv1.StartMode
		egress     bool
		poolReady  int
		imageReady bool
		want       gssv1.StartMode
		wantErr    string // FailedPrecondition with this message
	}{
		{requested: auto, poolReady: 2, imageReady: true, want: warm},
		{requested: auto, egress: true, poolReady: 2, imageReady: true, want: restore},
		{requested: auto, imageReady: true, want: restore},
		{requested: auto, want: cold},
		{requested: auto, egress: true, poolReady: 2, want: cold},
		{requested: cold, egress: true, poolReady: 2, imageReady: true, want: cold},
		{requested: restore, imageReady: true, want: restore},
		{requested: restore, egress: true, poolReady: 2, imageReady: true, want: restore},
		{requested: restore, poolReady: 2, wantErr: "no checkpoint image"},
		{requested: warm, poolReady: 1, want: warm},
		{requested: warm, imageReady: true, wantErr: "warm pool empty"},
		{requested: warm, egress: true, poolReady: 2, imageReady: true, wantErr: "warm pool cannot serve egress"},
	} {
		name := fmt.Sprintf("%v egress=%v pool=%d image=%v", tc.requested, tc.egress, tc.poolReady, tc.imageReady)
		got, err := chooseMode(tc.requested, tc.egress, tc.poolReady, tc.imageReady)
		if tc.wantErr != "" {
			if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != tc.wantErr {
				t.Errorf("%s: %v, %v; want FailedPrecondition %q", name, got, err, tc.wantErr)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%s: %v, %v; want %v", name, got, err, tc.want)
		}
	}
	if _, err := chooseMode(9, false, 0, true); status.Code(err) != codes.InvalidArgument {
		t.Errorf("unknown mode: %v, want InvalidArgument", err)
	}
}

// restoreEnv returns a started env whose image is ready.
func restoreEnv(t *testing.T, mod func(*Config, *Deps)) (*env, *imageFake) {
	t.Helper()
	e, f := imageEnv(t, t.TempDir(), mod)
	e.start(t)
	if !e.m.ImageReady() {
		t.Fatal("Start did not make the image ready")
	}
	return e, f
}

func TestCreateRestore(t *testing.T) {
	sr := recordSpans(t)
	e, f := restoreEnv(t, nil)
	before, paths := len(e.rt.Calls()), len(f.Paths())
	resp := e.mustCreate(t, "sb-1", 1, spec(1000, 512<<20))

	rid := "sb-1_1_"
	want := []string{
		"Create id=" + rid + " net=none",
		"Restore id=" + rid + " net=none",
		"Exec id=" + rid + " net=none",  // the readiness probe
		"State id=" + rid + " net=none", // the sandbox pid
	}
	if got := e.rt.Calls()[before:]; !slices.Equal(got, want) {
		t.Errorf("runtime calls = %q, want %q", got, want)
	}
	image := filepath.Join(e.m.cfg.CheckpointDir, testImageKey())
	if got := f.Paths()[paths:]; !slices.Equal(got, []string{"Restore " + image}) {
		t.Errorf("image paths = %q, want a restore from %s", got, image)
	}
	if resp.GetStartedAs() != gssv1.StartMode_START_MODE_RESTORE || resp.GetReadyLatency() == nil {
		t.Errorf("response = %v, want RESTORE with ready_latency", resp)
	}
	if l := e.m.List(); len(l) != 1 || l[0].GetStartedAs() != gssv1.StartMode_START_MODE_RESTORE {
		t.Errorf("List = %v", l)
	}
	// The bundle has the request's limits, not the image's.
	raw, err := os.ReadFile(filepath.Join(e.bundleDir(rid), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"limit": 536870912`) || !strings.Contains(string(raw), `"quota": 100000`) {
		t.Errorf("bundle does not carry 1000m / 512 MiB:\n%s", raw)
	}
	spans := sr.Ended()
	for _, name := range []string{"runsc.create", "runsc.restore", "sandbox.ready"} {
		checkParent(t, spans, name, "node.create")
	}
	create := lastSpan(spans, "node.create")
	if create == nil {
		t.Fatal("no node.create span")
	}
	for _, s := range spans {
		if s.Name() == "runsc.start" && s.Parent().SpanID() == create.SpanContext().SpanID() {
			t.Error("the restore path ran runsc start")
		}
	}
	if !slices.Contains(create.Attributes(), attribute.String("start.mode", "restore")) {
		t.Errorf("node.create attributes = %v", create.Attributes())
	}
}

func TestCreateRestoreFallsBackToCold(t *testing.T) {
	e, _ := restoreEnv(t, func(c *Config, _ *Deps) { c.CPUMillis = 1000 })
	e.rt.Fail["Restore"] = errors.New("restore failed")
	before := len(e.rt.Calls())
	resp := e.mustCreate(t, "sb-1", 1, spec(500, 256<<20))
	if resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("started_as = %v, want COLD", resp.GetStartedAs())
	}
	rid := "sb-1_1_"
	want := []string{
		"Create id=" + rid + " net=none",
		"Restore id=" + rid + " net=none",
		"Delete id=" + rid + " net=none", // what the restore made goes first
		"Create id=" + rid + " net=none",
		"Start id=" + rid + " net=none",
		"Exec id=" + rid + " net=none",
		"State id=" + rid + " net=none",
	}
	if got := e.rt.Calls()[before:]; !slices.Equal(got, want) {
		t.Errorf("runtime calls = %q, want %q", got, want)
	}
	if n := e.cg.count("Remove " + bundle.CgroupPath("n1", rid)); n != 1 {
		t.Errorf("cgroup removed %d times before the cold start, want 1: %q", n, e.cg.Calls())
	}
	if l := e.m.List(); len(l) != 1 || l[0].GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("List = %v", l)
	}
	if e.rt.Containers()[rid] != "running" {
		t.Errorf("containers = %v", e.rt.Containers())
	}

	// The node drops the image and restores no more until it restarts, also
	// after the time a node without an image looks for one again.
	if e.m.ImageReady() {
		t.Error("ImageReady after a failed restore")
	}
	e.clock.Set(t0.Add(2 * imageRecheck))
	before = len(e.rt.Calls())
	if resp := e.mustCreate(t, "sb-2", 1, spec(500, 256<<20)); resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("after the failed restore: started_as = %v, want COLD", resp.GetStartedAs())
	}
	if got := e.rt.Calls()[before:]; slices.Contains(got, "Restore id=sb-2_1_ net=none") {
		t.Errorf("restored after the image was dropped: %q", got)
	}
	if _, err := e.m.EnsureImage(context.Background()); err == nil {
		t.Error("EnsureImage succeeded after the image was dropped")
	}
	// sb-1 counts once: the node (1000m) is full.
	_, err := e.create("sb-3", 1, spec(500, 256<<20))
	wantCode(t, err, codes.ResourceExhausted, "third 500m sandbox")
	_, err = e.create("sb-3", 1, &gssv1.SandboxSpec{StartMode: gssv1.StartMode_START_MODE_RESTORE})
	wantCode(t, err, codes.FailedPrecondition, "forced RESTORE after the image was dropped")
}

// notReadyAfterRestore makes the first readiness probe of each sandbox whose
// runsc ID is in fail (the probe after its restore) fail; the cold probe
// after the fallback passes.
func notReadyAfterRestore(e *env, fail ...string) {
	var mu sync.Mutex
	probed := map[string]bool{}
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		if slices.Contains(fail, ref.ID) && !probed[ref.ID] {
			probed[ref.ID] = true
			return 1, nil // the restored zygote does not answer
		}
		return 0, nil
	}
}

// One restored sandbox that does not get ready falls back to cold but
// keeps the image: a slow probe under load is not a bad image.
func TestRestoreNotReadyFallsBack(t *testing.T) {
	e, _ := restoreEnv(t, nil)
	notReadyAfterRestore(e, "sb-1_1_")
	if resp := e.mustCreate(t, "sb-1", 1, nil); resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("not ready after restore: started_as = %v, want COLD", resp.GetStartedAs())
	}
	if !e.m.ImageReady() {
		t.Error("one readiness failure after a restore dropped the image")
	}
	if resp := e.mustCreate(t, "sb-2", 1, nil); resp.GetStartedAs() != gssv1.StartMode_START_MODE_RESTORE {
		t.Errorf("next create: started_as = %v, want RESTORE", resp.GetStartedAs())
	}
}

// Readiness failures after a restore drop the image only when
// readyFailuresToDrop of them come in a row; a good restore resets the count.
func TestRestoreNotReadyInARowDropsImage(t *testing.T) {
	if readyFailuresToDrop != 3 {
		t.Errorf("readyFailuresToDrop = %d, want 3", readyFailuresToDrop)
	}
	e, _ := restoreEnv(t, nil)
	// fail, fail, good, fail, fail: never three in a row.
	notReadyAfterRestore(e, "a1_1_", "a2_1_", "a4_1_", "a5_1_")
	for i, id := range []string{"a1", "a2", "a3", "a4", "a5"} {
		want := gssv1.StartMode_START_MODE_COLD
		if id == "a3" {
			want = gssv1.StartMode_START_MODE_RESTORE
		}
		if resp := e.mustCreate(t, id, 1, nil); resp.GetStartedAs() != want {
			t.Errorf("create %d (%s): started_as = %v, want %v", i+1, id, resp.GetStartedAs(), want)
		}
		if !e.m.ImageReady() {
			t.Fatalf("image dropped after create %d (%s); at most two failures were in a row", i+1, id)
		}
	}
	// The third in a row drops it; the request itself still starts cold.
	notReadyAfterRestore(e, "a6_1_")
	if resp := e.mustCreate(t, "a6", 1, nil); resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("third failure in a row: started_as = %v, want COLD", resp.GetStartedAs())
	}
	if e.m.ImageReady() {
		t.Error("ImageReady after three readiness failures in a row")
	}
	before := len(e.rt.Calls())
	if resp := e.mustCreate(t, "a7", 1, nil); resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("after the drop: started_as = %v, want COLD", resp.GetStartedAs())
	}
	if got := e.rt.Calls()[before:]; slices.Contains(got, "Restore id=a7_1_ net=none") {
		t.Errorf("restored after the image was dropped: %q", got)
	}
}

// The readiness probe of a restored sandbox is bounded by
// restoreReadyTimeout (5 s), not by the cold path's ReadyTimeout.
func TestRestoreReadyBound(t *testing.T) {
	if got := newEnv(t, nil).m.restoreReadyTimeout; got != 5*time.Second {
		t.Errorf("restoreReadyTimeout = %v, want 5s", got)
	}
	e, _ := restoreEnv(t, nil)
	e.m.restoreReadyTimeout = 50 * time.Millisecond
	first := true
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		if ref.ID == "sb-1_1_" && first {
			first = false
			<-ctx.Done() // the restored zygote never answers
			return -1, ctx.Err()
		}
		return 0, nil
	}
	begin := time.Now()
	resp := e.mustCreate(t, "sb-1", 1, nil)
	if d := time.Since(begin); d > 2*time.Second {
		t.Errorf("Create took %v; the restored probe should end after 50ms (ReadyTimeout is %v)", d, e.m.cfg.ReadyTimeout)
	}
	if resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("started_as = %v, want COLD", resp.GetStartedAs())
	}
	if !e.m.ImageReady() {
		t.Error("one slow restored probe dropped the image")
	}
}

// A failure in a step the cold path shares neither falls back nor drops the
// image; nor does a restore that ends because the caller went away.
func TestRestoreFailureKeepsImage(t *testing.T) {
	e, _ := restoreEnv(t, nil)
	e.rt.Fail["Create"] = errors.New("create failed")
	before := len(e.rt.Calls())
	_, err := e.create("sb-1", 1, nil)
	wantCode(t, err, codes.Internal, "create failure on the restore path")
	if got, want := e.rt.Calls()[before:], []string{"Create id=sb-1_1_ net=none", "Delete id=sb-1_1_ net=none"}; !slices.Equal(got, want) {
		t.Errorf("runtime calls = %q, want %q", got, want)
	}
	if !e.m.ImageReady() {
		t.Error("a runsc create failure dropped the image")
	}

	e.rt.Delay["Restore"] = 5 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && !slices.Contains(e.rt.Calls(), "Restore id=sb-2_1_ net=none") {
			time.Sleep(5 * time.Millisecond)
		}
	}()
	before = len(e.rt.Calls())
	if _, err := e.m.Create(ctx, &gssv1.NodeCreateRequest{SandboxId: "sb-2", Incarnation: 1}); err == nil {
		t.Fatal("cancelled Create succeeded")
	}
	if got := e.rt.Calls()[before:]; slices.Contains(got, "Start id=sb-2_1_ net=none") {
		t.Errorf("a cancelled Create fell back to cold: %q", got)
	}
	if !e.m.ImageReady() {
		t.Error("a cancelled restore dropped the image")
	}
	delete(e.rt.Delay, "Restore")
	if resp := e.mustCreate(t, "sb-3", 1, nil); resp.GetStartedAs() != gssv1.StartMode_START_MODE_RESTORE {
		t.Errorf("started_as = %v, want RESTORE", resp.GetStartedAs())
	}
}

func TestForcedRestoreFailure(t *testing.T) {
	e, _ := restoreEnv(t, nil)
	e.rt.Fail["Restore"] = errors.New("restore failed")
	before := len(e.rt.Calls())
	_, err := e.create("sb-1", 1, &gssv1.SandboxSpec{StartMode: gssv1.StartMode_START_MODE_RESTORE})
	wantCode(t, err, codes.Internal, "forced RESTORE")
	if !strings.Contains(err.Error(), "runsc.restore") || !strings.Contains(err.Error(), "restore failed") {
		t.Errorf("error %v does not name the step and cause", err)
	}
	if got := e.rt.Calls()[before:]; slices.Contains(got, "Start id=sb-1_1_ net=none") {
		t.Errorf("forced RESTORE started cold: %q", got)
	}
	if _, ok := e.rt.Containers()["sb-1_1_"]; ok || exists(e.bundleDir("sb-1_1_")) || e.inTable("sb-1") {
		t.Errorf("sb-1 not rolled back: containers %v", e.rt.Containers())
	}
	if e.m.ImageReady() {
		t.Error("ImageReady after a failed forced restore")
	}
}

// stuckDelete fails every Delete of one container while stuck is set.
type stuckDelete struct {
	runsc.Runtime
	id    string
	stuck atomic.Bool
}

func (s *stuckDelete) Delete(ctx context.Context, ref runsc.Ref) error {
	if s.stuck.Load() && ref.ID == s.id {
		return errors.New("delete failed")
	}
	return s.Runtime.Delete(ctx, ref)
}

// When the cleanup before the cold start fails, the Create fails and the
// entry stays broken, holding its capacity until a retry removes it.
func TestRestoreFallbackCleanupFails(t *testing.T) {
	var sd *stuckDelete
	e, _ := restoreEnv(t, func(c *Config, d *Deps) {
		c.CPUMillis = 500
		sd = &stuckDelete{Runtime: d.Runtime, id: "sb-1_1_"}
		d.Runtime = sd
	})
	sd.stuck.Store(true)
	e.rt.Fail["Restore"] = errors.New("restore failed")
	_, err := e.create("sb-1", 1, spec(500, 256<<20))
	wantCode(t, err, codes.Internal, "fallback with a failing cleanup")
	if !strings.Contains(err.Error(), "cleanup") {
		t.Errorf("error %v does not mention the cleanup", err)
	}
	if slices.Contains(e.rt.Calls(), "Start id=sb-1_1_ net=none") {
		t.Errorf("cold start ran on a sandbox that was not cleaned up: %q", e.rt.Calls())
	}
	if !e.inTable("sb-1") || len(e.m.List()) != 0 {
		t.Errorf("sb-1: in table %v, List %v; want broken (in the table, not listed)", e.inTable("sb-1"), e.m.List())
	}
	_, err = e.create("sb-2", 1, spec(500, 256<<20))
	wantCode(t, err, codes.ResourceExhausted, "sb-2 while broken sb-1 holds the node")

	sd.stuck.Store(false)
	eventually(t, "the liveness loop to finish sb-1's teardown", func() bool { return !e.inTable("sb-1") })
	if _, ok := e.rt.Containers()["sb-1_1_"]; ok {
		t.Errorf("containers = %v", e.rt.Containers())
	}
	e.mustCreate(t, "sb-2", 1, spec(500, 256<<20))
}

// A node whose own image build failed picks up the image another agent on
// the host builds, looking at most once per imageRecheck.
func TestCreateFindsSiblingImage(t *testing.T) {
	ck := t.TempDir()
	a, _ := imageEnv(t, ck, nil)
	a.rt.Fail["Checkpoint"] = errors.New("checkpoint failed")
	a.start(t)
	if a.m.ImageReady() {
		t.Fatal("ImageReady after a failed build")
	}
	if resp := a.mustCreate(t, "sb-1", 1, nil); resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("no image anywhere: started_as = %v, want COLD", resp.GetStartedAs())
	}
	b, _ := imageEnv(t, ck, nil)
	if _, err := b.m.EnsureImage(context.Background()); err != nil {
		t.Fatal(err)
	}
	if resp := a.mustCreate(t, "sb-2", 1, nil); resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("within %v of the last look: started_as = %v, want COLD", imageRecheck, resp.GetStartedAs())
	}
	a.clock.Set(t0.Add(imageRecheck))
	if resp := a.mustCreate(t, "sb-3", 1, nil); resp.GetStartedAs() != gssv1.StartMode_START_MODE_RESTORE {
		t.Errorf("after %v: started_as = %v, want RESTORE from the sibling's image", imageRecheck, resp.GetStartedAs())
	}
	if !a.m.ImageReady() {
		t.Error("ImageReady = false after picking up the sibling's image")
	}
}

func TestEgressUsesRestore(t *testing.T) {
	n := &fakeNet{}
	e, _ := restoreEnv(t, func(_ *Config, d *Deps) { d.Net = n })
	egress := &gssv1.SandboxSpec{EgressAllow: []string{"api.example:443"}}
	before := len(e.rt.Calls())
	resp := e.mustCreate(t, "eg-1", 1, egress)
	if resp.GetStartedAs() != gssv1.StartMode_START_MODE_RESTORE {
		t.Errorf("started_as = %v, want RESTORE", resp.GetStartedAs())
	}
	got := e.rt.Calls()[before:]
	for _, call := range []string{"Create id=eg-1_1_ net=sandbox", "Restore id=eg-1_1_ net=sandbox"} {
		if !slices.Contains(got, call) {
			t.Errorf("runtime calls = %q, want %q", got, call)
		}
	}
	raw, err := os.ReadFile(filepath.Join(e.bundleDir("eg-1_1_"), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"path": "/var/run/netns/gss-eg-1_1_"`) {
		t.Error("bundle does not join the prepared namespace")
	}
	if got, want := n.Calls(), []string{"Setup eg-1_1_ api.example:443"}; !slices.Equal(got, want) {
		t.Errorf("net calls = %q, want %q", got, want)
	}

	// A fallback to cold tears the network down and sets it up again.
	e.rt.Fail["Restore"] = errors.New("restore failed")
	if resp := e.mustCreate(t, "eg-2", 1, egress); resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("started_as = %v, want COLD", resp.GetStartedAs())
	}
	want := []string{"Setup eg-2_1_ api.example:443", "Teardown eg-2_1_", "Setup eg-2_1_ api.example:443"}
	if got := n.Calls()[1:]; !slices.Equal(got, want) {
		t.Errorf("net calls = %q, want %q", got, want)
	}
	if !slices.Contains(e.rt.Calls(), "Start id=eg-2_1_ net=sandbox") {
		t.Errorf("runtime calls = %q", e.rt.Calls())
	}
}

// A node with no warm pool and no checkpoint image serves only COLD.
func TestForcedModesUnavailable(t *testing.T) {
	e := startEnv(t, nil)
	before := len(e.rt.Calls())
	for _, mode := range []gssv1.StartMode{gssv1.StartMode_START_MODE_WARM, gssv1.StartMode_START_MODE_RESTORE} {
		_, err := e.create("sb-1", 1, &gssv1.SandboxSpec{StartMode: mode})
		wantCode(t, err, codes.FailedPrecondition, mode.String())
	}
	if got := e.rt.Calls()[before:]; len(got) != 0 {
		t.Errorf("runtime calls = %q", got)
	}
	resp := e.mustCreate(t, "sb-1", 1, &gssv1.SandboxSpec{StartMode: gssv1.StartMode_START_MODE_COLD})
	if resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("forced COLD started as %v", resp.GetStartedAs())
	}
}

func TestClose(t *testing.T) {
	e := newEnv(t, nil)
	if err := e.m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.mustCreate(t, "sb-1", 1, nil)
	e.mustCreate(t, "sb-2", 1, nil)
	if err := e.m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(e.rt.Containers()) != 0 || len(e.m.List()) != 0 {
		t.Errorf("after Close: containers %v, list %v", e.rt.Containers(), e.m.List())
	}
	ents, _ := os.ReadDir(filepath.Join(e.state, "bundles"))
	if len(ents) != 0 {
		t.Errorf("bundles left: %v", ents)
	}
	_, err := e.create("sb-3", 1, nil)
	wantCode(t, err, codes.Unavailable, "Create after Close")
	if err := e.m.Close(context.Background()); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestNewManagerValidates(t *testing.T) {
	ok := Config{NodeID: "n1", CPUMillis: 1000, MemoryBytes: 1 << 30, MaxSandboxes: 1, RootFS: "/r", StateDir: "/s"}
	deps := Deps{Runtime: runsc.NewFake(), Cgroups: &fakeCgroups{}}
	if _, err := NewManager(ok, deps); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	bad := map[string]func(*Config, *Deps){
		"node id":       func(c *Config, _ *Deps) { c.NodeID = "a/b" },
		"empty node id": func(c *Config, _ *Deps) { c.NodeID = "" },
		"cpu":           func(c *Config, _ *Deps) { c.CPUMillis = 0 },
		"memory":        func(c *Config, _ *Deps) { c.MemoryBytes = -1 },
		"max":           func(c *Config, _ *Deps) { c.MaxSandboxes = 0 },
		"pool":          func(c *Config, _ *Deps) { c.WarmPoolSize = -1 },
		"rootfs":        func(c *Config, _ *Deps) { c.RootFS = "rel" },
		"state":         func(c *Config, _ *Deps) { c.StateDir = "" },
		"runtime":       func(_ *Config, d *Deps) { d.Runtime = nil },
		"cgroups":       func(_ *Config, d *Deps) { d.Cgroups = nil },
	}
	for name, mod := range bad {
		c, d := ok, deps
		mod(&c, &d)
		if _, err := NewManager(c, d); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestCreateSpans(t *testing.T) {
	sr := recordSpans(t)

	e := startEnv(t, nil)
	sr.Reset()
	e.mustCreate(t, "sb-1", 1, nil)
	if err := e.m.Destroy(context.Background(), "sb-1", 0); err != nil {
		t.Fatal(err)
	}
	spans := sr.Ended()
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byName[s.Name()] = s
	}
	parent := func(child, want string) {
		t.Helper()
		c, p := byName[child], byName[want]
		if c == nil || p == nil {
			t.Fatalf("missing span %q or %q in %d spans", child, want, len(spans))
		}
		if c.Parent().SpanID() != p.SpanContext().SpanID() {
			t.Errorf("span %s is not a child of %s", child, want)
		}
	}
	parent("runsc.create", "node.create")
	parent("runsc.start", "node.create")
	parent("sandbox.ready", "node.create")
	parent("runsc.exec", "sandbox.ready")
	parent("runsc.state", "node.create")
	parent("runsc.delete", "node.destroy")
	if !slices.Contains(byName["node.create"].Attributes(), attribute.String("start.mode", "cold")) {
		t.Errorf("node.create attributes = %v", byName["node.create"].Attributes())
	}
}

func TestCgroupFS(t *testing.T) {
	root := t.TempDir()
	cg := NewCgroupFS(root)
	sb := filepath.Join(root, "gss", "n1", "sb-1_1_")
	if err := os.MkdirAll(sb, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"cpu.max": "max 100000\n", "memory.max": "max\n",
		"memory.events": "low 0\nhigh 0\nmax 4\noom 2\noom_kill 3\noom_group_kill 0\n",
	} {
		if err := os.WriteFile(filepath.Join(sb, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := cg.SetLimits("/gss/n1/sb-1_1_", 1500, 512<<20); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"cpu.max": "150000 100000", "memory.max": "536870912"} {
		if b, _ := os.ReadFile(filepath.Join(sb, name)); string(b) != want {
			t.Errorf("%s = %q, want %q", name, b, want)
		}
	}
	if n, err := cg.OOMKills("/gss/n1/sb-1_1_"); err != nil || n != 3 {
		t.Errorf("OOMKills = %d, %v; want 3", n, err)
	}
	if err := cg.SetLimits("/gss/n1/absent", 1000, 1<<30); err == nil {
		t.Error("SetLimits on a missing cgroup succeeded")
	}
	for _, p := range []string{"gss/n1", "/gss/../etc", "/gss/n1/", "/", ""} {
		if err := cg.Remove(p); err == nil {
			t.Errorf("Remove(%q) accepted", p)
		}
	}

	if err := cg.Remove("/gss/n1/absent"); err != nil {
		t.Errorf("Remove of a missing cgroup: %v", err)
	}
	empty := filepath.Join(root, "gss", "n1", "sb-2_1_")
	if err := os.MkdirAll(filepath.Join(empty, "child"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := cg.Remove("/gss/n1/sb-2_1_"); err != nil || exists(empty) {
		t.Errorf("Remove: %v, still there: %v", err, exists(empty))
	}

	if err := cg.Ensure("/gss/n3"); err != nil || !exists(filepath.Join(root, "gss", "n3")) {
		t.Errorf("Ensure: %v", err)
	}
	if err := cg.Ensure("/gss/n3"); err != nil {
		t.Errorf("Ensure of an existing cgroup: %v", err)
	}
	if err := cg.Ensure("/gss/../n3"); err == nil {
		t.Error("Ensure accepted an unclean path")
	}

	if err := cg.KillTree("/gss/absent"); err != nil {
		t.Errorf("KillTree of a missing cgroup: %v", err)
	}
	node := filepath.Join(root, "gss", "n2")
	for _, d := range []string{"a_1_/inner", "b_1_"} {
		if err := os.MkdirAll(filepath.Join(node, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(node, "cgroup.kill"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := cg.KillTree("/gss/n2"); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(node, "cgroup.kill")); string(b) != "1" {
		t.Errorf("cgroup.kill = %q", b)
	}
	ents, _ := os.ReadDir(node)
	for _, ent := range ents {
		if ent.IsDir() {
			t.Errorf("child cgroup %s left", ent.Name())
		}
	}
}
