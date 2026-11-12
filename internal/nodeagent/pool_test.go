package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bundle"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// warmIDRE is the form of a pool sandbox's runsc ID.
var warmIDRE = regexp.MustCompile(`^warm_[0-9a-f]{8}_$`)

var (
	coldSpec = &gssv1.SandboxSpec{StartMode: gssv1.StartMode_START_MODE_COLD}
	warmSpec = &gssv1.SandboxSpec{StartMode: gssv1.StartMode_START_MODE_WARM}
)

// withPool sets WarmPoolSize to size, then applies mod (if not nil).
func withPool(size int, mod func(*Config, *Deps)) func(*Config, *Deps) {
	return func(c *Config, d *Deps) {
		c.WarmPoolSize = size
		if mod != nil {
			mod(c, d)
		}
	}
}

// poolEnv returns a started env whose node keeps a warm pool of size.
func poolEnv(t *testing.T, size int, mod func(*Config, *Deps)) *env {
	t.Helper()
	return startEnv(t, withPool(size, mod))
}

// waitWarm waits until exactly n pool sandboxes are ready.
func (e *env) waitWarm(t *testing.T, n int) {
	t.Helper()
	eventually(t, fmt.Sprintf("%d ready pool sandboxes", n), func() bool { return e.m.WarmReady() == n })
}

// warmIDs returns the runsc IDs of the ready pool sandboxes.
func (e *env) warmIDs() []string {
	e.m.mu.Lock()
	defer e.m.mu.Unlock()
	var out []string
	for _, p := range e.m.warm {
		out = append(out, p.ref.ID)
	}
	return out
}

// runscIDOf returns the runsc ID of the table entry of sandbox id.
func (e *env) runscIDOf(t *testing.T, id string) string {
	t.Helper()
	e.m.mu.Lock()
	defer e.m.mu.Unlock()
	s := e.m.sandboxes[id]
	if s == nil {
		t.Fatalf("no table entry for %s", id)
	}
	return s.ref.ID
}

// poolIDs returns, in order, the runsc IDs of pool sandboxes in calls of
// method, such as "Create id=warm_0123abcd_ net=none".
func poolIDs(calls []string, method string) []string {
	var out []string
	for _, c := range calls {
		f := strings.Fields(c)
		if len(f) == 3 && f[0] == method && strings.HasPrefix(f[1], "id=warm_") {
			out = append(out, strings.TrimPrefix(f[1], "id="))
		}
	}
	return out
}

// gatedPool holds runsc start and restore of pool sandboxes while hold is
// set: a held call goes on once hold is cleared, and fails when its ctx ends
// first (a Create taking the sandbox's place, or Close). blocked counts the
// calls held.
type gatedPool struct {
	runsc.Runtime
	hold    atomic.Bool
	blocked atomic.Int32
}

func (g *gatedPool) wait(ctx context.Context, ref runsc.Ref) error {
	if !g.hold.Load() || !strings.HasPrefix(ref.ID, "warm_") {
		return nil
	}
	g.blocked.Add(1)
	for g.hold.Load() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	return nil
}

func (g *gatedPool) Start(ctx context.Context, ref runsc.Ref) error {
	if err := g.wait(ctx, ref); err != nil {
		return err
	}
	return g.Runtime.Start(ctx, ref)
}

func (g *gatedPool) Restore(ctx context.Context, ref runsc.Ref, bundleDir, imagePath string) error {
	if err := g.wait(ctx, ref); err != nil {
		return err
	}
	return g.Runtime.Restore(ctx, ref, bundleDir, imagePath)
}

// gate wraps the env's runtime in a gatedPool, stored in *g.
func gate(g **gatedPool) func(*Config, *Deps) {
	return func(_ *Config, d *Deps) {
		*g = &gatedPool{Runtime: d.Runtime}
		d.Runtime = *g
	}
}

// failLimits fails the next fail calls of SetLimits, recording each as
// "SetLimits failed <path>".
type failLimits struct {
	*fakeCgroups
	fail atomic.Int32
}

func (c *failLimits) SetLimits(path string, cpu, mem int64) error {
	if c.fail.Add(-1) >= 0 {
		c.record("SetLimits failed " + path)
		return errors.New("write cpu.max: permission denied")
	}
	return c.fakeCgroups.SetLimits(path, cpu, mem)
}

// holdLimits holds every SetLimits: it sends on entered, then waits for
// the outcome on release (nil: the limits are written).
type holdLimits struct {
	*fakeCgroups
	entered chan struct{}
	release chan error
}

func (c *holdLimits) SetLimits(path string, cpu, mem int64) error {
	c.entered <- struct{}{}
	if err := <-c.release; err != nil {
		return err
	}
	return c.fakeCgroups.SetLimits(path, cpu, mem)
}

// holdLimitsOf wraps the env's cgroups in a holdLimits, stored in *c.
func holdLimitsOf(c **holdLimits) func(*Config, *Deps) {
	return func(_ *Config, d *Deps) {
		*c = &holdLimits{fakeCgroups: d.Cgroups.(*fakeCgroups), entered: make(chan struct{}), release: make(chan error)}
		d.Cgroups = *c
	}
}

// stuckPoolDelete fails runsc delete of pool sandboxes while stuck is set.
type stuckPoolDelete struct {
	runsc.Runtime
	stuck atomic.Bool
	fails atomic.Int32 // deletes failed so far
}

func (s *stuckPoolDelete) Delete(ctx context.Context, ref runsc.Ref) error {
	if s.stuck.Load() && strings.HasPrefix(ref.ID, "warm_") {
		s.fails.Add(1)
		return errors.New("delete failed")
	}
	return s.Runtime.Delete(ctx, ref)
}

// pidOf returns the sandbox process of the container rid.
func (e *env) pidOf(t *testing.T, rid string) int {
	t.Helper()
	st, err := e.rt.State(context.Background(), runsc.Ref{ID: rid})
	if err != nil {
		t.Fatal(err)
	}
	return st.PID
}

// brokenEntry reports whether the table holds rid as a broken entry under
// its runsc ID.
func (e *env) brokenEntry(rid string) bool {
	e.m.mu.Lock()
	defer e.m.mu.Unlock()
	s := e.m.sandboxes[rid]
	return s != nil && s.state == stateBroken
}

func TestPoolFills(t *testing.T) {
	e := poolEnv(t, 2, func(c *Config, _ *Deps) { c.CPUMillis = 1000 })
	e.waitWarm(t, 2)
	ids := e.warmIDs()
	if len(ids) != 2 || ids[0] == ids[1] {
		t.Fatalf("pool runsc IDs = %q, want two distinct", ids)
	}
	for _, id := range ids {
		if !warmIDRE.MatchString(id) {
			t.Errorf("pool runsc ID %q does not match %s", id, warmIDRE)
		}
		if e.rt.Containers()[id] != "running" {
			t.Errorf("pool sandbox %s: containers = %v", id, e.rt.Containers())
		}
		if !exists(filepath.Join(e.bundleDir(id), "config.json")) {
			t.Errorf("pool sandbox %s has no bundle", id)
		}
	}
	if got := e.m.List(); len(got) != 0 {
		t.Errorf("List = %v, want no pool sandboxes", got)
	}
	// The pool stops at its size, with no network.
	time.Sleep(50 * time.Millisecond)
	if n := e.m.WarmReady(); n != 2 {
		t.Errorf("WarmReady = %d, want 2", n)
	}
	if got := poolIDs(e.rt.Calls(), "Create"); len(got) != 2 {
		t.Errorf("pool sandboxes created: %q, want 2", got)
	}
	for _, c := range e.rt.Calls() {
		if strings.Contains(c, "id=warm_") && !strings.HasSuffix(c, " net=none") {
			t.Errorf("pool runtime call %q has a network", c)
		}
	}

	// Pool sandboxes do not count against CPU or memory: two 500m sandboxes
	// fill the 1000m node, and the pool makes no room for a third (it gives
	// way only at MaxSandboxes).
	cold500 := spec(500, 256<<20)
	cold500.StartMode = gssv1.StartMode_START_MODE_COLD
	e.mustCreate(t, "sb-1", 1, cold500)
	e.mustCreate(t, "sb-2", 1, cold500)
	_, err := e.create("sb-3", 1, cold500)
	wantCode(t, err, codes.ResourceExhausted, "third 500m sandbox")
	// A claimed sandbox counts like any other.
	_, err = e.create("sb-3", 1, warmSpec)
	wantCode(t, err, codes.ResourceExhausted, "claim of 500m on a full node")
	if n := e.m.WarmReady(); n != 2 {
		t.Errorf("WarmReady = %d after the refused Creates, want 2", n)
	}
}

func TestClaim(t *testing.T) {
	sr := recordSpans(t)
	e := poolEnv(t, 2, nil)
	e.waitWarm(t, 2)
	pooled := e.warmIDs()
	before := len(e.rt.Calls())
	resp := e.mustCreate(t, "sb-1", 1, spec(1000, 512<<20))
	if resp.GetStartedAs() != gssv1.StartMode_START_MODE_WARM || resp.GetReadyLatency() == nil {
		t.Fatalf("response = %v, want WARM with ready_latency", resp)
	}
	rid := e.runscIDOf(t, "sb-1")
	if !slices.Contains(pooled, rid) {
		t.Fatalf("sb-1 runs as %s, not one of the pool's %q", rid, pooled)
	}

	// No runsc call is on the claim path: none for the claimed sandbox
	// (the refill's are for new ones), and node.create has no child but
	// pool.claim.
	for _, c := range e.rt.Calls()[before:] {
		if strings.Contains(c, "id="+rid+" ") {
			t.Errorf("runtime call %q on the claimed sandbox", c)
		}
	}
	spans := sr.Ended()
	create := lastSpan(spans, "node.create")
	if create == nil {
		t.Fatal("no node.create span")
	}
	var children []string
	for _, s := range spans {
		if s.Parent().SpanID() == create.SpanContext().SpanID() {
			children = append(children, s.Name())
		}
	}
	if !slices.Equal(children, []string{"pool.claim"}) {
		t.Errorf("node.create children = %q, want only pool.claim", children)
	}
	if !slices.Contains(create.Attributes(), attribute.String("start.mode", "warm")) {
		t.Errorf("node.create attributes = %v", create.Attributes())
	}

	// The cgroup gets the request's limits.
	if n := e.cg.count(fmt.Sprintf("SetLimits %s 1000 536870912", bundle.CgroupPath("n1", rid))); n != 1 {
		t.Errorf("cgroup calls = %q, want one SetLimits of %s to 1000m / 512 MiB", e.cg.Calls(), rid)
	}
	l := e.m.List()
	if len(l) != 1 || l[0].GetId() != "sb-1" || l[0].GetIncarnation() != 1 ||
		l[0].GetStartedAs() != gssv1.StartMode_START_MODE_WARM ||
		l[0].GetSpec().GetResources().GetCpuMillis() != 1000 || l[0].GetSpec().GetResources().GetMemoryBytes() != 512<<20 {
		t.Errorf("List = %v, want sb-1/1 WARM with 1000m / 512 MiB", l)
	}

	// The pool refills behind the claim.
	e.waitWarm(t, 2)
	if slices.Contains(e.warmIDs(), rid) {
		t.Errorf("claimed %s is still in the pool %q", rid, e.warmIDs())
	}
	if n := len(e.rt.Containers()); n != 3 {
		t.Errorf("containers = %v, want sb-1 and two pool sandboxes", e.rt.Containers())
	}
}

func TestClaimRace(t *testing.T) {
	var g *gatedPool
	e := poolEnv(t, 1, func(c *Config, d *Deps) {
		c.CPUMillis = 8000 // ten 500m sandboxes
		gate(&g)(c, d)
	})
	e.waitWarm(t, 1)
	g.hold.Store(true) // no refill finishes during the race

	const n = 10
	resps, errs := make([]*gssv1.NodeCreateResponse, n), make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resps[i], errs[i] = e.create(fmt.Sprintf("sb-%d", i), 1, nil)
		}()
	}
	wg.Wait()
	warm := 0
	rids := map[string]bool{}
	for i := range n {
		id := fmt.Sprintf("sb-%d", i)
		if errs[i] != nil {
			t.Fatalf("Create %s: %v", id, errs[i])
		}
		switch mode := resps[i].GetStartedAs(); mode {
		case gssv1.StartMode_START_MODE_WARM:
			warm++
			if rid := e.runscIDOf(t, id); !warmIDRE.MatchString(rid) {
				t.Errorf("%s started WARM as %s", id, rid)
			}
		case gssv1.StartMode_START_MODE_RESTORE, gssv1.StartMode_START_MODE_COLD:
		default:
			t.Errorf("%s started as %v", id, mode)
		}
		rids[e.runscIDOf(t, id)] = true
	}
	if warm != 1 {
		t.Errorf("%d Creates started WARM from a pool of one, want exactly 1", warm)
	}
	if len(rids) != n {
		t.Errorf("runsc IDs = %v, want %d distinct", rids, n)
	}
}

func TestForcedWarmEmptyPool(t *testing.T) {
	wantEmpty := func(err error, what string) {
		t.Helper()
		if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "warm pool empty" {
			t.Errorf("%s: %v, want FailedPrecondition \"warm pool empty\"", what, err)
		}
	}
	e := startEnv(t, nil)
	_, err := e.create("sb-1", 1, warmSpec)
	wantEmpty(err, "node without a pool")

	// A pool of one whose refill cannot finish: of five forced-WARM Creates
	// at once, one claims the sandbox and four fail, whether they found the
	// pool empty when choosing the path or at the claim. A failed one runs
	// nothing and leaves nothing.
	var g *gatedPool
	p := poolEnv(t, 1, gate(&g))
	p.waitWarm(t, 1)
	g.hold.Store(true)
	before := len(p.rt.Calls())
	const n = 5
	resps, errs := make([]*gssv1.NodeCreateResponse, n), make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resps[i], errs[i] = p.create(fmt.Sprintf("sb-%d", i), 1, warmSpec)
		}()
	}
	wg.Wait()
	ok := 0
	for i := range n {
		id := fmt.Sprintf("sb-%d", i)
		if errs[i] == nil {
			ok++
			if resps[i].GetStartedAs() != gssv1.StartMode_START_MODE_WARM {
				t.Errorf("%s: forced WARM started as %v", id, resps[i].GetStartedAs())
			}
			continue
		}
		wantEmpty(errs[i], id)
		if p.inTable(id) {
			t.Errorf("failed %s left a table entry", id)
		}
	}
	if ok != 1 {
		t.Errorf("%d forced-WARM Creates succeeded from a pool of one, want 1", ok)
	}
	for _, c := range p.rt.Calls()[before:] {
		if !strings.Contains(c, "id=warm_") {
			t.Errorf("runtime call %q for a forced-WARM Create", c)
		}
	}
}

func TestEgressNeverWarm(t *testing.T) {
	n := &fakeNet{}
	e := poolEnv(t, 2, func(_ *Config, d *Deps) { d.Net = n })
	e.waitWarm(t, 2)
	egress := &gssv1.SandboxSpec{EgressAllow: []string{"api.example:443"}}
	resp := e.mustCreate(t, "eg-1", 1, egress)
	if resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("egress started as %v with a full pool, want COLD", resp.GetStartedAs())
	}
	if rid := e.runscIDOf(t, "eg-1"); rid != "eg-1_1_" {
		t.Errorf("egress sandbox runs as %s, want eg-1_1_", rid)
	}
	if !slices.Contains(e.rt.Calls(), "Create id=eg-1_1_ net=sandbox") {
		t.Errorf("runtime calls = %q", e.rt.Calls())
	}
	if got := e.m.WarmReady(); got != 2 {
		t.Errorf("WarmReady = %d after an egress Create, want 2 (nothing claimed)", got)
	}
	_, err := e.create("eg-2", 1, &gssv1.SandboxSpec{StartMode: gssv1.StartMode_START_MODE_WARM, EgressAllow: []string{"api.example:443"}})
	if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "warm pool cannot serve egress" {
		t.Errorf("forced WARM with egress: %v, want FailedPrecondition \"warm pool cannot serve egress\"", err)
	}
}

func TestPoolYieldsAtMax(t *testing.T) {
	e := poolEnv(t, 2, func(c *Config, _ *Deps) { c.MaxSandboxes = 3 })
	e.waitWarm(t, 2)
	pooled := e.warmIDs()
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("sb-%d", i)
		if resp := e.mustCreate(t, id, 1, coldSpec); resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
			t.Errorf("%s started as %v, want COLD", id, resp.GetStartedAs())
		}
	}
	if n := e.m.WarmReady(); n != 0 {
		t.Errorf("WarmReady = %d with the node full, want 0", n)
	}
	want := map[string]string{"sb-1_1_": "running", "sb-2_1_": "running", "sb-3_1_": "running"}
	if got := e.rt.Containers(); len(got) != len(want) || got["sb-1_1_"] != "running" || got["sb-2_1_"] != "running" || got["sb-3_1_"] != "running" {
		t.Errorf("containers = %v, want %v", got, want)
	}
	for _, id := range pooled {
		if !slices.Contains(e.rt.Calls(), "Delete id="+id+" net=none") || exists(e.bundleDir(id)) {
			t.Errorf("pool sandbox %s not removed: calls %q", id, e.rt.Calls())
		}
	}
	// The pool does not refill on a full node, and a fourth Create finds
	// nothing to take the place of.
	created := len(poolIDs(e.rt.Calls(), "Create"))
	time.Sleep(50 * time.Millisecond)
	if n := len(poolIDs(e.rt.Calls(), "Create")); n != created || e.m.WarmReady() != 0 {
		t.Errorf("pool started %d sandboxes on a full node", n-created)
	}
	_, err := e.create("sb-4", 1, coldSpec)
	wantCode(t, err, codes.ResourceExhausted, "fourth sandbox")
	// A destroy makes room, and the pool takes it.
	if err := e.m.Destroy(context.Background(), "sb-1", 0); err != nil {
		t.Fatal(err)
	}
	e.waitWarm(t, 1)
}

// A pool sandbox still starting gives way too: the Create that needs its
// place ends the start and waits for the teardown.
func TestPoolYieldsDuringFill(t *testing.T) {
	var g *gatedPool
	e, _ := restoreEnv(t, withPool(1, func(c *Config, d *Deps) {
		c.MaxSandboxes = 1
		gate(&g)(c, d)
		g.hold.Store(true) // pool restores never finish; the image build's do
	}))
	eventually(t, "the pool restore to start", func() bool { return g.blocked.Load() == 1 })
	resp := e.mustCreate(t, "sb-1", 1, coldSpec)
	if resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("started as %v, want COLD", resp.GetStartedAs())
	}
	started := poolIDs(e.rt.Calls(), "Create")
	if len(started) != 1 {
		t.Fatalf("pool sandboxes created: %q, want 1", started)
	}
	if got := e.rt.Containers(); len(got) != 1 || got["sb-1_1_"] != "running" {
		t.Errorf("containers = %v, want only sb-1_1_", got)
	}
	if exists(e.bundleDir(started[0])) || e.m.WarmReady() != 0 {
		t.Errorf("pool sandbox %s not removed (WarmReady %d)", started[0], e.m.WarmReady())
	}
	if !e.m.ImageReady() {
		t.Error("a pool restore ended to make room dropped the image")
	}
}

// Pool sandboxes are restored when the node has an image, and a failed pool
// restore counts as a Create's does: a failed runsc restore drops the image,
// and the pool then fills cold.
func TestPoolRestores(t *testing.T) {
	e, _ := restoreEnv(t, withPool(2, nil))
	e.waitWarm(t, 2)
	for _, id := range e.warmIDs() {
		if !slices.Contains(e.rt.Calls(), "Restore id="+id+" net=none") {
			t.Errorf("pool sandbox %s was not restored: %q", id, e.rt.Calls())
		}
	}
	if got := poolIDs(e.rt.Calls(), "Start"); len(got) != 0 {
		t.Errorf("pool sandboxes started cold with an image: %q", got)
	}
	e.rt.Fail["Restore"] = errors.New("restore failed")
	if resp := e.mustCreate(t, "sb-1", 1, nil); resp.GetStartedAs() != gssv1.StartMode_START_MODE_WARM {
		t.Fatalf("started as %v, want WARM", resp.GetStartedAs())
	}
	e.waitWarm(t, 2)
	if e.m.ImageReady() {
		t.Error("ImageReady after a failed pool restore")
	}
	if got := poolIDs(e.rt.Calls(), "Start"); len(got) != 1 {
		t.Errorf("pool sandboxes started cold after the image was dropped: %q, want 1", got)
	}
}

func TestPoolRetryAfterFailure(t *testing.T) {
	m, err := NewManager(Config{NodeID: "n1", CPUMillis: 1000, MemoryBytes: 1 << 30, MaxSandboxes: 1, RootFS: "/r", StateDir: "/s"},
		Deps{Runtime: runsc.NewFake(), Cgroups: &fakeCgroups{}})
	if err != nil {
		t.Fatal(err)
	}
	if m.poolRetry != 2*time.Second {
		t.Errorf("poolRetry = %v, want 2s", m.poolRetry)
	}

	const retry = 100 * time.Millisecond
	e := newEnv(t, withPool(2, nil))
	e.m.poolRetry = retry
	e.rt.Fail["Start"] = errors.New("boom")
	begin := time.Now()
	e.start(t)
	e.waitWarm(t, 2)
	if d := time.Since(begin); d < retry {
		t.Errorf("pool full after %v; the retry after a failed start waits %v", d, retry)
	}
	started := poolIDs(e.rt.Calls(), "Create")
	if len(started) != 3 {
		t.Fatalf("pool sandboxes created: %q, want 3 (one failed)", started)
	}
	failed := started[0]
	if slices.Contains(e.warmIDs(), failed) {
		t.Errorf("failed %s is in the pool", failed)
	}
	if _, ok := e.rt.Containers()[failed]; ok || exists(e.bundleDir(failed)) || e.cg.count("Remove "+bundle.CgroupPath("n1", failed)) != 1 {
		t.Errorf("failed pool sandbox %s not cleaned up: containers %v, cgroups %q", failed, e.rt.Containers(), e.cg.Calls())
	}
}

func TestSetLimitsFailureFallsBack(t *testing.T) {
	var cg *failLimits
	e := poolEnv(t, 2, func(_ *Config, d *Deps) {
		cg = &failLimits{fakeCgroups: d.Cgroups.(*fakeCgroups)}
		d.Cgroups = cg
	})
	e.waitWarm(t, 2)
	cg.fail.Store(1)
	resp := e.mustCreate(t, "sb-1", 1, spec(1000, 512<<20))
	if resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("started as %v after a failed claim, want COLD", resp.GetStartedAs())
	}
	if rid := e.runscIDOf(t, "sb-1"); rid != "sb-1_1_" {
		t.Errorf("sb-1 runs as %s, want sb-1_1_", rid)
	}
	var bad string
	for _, c := range e.cg.Calls() {
		if p, ok := strings.CutPrefix(c, "SetLimits failed "); ok {
			bad = filepath.Base(p)
		}
	}
	if !warmIDRE.MatchString(bad) {
		t.Fatalf("no failed SetLimits on a pool sandbox: %q", e.cg.Calls())
	}
	if _, ok := e.rt.Containers()[bad]; ok || exists(e.bundleDir(bad)) || e.cg.count("Remove "+bundle.CgroupPath("n1", bad)) != 1 {
		t.Errorf("pool sandbox %s of the failed claim not removed: containers %v", bad, e.rt.Containers())
	}
	e.waitWarm(t, 2)

	// A forced WARM fails with the claim.
	cg.fail.Store(1)
	_, err := e.create("sb-2", 1, warmSpec)
	wantCode(t, err, codes.Internal, "forced WARM with a failing SetLimits")
	if !strings.Contains(err.Error(), "pool.claim") {
		t.Errorf("error %v does not name pool.claim", err)
	}
	if e.inTable("sb-2") {
		t.Error("the failed forced WARM left a table entry")
	}
	e.waitWarm(t, 2)
}

// A claimed sandbox keeps its runsc ID: Exec, Destroy, the reaper and the
// liveness check all reach it by that ID.
func TestClaimedSandbox(t *testing.T) {
	e := newEnv(t, withPool(1, nil))
	var dead atomic.Int64
	e.m.processAlive = func(pid int) bool { return int64(pid) != dead.Load() }
	e.start(t)
	ctx := context.Background()

	e.waitWarm(t, 1)
	var mu sync.Mutex
	var pidDirs []string
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		if s.InternalPIDFile != "" {
			mu.Lock()
			pidDirs = append(pidDirs, ref.ID+" "+filepath.Dir(s.InternalPIDFile))
			mu.Unlock()
		}
		return 0, nil
	}
	e.mustCreate(t, "sb-1", 1, nil)
	rid := e.runscIDOf(t, "sb-1")
	c := &collector{}
	if err := e.m.Exec(ctx, pyReq("sb-1", "print(1)"), c.send); err != nil {
		t.Fatal(err)
	}
	if exit := c.exit(t); exit.GetCode() != 0 {
		t.Errorf("exit = %v", exit)
	}
	mu.Lock()
	got := slices.Clone(pidDirs)
	mu.Unlock()
	if want := []string{rid + " " + e.execDir(rid)}; !slices.Equal(got, want) {
		t.Errorf("Exec pid dirs = %q, want %q", got, want)
	}
	if err := e.m.Destroy(ctx, "sb-1", 0); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.rt.Containers()[rid]; ok || exists(e.bundleDir(rid)) || exists(e.execDir(rid)) || e.cg.count("Remove "+bundle.CgroupPath("n1", rid)) != 1 {
		t.Errorf("Destroy left %s: containers %v, cgroups %q", rid, e.rt.Containers(), e.cg.Calls())
	}

	// The reaper.
	e.waitWarm(t, 1)
	e.mustCreate(t, "sb-2", 1, &gssv1.SandboxSpec{Ttl: durationpb.New(time.Minute)})
	rid = e.runscIDOf(t, "sb-2")
	e.clock.Set(t0.Add(71 * time.Second))
	eventually(t, "the reaper to destroy sb-2", func() bool { return !e.inTable("sb-2") })
	if _, ok := e.rt.Containers()[rid]; ok || exists(e.bundleDir(rid)) {
		t.Errorf("reaper left %s: containers %v", rid, e.rt.Containers())
	}

	// The liveness check.
	e.waitWarm(t, 1)
	e.mustCreate(t, "sb-3", 1, nil)
	rid = e.runscIDOf(t, "sb-3")
	st, err := e.rt.State(ctx, runsc.Ref{ID: rid})
	if err != nil {
		t.Fatal(err)
	}
	dead.Store(int64(st.PID))
	eventually(t, "the liveness loop to destroy sb-3", func() bool { return !e.inTable("sb-3") })
	if _, ok := e.rt.Containers()[rid]; ok || exists(e.bundleDir(rid)) {
		t.Errorf("liveness loop left %s: containers %v", rid, e.rt.Containers())
	}
}

func TestCloseDestroysPool(t *testing.T) {
	e := newEnv(t, withPool(2, nil))
	if err := e.m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.waitWarm(t, 2)
	e.mustCreate(t, "sb-1", 1, nil) // claims one
	e.waitWarm(t, 2)
	ids := append(e.warmIDs(), e.runscIDOf(t, "sb-1"))
	if err := e.m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := e.rt.Containers(); len(got) != 0 {
		t.Errorf("containers after Close = %v", got)
	}
	if ents, _ := os.ReadDir(filepath.Join(e.state, "bundles")); len(ents) != 0 {
		t.Errorf("bundles after Close: %v", ents)
	}
	for _, id := range ids {
		if e.cg.count("Remove "+bundle.CgroupPath("n1", id)) != 1 {
			t.Errorf("cgroup of %s not removed: %q", id, e.cg.Calls())
		}
	}
	if n := e.m.WarmReady(); n != 0 {
		t.Errorf("WarmReady after Close = %d", n)
	}
	calls := len(e.rt.Calls())
	time.Sleep(50 * time.Millisecond)
	if got := e.rt.Calls()[calls:]; len(got) != 0 {
		t.Errorf("runtime calls after Close: %q", got)
	}

	// A pool start in flight ends with Close.
	var g *gatedPool
	f := newEnv(t, withPool(1, gate(&g)))
	g.hold.Store(true)
	if err := f.m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the pool start to begin", func() bool { return g.blocked.Load() == 1 })
	begin := time.Now()
	if err := f.m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(begin); d > time.Second {
		t.Errorf("Close took %v with a pool start in flight", d)
	}
	if got := f.rt.Containers(); len(got) != 0 {
		t.Errorf("containers after Close = %v", got)
	}
	if ents, _ := os.ReadDir(filepath.Join(f.state, "bundles")); len(ents) != 0 {
		t.Errorf("bundles after Close: %v", ents)
	}
}

// A claimed sandbox is starting, not ready, until its limits are written:
// an Exec in that window gets Unavailable, as for any sandbox still
// starting, so nothing runs at the pool's size.
func TestClaimNotReadyUntilLimits(t *testing.T) {
	var cg *holdLimits
	e := poolEnv(t, 1, holdLimitsOf(&cg))
	e.waitWarm(t, 1)
	ctx := context.Background()
	var resp *gssv1.NodeCreateResponse
	created := make(chan error, 1)
	go func() {
		var err error
		resp, err = e.create("sb-1", 1, spec(1000, 512<<20))
		created <- err
	}()
	<-cg.entered
	c := &collector{}
	err := e.m.Exec(ctx, pyReq("sb-1", "print(1)"), c.send)
	if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "still starting") {
		t.Errorf("Exec during the claim: %v, want Unavailable \"still starting\"", err)
	}
	if len(c.events) != 0 {
		t.Errorf("Exec during the claim sent %d events", len(c.events))
	}
	cg.release <- nil
	if err := <-created; err != nil {
		t.Fatal(err)
	}
	if resp.GetStartedAs() != gssv1.StartMode_START_MODE_WARM {
		t.Errorf("started as %v, want WARM", resp.GetStartedAs())
	}
	c = &collector{}
	if err := e.m.Exec(ctx, pyReq("sb-1", "print(1)"), c.send); err != nil {
		t.Fatalf("Exec after the claim: %v", err)
	}
	c.exit(t)
}

// A claim whose SetLimits fails ends the Execs registered on the sandbox,
// as a destroy does, before the container goes. No Exec can register while
// the claimed sandbox is starting, so the test marks it ready by hand to
// register one.
func TestClaimFailureEndsExecs(t *testing.T) {
	var cg *holdLimits
	e := newEnv(t, withPool(1, holdLimitsOf(&cg)))
	// User Execs (they have a pid file) run until stopped; probes pass.
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		if s.InternalPIDFile == "" {
			return 0, nil
		}
		<-ctx.Done()
		return -1, ctx.Err()
	}
	e.start(t)
	e.waitWarm(t, 1)
	created := make(chan error, 1)
	go func() {
		_, err := e.create("sb-1", 1, warmSpec)
		created <- err
	}()
	<-cg.entered
	e.m.mu.Lock()
	s := e.m.sandboxes["sb-1"]
	rid := s.ref.ID
	s.state = stateReady
	e.m.mu.Unlock()
	execd := make(chan error, 1)
	go func() { execd <- e.m.Exec(context.Background(), pyReq("sb-1", "print(1)"), (&collector{}).send) }()
	eventually(t, "the Exec to register", func() bool {
		e.m.mu.Lock()
		defer e.m.mu.Unlock()
		return len(s.execs) == 1
	})
	cg.release <- errors.New("write cpu.max: permission denied")
	wantCode(t, <-created, codes.Internal, "forced WARM with a failed SetLimits")
	select {
	case err := <-execd:
		wantCode(t, err, codes.Unavailable, "Exec on the failed claim")
	case <-time.After(2 * time.Second):
		t.Fatal("the Exec on the failed claim did not end")
	}
	if _, ok := e.rt.Containers()[rid]; ok || e.inTable("sb-1") || exists(e.bundleDir(rid)) {
		t.Errorf("failed claim left %s: containers %v", rid, e.rt.Containers())
	}
}

// The liveness loop drops a ready pool sandbox whose process is gone, and
// the pool replaces it.
func TestPoolDropsDeadSandbox(t *testing.T) {
	var g *gatedPool
	e := newEnv(t, withPool(2, gate(&g)))
	var dead atomic.Int64
	e.m.processAlive = func(pid int) bool { return int64(pid) != dead.Load() }
	e.start(t)
	e.waitWarm(t, 2)
	gone := e.warmIDs()[0]
	g.hold.Store(true) // the replacement waits, so the drop shows
	dead.Store(int64(e.pidOf(t, gone)))
	e.waitWarm(t, 1)
	if slices.Contains(e.warmIDs(), gone) {
		t.Fatalf("dead %s is still in the pool", gone)
	}
	eventually(t, "the dead pool sandbox's teardown", func() bool {
		_, ok := e.rt.Containers()[gone]
		return !ok && !exists(e.bundleDir(gone)) && e.cg.count("Remove "+bundle.CgroupPath("n1", gone)) == 1
	})
	g.hold.Store(false)
	e.waitWarm(t, 2)
	if slices.Contains(e.warmIDs(), gone) {
		t.Errorf("dead %s is back in the pool %q", gone, e.warmIDs())
	}
}

// A pool sandbox whose teardown fails stays as a broken table entry under
// its runsc ID: not listed, counted against the node, retried by the
// liveness loop, and removed by Close.
func TestPoolTeardownFailure(t *testing.T) {
	var sd *stuckPoolDelete
	stuck := func(c *Config, d *Deps) {
		c.MaxSandboxes = 1
		sd = &stuckPoolDelete{Runtime: d.Runtime}
		d.Runtime = sd
	}
	e := newEnv(t, withPool(1, stuck))
	var dead atomic.Int64
	e.m.processAlive = func(pid int) bool { return int64(pid) != dead.Load() }
	e.start(t)
	e.waitWarm(t, 1)
	rid := e.warmIDs()[0]
	sd.stuck.Store(true)
	dead.Store(int64(e.pidOf(t, rid)))
	eventually(t, "a broken entry for the dead pool sandbox", func() bool { return e.brokenEntry(rid) })
	if got := e.m.List(); len(got) != 0 {
		t.Errorf("List = %v, want nothing", got)
	}
	_, err := e.create("sb-1", 1, coldSpec)
	wantCode(t, err, codes.ResourceExhausted, "Create while the broken pool sandbox holds the node")
	fails := sd.fails.Load()
	eventually(t, "the liveness loop to retry the teardown", func() bool { return sd.fails.Load() > fails })
	sd.stuck.Store(false)
	eventually(t, "the retried teardown", func() bool { return !e.inTable(rid) })
	if _, ok := e.rt.Containers()[rid]; ok || exists(e.bundleDir(rid)) {
		t.Errorf("%s not cleaned up: containers %v", rid, e.rt.Containers())
	}
	e.waitWarm(t, 1) // the node has room for the pool again

	// A teardown that fails when a Create takes a pool sandbox's place: the
	// Create fails, and Close removes the broken entry.
	f := newEnv(t, withPool(1, stuck))
	f.m.liveEvery = time.Hour // only Close retries
	if err := f.m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.waitWarm(t, 1)
	rid = f.warmIDs()[0]
	sd.stuck.Store(true)
	_, err = f.create("sb-1", 1, coldSpec)
	wantCode(t, err, codes.Internal, "Create whose pool sandbox would not go")
	if !f.brokenEntry(rid) || f.inTable("sb-1") {
		t.Errorf("broken %s in the table: %v, sb-1 in the table: %v; want true, false", rid, f.brokenEntry(rid), f.inTable("sb-1"))
	}
	sd.stuck.Store(false)
	if err := f.m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.rt.Containers(); len(got) != 0 || f.inTable(rid) || exists(f.bundleDir(rid)) {
		t.Errorf("after Close: containers %v, %s in the table %v", got, rid, f.inTable(rid))
	}
}

// A claim never hands out a pool sandbox whose process is gone: it has it
// torn down and takes the next one, or misses when none is left.
func TestClaimSkipsDeadSandbox(t *testing.T) {
	var dead atomic.Int64
	alive := func(pid int) bool { return int64(pid) != dead.Load() }
	e := newEnv(t, withPool(2, nil))
	e.m.liveEvery = time.Hour // only the claim looks at the pool's processes
	e.m.processAlive = alive
	e.start(t)
	e.waitWarm(t, 2)
	ids := e.warmIDs()
	dead.Store(int64(e.pidOf(t, ids[0]))) // the oldest, which a claim takes first
	if resp := e.mustCreate(t, "sb-1", 1, nil); resp.GetStartedAs() != gssv1.StartMode_START_MODE_WARM {
		t.Errorf("started as %v, want WARM", resp.GetStartedAs())
	}
	if rid := e.runscIDOf(t, "sb-1"); rid != ids[1] {
		t.Errorf("sb-1 runs as %s, want the live pool sandbox %s (not the dead %s)", rid, ids[1], ids[0])
	}
	eventually(t, "the dead pool sandbox's teardown", func() bool {
		_, ok := e.rt.Containers()[ids[0]]
		return !ok && !exists(e.bundleDir(ids[0]))
	})
	e.waitWarm(t, 2)
	if slices.Contains(e.warmIDs(), ids[0]) {
		t.Errorf("dead %s is back in the pool", ids[0])
	}

	// With the only pool sandbox dead, AUTO misses and starts cold, and a
	// forced WARM finds the pool empty.
	var g *gatedPool
	f := newEnv(t, withPool(1, gate(&g)))
	f.m.liveEvery = time.Hour
	f.m.processAlive = alive
	f.start(t)
	f.waitWarm(t, 1)
	g.hold.Store(true) // no replacement
	dead.Store(int64(f.pidOf(t, f.warmIDs()[0])))
	if resp := f.mustCreate(t, "sb-1", 1, nil); resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("AUTO with a dead pool started as %v, want COLD", resp.GetStartedAs())
	}
	_, err := f.create("sb-2", 1, warmSpec)
	if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "warm pool empty" {
		t.Errorf("forced WARM with a dead pool: %v, want FailedPrecondition \"warm pool empty\"", err)
	}
}

// A claim that races Close ends either WARM, and then destroyed by Close,
// or Unavailable; Close leaves nothing.
func TestClaimDuringClose(t *testing.T) {
	ctx := context.Background()
	empty := func(e *env, what string) {
		t.Helper()
		e.m.mu.Lock()
		entries := len(e.m.sandboxes)
		e.m.mu.Unlock()
		ents, _ := os.ReadDir(filepath.Join(e.state, "bundles"))
		if got := e.rt.Containers(); len(got) != 0 || len(ents) != 0 || entries != 0 || e.m.WarmReady() != 0 {
			t.Errorf("%s: containers %v, bundles %v, %d table entries, WarmReady %d; want nothing", what, got, ents, entries, e.m.WarmReady())
		}
	}

	// Close begins while a claim waits in SetLimits: Close waits for the
	// claim, then destroys the sandbox.
	var cg *holdLimits
	e := newEnv(t, withPool(1, holdLimitsOf(&cg)))
	if err := e.m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	e.waitWarm(t, 1)
	var resp *gssv1.NodeCreateResponse
	created := make(chan error, 1)
	go func() {
		var err error
		resp, err = e.create("sb-1", 1, nil)
		created <- err
	}()
	<-cg.entered
	closed := make(chan error, 1)
	go func() { closed <- e.m.Close(ctx) }()
	select {
	case err := <-closed:
		t.Errorf("Close returned (%v) while a claim was in flight", err)
	case <-time.After(50 * time.Millisecond):
	}
	cg.release <- nil
	if err := <-created; err != nil || resp.GetStartedAs() != gssv1.StartMode_START_MODE_WARM {
		t.Errorf("Create during Close: %v, %v; want WARM", resp, err)
	}
	if err := <-closed; err != nil {
		t.Errorf("Close: %v", err)
	}
	empty(e, "claim held during Close")

	// Unheld, in whichever order the two run.
	for i := range 20 {
		f := newEnv(t, withPool(1, nil))
		if err := f.m.Start(ctx); err != nil {
			t.Fatal(err)
		}
		f.waitWarm(t, 1)
		var resp *gssv1.NodeCreateResponse
		var cerr, closeErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); resp, cerr = f.create("sb-1", 1, nil) }()
		go func() { defer wg.Done(); closeErr = f.m.Close(ctx) }()
		wg.Wait()
		if !(cerr == nil && resp.GetStartedAs() == gssv1.StartMode_START_MODE_WARM) && status.Code(cerr) != codes.Unavailable {
			t.Errorf("round %d: Create %v, %v; want WARM or Unavailable", i, resp, cerr)
		}
		if closeErr != nil {
			t.Errorf("round %d: Close: %v", i, closeErr)
		}
		empty(f, fmt.Sprintf("round %d", i))
	}
}
