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
// set, until the call's ctx ends (a Create taking the sandbox's place, or
// Close). blocked counts the calls held.
type gatedPool struct {
	runsc.Runtime
	hold    atomic.Bool
	blocked atomic.Int32
}

func (g *gatedPool) held(ctx context.Context, ref runsc.Ref) (bool, error) {
	if !g.hold.Load() || !strings.HasPrefix(ref.ID, "warm_") {
		return false, nil
	}
	g.blocked.Add(1)
	<-ctx.Done()
	return true, ctx.Err()
}

func (g *gatedPool) Start(ctx context.Context, ref runsc.Ref) error {
	if held, err := g.held(ctx, ref); held {
		return err
	}
	return g.Runtime.Start(ctx, ref)
}

func (g *gatedPool) Restore(ctx context.Context, ref runsc.Ref, bundleDir, imagePath string) error {
	if held, err := g.held(ctx, ref); held {
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
