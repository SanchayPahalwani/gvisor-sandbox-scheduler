package cluster

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

const mib = int64(1 << 20)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newState() (*State, *fakeClock) {
	c := &fakeClock{t: time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)}
	return New(DefaultConfig(), c.now), c
}

func nodeInfo(id, group string, cpu int64) *gssv1.NodeInfo {
	return &gssv1.NodeInfo{
		Id: id, Group: group, Address: id + ":7000",
		Capacity:     &gssv1.Resources{CpuMillis: cpu, MemoryBytes: 8 << 30},
		MaxSandboxes: 10,
	}
}

func specTTL(ttl time.Duration) *gssv1.SandboxSpec {
	return &gssv1.SandboxSpec{
		Resources: &gssv1.Resources{CpuMillis: 500, MemoryBytes: 256 * mib},
		Ttl:       durationpb.New(ttl),
	}
}

func spec() *gssv1.SandboxSpec { return specTTL(10 * time.Minute) }

func node(t *testing.T, s *State, id string) *gssv1.Node {
	t.Helper()
	for _, n := range s.Nodes() {
		if n.GetInfo().GetId() == id {
			return n
		}
	}
	t.Fatalf("node %s not found", id)
	return nil
}

func get(t *testing.T, s *State, id string) *gssv1.Sandbox {
	t.Helper()
	sb, ok := s.Get(id)
	if !ok {
		t.Fatalf("sandbox %s not found", id)
	}
	return sb
}

// place adds id and reserves and commits it on nodeID.
func place(t *testing.T, s *State, id, nodeID string, sp *gssv1.SandboxSpec) {
	t.Helper()
	s.Add(id, sp)
	inc, _, err := s.Reserve(id, nodeID, nil)
	if err != nil {
		t.Fatalf("reserve %s on %s: %v", id, nodeID, err)
	}
	if !s.Commit(id, inc, gssv1.StartMode_START_MODE_COLD, time.Second) {
		t.Fatalf("commit %s failed", id)
	}
}

func TestReserveCommit(t *testing.T) {
	s, _ := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	sb := s.Add("sb-1", spec())
	if sb.State != gssv1.SandboxState_SANDBOX_STATE_PENDING || sb.Incarnation != 0 {
		t.Fatalf("after Add: %v inc %d", sb.State, sb.Incarnation)
	}
	p := &gssv1.Placement{NodesTotal: 1, NodesFeasible: 1}
	inc, remaining, err := s.Reserve("sb-1", "n1", p)
	if err != nil || inc != 1 || remaining != 10*time.Minute {
		t.Fatalf("Reserve = %d, %v, %v", inc, remaining, err)
	}
	got := get(t, s, "sb-1")
	if got.State != gssv1.SandboxState_SANDBOX_STATE_STARTING || got.NodeId != "n1" || got.NodeGroup != "a" {
		t.Fatalf("after Reserve: %v", got)
	}
	if got.GetPlacement().GetNodesTotal() != 1 {
		t.Errorf("placement not stored: %v", got.Placement)
	}
	n := node(t, s, "n1")
	if n.Allocated.CpuMillis != 500 || n.Allocated.MemoryBytes != 256*mib || n.Sandboxes != 1 {
		t.Fatalf("node after Reserve: %v", n)
	}
	if !s.Commit("sb-1", 1, gssv1.StartMode_START_MODE_COLD, 300*time.Millisecond) {
		t.Fatal("Commit returned false")
	}
	got = get(t, s, "sb-1")
	if got.State != gssv1.SandboxState_SANDBOX_STATE_READY ||
		got.StartedAs != gssv1.StartMode_START_MODE_COLD ||
		got.StartLatency.AsDuration() != 300*time.Millisecond {
		t.Fatalf("after Commit: %v", got)
	}
}

func TestReserveRejectsOverCapacity(t *testing.T) {
	s, _ := newState()
	s.Register(nodeInfo("n1", "a", 1000))
	for _, id := range []string{"sb-1", "sb-2", "sb-3"} {
		s.Add(id, spec())
	}
	for _, id := range []string{"sb-1", "sb-2"} {
		if _, _, err := s.Reserve(id, "n1", nil); err != nil {
			t.Fatalf("reserve %s: %v", id, err)
		}
	}
	if _, _, err := s.Reserve("sb-3", "n1", nil); err == nil {
		t.Fatal("third reserve succeeded")
	}
	n := node(t, s, "n1")
	if n.Allocated.CpuMillis != 1000 || n.Sandboxes != 2 {
		t.Fatalf("allocation changed: %v", n)
	}
	if st := get(t, s, "sb-3"); st.State != gssv1.SandboxState_SANDBOX_STATE_PENDING || st.Incarnation != 0 {
		t.Fatalf("rejected sandbox changed: %v", st)
	}

	// max_sandboxes is enforced as well.
	small := nodeInfo("n2", "a", 100000)
	small.MaxSandboxes = 1
	s.Register(small)
	s.Add("sb-4", spec())
	if _, _, err := s.Reserve("sb-3", "n2", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Reserve("sb-4", "n2", nil); err == nil {
		t.Fatal("reserve beyond max_sandboxes succeeded")
	}
}

func TestReserveRejectsDeadNode(t *testing.T) {
	s, c := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	c.advance(4 * time.Second)
	if r := s.Tick(); !reflect.DeepEqual(r.DeadNodes, []string{"n1"}) {
		t.Fatalf("DeadNodes = %v", r.DeadNodes)
	}
	s.Add("sb-1", spec())
	if _, _, err := s.Reserve("sb-1", "n1", nil); err == nil {
		t.Fatal("reserve on dead node succeeded")
	}
	if _, _, err := s.Reserve("sb-1", "nope", nil); err == nil {
		t.Fatal("reserve on unknown node succeeded")
	}
}

func TestUnreserveFrees(t *testing.T) {
	s, _ := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	s.Add("sb-1", spec())
	inc, _, err := s.Reserve("sb-1", "n1", nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Unreserve("sb-1", inc, "node refused")
	sb := get(t, s, "sb-1")
	if sb.State != gssv1.SandboxState_SANDBOX_STATE_PENDING || sb.Incarnation != 1 || sb.Message != "node refused" {
		t.Fatalf("after Unreserve: %v", sb)
	}
	if n := node(t, s, "n1"); n.Allocated.CpuMillis != 0 || n.Allocated.MemoryBytes != 0 || n.Sandboxes != 0 {
		t.Fatalf("node after Unreserve: %v", n)
	}
	// A second Unreserve at the same incarnation releases nothing more.
	s.Unreserve("sb-1", inc, "again")
	if n := node(t, s, "n1"); n.Allocated.CpuMillis != 0 || n.Sandboxes != 0 {
		t.Fatalf("double Unreserve: %v", n)
	}
}

func TestCommitStale(t *testing.T) {
	s, _ := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	s.Add("sb-1", spec())
	inc, _, err := s.Reserve("sb-1", "n1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if nodeID, existed := s.Destroy("sb-1"); nodeID != "n1" || !existed {
		t.Fatalf("Destroy = %q, %v", nodeID, existed)
	}
	if s.Commit("sb-1", inc, gssv1.StartMode_START_MODE_COLD, time.Second) {
		t.Fatal("stale Commit returned true")
	}
	if sb := get(t, s, "sb-1"); sb.State != gssv1.SandboxState_SANDBOX_STATE_DESTROYED {
		t.Fatalf("state = %v", sb.State)
	}
	if n := node(t, s, "n1"); n.Sandboxes != 0 || n.Allocated.CpuMillis != 0 {
		t.Fatalf("node after stale commit: %v", n)
	}
}

func TestCommitWarmLowersWarmCount(t *testing.T) {
	s, _ := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	s.Heartbeat(&gssv1.HeartbeatRequest{NodeId: "n1", WarmReady: 2})
	warm := func() int {
		nodes, _ := s.View(spec())
		return nodes[0].WarmReady
	}
	for i, want := range []int{1, 0, 0} {
		id := []string{"sb-1", "sb-2", "sb-3"}[i]
		s.Add(id, spec())
		inc, _, err := s.Reserve(id, "n1", nil)
		if err != nil {
			t.Fatal(err)
		}
		if !s.Commit(id, inc, gssv1.StartMode_START_MODE_WARM, 10*time.Millisecond) {
			t.Fatal("Commit false")
		}
		if got := warm(); got != want {
			t.Fatalf("after commit %d: WarmReady %d, want %d", i+1, got, want)
		}
	}
}

// TestDestroyUpTo: a destroy that names an incarnation leaves a sandbox at a
// higher one alone; the same or a higher incarnation, or 0, destroys it.
func TestDestroyUpTo(t *testing.T) {
	s, _ := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	s.Add("sb-1", spec())
	inc, _, err := s.Reserve("sb-1", "n1", nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Unreserve("sb-1", inc, "node create failed") // the incarnation stays at 1
	place2 := func() {
		inc, _, err := s.Reserve("sb-1", "n1", nil)
		if err != nil || !s.Commit("sb-1", inc, gssv1.StartMode_START_MODE_COLD, time.Second) {
			t.Fatalf("second placement: %d, %v", inc, err)
		}
	}
	place2()
	if nodeID, existed := s.DestroyUpTo("sb-1", 1); existed || nodeID != "" {
		t.Errorf("DestroyUpTo(1) of incarnation 2 = %q, %v; want nothing done", nodeID, existed)
	}
	if sb := get(t, s, "sb-1"); sb.State != gssv1.SandboxState_SANDBOX_STATE_READY || sb.Incarnation != 2 {
		t.Fatalf("after DestroyUpTo(1): %v at %d, want READY at 2", sb.State, sb.Incarnation)
	}
	if nodeID, existed := s.DestroyUpTo("sb-1", 2); !existed || nodeID != "n1" {
		t.Errorf("DestroyUpTo(2) = %q, %v; want n1, true", nodeID, existed)
	}
	if sb := get(t, s, "sb-1"); sb.State != gssv1.SandboxState_SANDBOX_STATE_DESTROYED {
		t.Errorf("after DestroyUpTo(2): %v, want DESTROYED", sb.State)
	}
	if n := node(t, s, "n1"); n.Sandboxes != 0 || n.Allocated.CpuMillis != 0 {
		t.Errorf("node after the destroy: %v", n)
	}
	place(t, s, "sb-2", "n1", spec())
	if _, existed := s.DestroyUpTo("sb-2", 7); !existed {
		t.Error("DestroyUpTo(7) of incarnation 1: want it destroyed")
	}
}

func TestDestroyFreesOnce(t *testing.T) {
	s, _ := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	place(t, s, "sb-1", "n1", spec())
	place(t, s, "sb-2", "n1", spec())
	if _, existed := s.Destroy("sb-1"); !existed {
		t.Fatal("first Destroy: existed false")
	}
	if nodeID, existed := s.Destroy("sb-1"); existed || nodeID != "" {
		t.Fatalf("second Destroy = %q, %v", nodeID, existed)
	}
	n := node(t, s, "n1")
	if n.Allocated.CpuMillis != 500 || n.Allocated.MemoryBytes != 256*mib || n.Sandboxes != 1 {
		t.Fatalf("node after double destroy: %v", n)
	}
	if _, existed := s.Destroy("unknown"); existed {
		t.Fatal("unknown Destroy: existed true")
	}
}

func TestNodeTimeout(t *testing.T) {
	s, c := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	place(t, s, "sb-1", "n1", spec())
	place(t, s, "sb-2", "n1", spec())
	s.Heartbeat(&gssv1.HeartbeatRequest{NodeId: "n1", Sandboxes: []*gssv1.NodeSandbox{
		{Id: "sb-1", Spec: spec(), Incarnation: 1}, {Id: "sb-2", Spec: spec(), Incarnation: 1},
	}})
	c.advance(3 * time.Second)
	if r := s.Tick(); len(r.DeadNodes) != 0 || len(r.Lost) != 0 {
		t.Fatalf("at +3.0s: %+v", r)
	}
	c.advance(100 * time.Millisecond)
	r := s.Tick()
	if !reflect.DeepEqual(r.DeadNodes, []string{"n1"}) || !reflect.DeepEqual(r.Lost, []string{"sb-1", "sb-2"}) {
		t.Fatalf("at +3.1s: %+v", r)
	}
	n := node(t, s, "n1")
	if n.State != gssv1.NodeState_NODE_STATE_DEAD || n.Allocated.CpuMillis != 0 || n.Allocated.MemoryBytes != 0 || n.Sandboxes != 0 {
		t.Fatalf("dead node: %v", n)
	}
	if sb := get(t, s, "sb-1"); sb.State != gssv1.SandboxState_SANDBOX_STATE_LOST {
		t.Fatalf("sb-1 state %v", sb.State)
	}
	if r := s.Tick(); len(r.DeadNodes) != 0 || len(r.Lost) != 0 || len(r.Expired) != 0 {
		t.Fatalf("second Tick: %+v", r)
	}
	if nodes, _ := s.View(spec()); nodes[0].Ready {
		t.Fatal("dead node is Ready in View")
	}
}

func TestLostCanBeReservedElsewhere(t *testing.T) {
	s, c := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	s.Register(nodeInfo("n2", "b", 4000))
	place(t, s, "sb-1", "n1", spec())
	c.advance(3100 * time.Millisecond)
	s.Heartbeat(&gssv1.HeartbeatRequest{NodeId: "n2"})
	if r := s.Tick(); !reflect.DeepEqual(r.Lost, []string{"sb-1"}) {
		t.Fatalf("Lost = %v", r.Lost)
	}
	if got := s.NeedsPlacement(); !reflect.DeepEqual(got, []string{"sb-1"}) {
		t.Fatalf("NeedsPlacement = %v", got)
	}
	inc, _, err := s.Reserve("sb-1", "n2", nil)
	if err != nil || inc != 2 {
		t.Fatalf("Reserve = %d, %v", inc, err)
	}
	if !s.Commit("sb-1", inc, gssv1.StartMode_START_MODE_COLD, time.Second) {
		t.Fatal("Commit false")
	}
	if got := s.NeedsPlacement(); len(got) != 0 {
		t.Fatalf("NeedsPlacement after Commit = %v", got)
	}
	sb := get(t, s, "sb-1")
	if sb.NodeId != "n2" || sb.NodeGroup != "b" || sb.State != gssv1.SandboxState_SANDBOX_STATE_READY {
		t.Fatalf("sandbox: %v", sb)
	}
	if n := node(t, s, "n2"); n.Allocated.CpuMillis != 500 || n.Sandboxes != 1 {
		t.Fatalf("n2: %v", n)
	}
}

func TestNeedsPlacementOrder(t *testing.T) {
	s, c := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	s.Add("sb-b", spec())
	c.advance(time.Second)
	s.Add("sb-a", spec())
	s.Add("sb-new", spec()) // PENDING at incarnation 0: not the rescheduler's job
	for _, id := range []string{"sb-a", "sb-b"} {
		inc, _, err := s.Reserve(id, "n1", nil)
		if err != nil {
			t.Fatal(err)
		}
		s.Unreserve(id, inc, "refused")
	}
	if got := s.NeedsPlacement(); !reflect.DeepEqual(got, []string{"sb-b", "sb-a"}) {
		t.Fatalf("NeedsPlacement = %v", got)
	}
}

func TestTTLExpiry(t *testing.T) {
	s, c := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	place(t, s, "sb-1", "n1", specTTL(time.Minute))
	c.advance(61 * time.Second)
	s.Heartbeat(&gssv1.HeartbeatRequest{NodeId: "n1", Sandboxes: []*gssv1.NodeSandbox{
		{Id: "sb-1", Spec: specTTL(time.Minute), Incarnation: 1},
	}})
	r := s.Tick()
	if !reflect.DeepEqual(r.Expired, []Expired{{ID: "sb-1", NodeID: "n1"}}) {
		t.Fatalf("Expired = %+v", r.Expired)
	}
	sb := get(t, s, "sb-1")
	if sb.State != gssv1.SandboxState_SANDBOX_STATE_DESTROYED || sb.Message != "ttl expired" {
		t.Fatalf("sandbox: %v", sb)
	}
	if n := node(t, s, "n1"); n.Allocated.CpuMillis != 0 || n.Sandboxes != 0 {
		t.Fatalf("node: %v", n)
	}
	hb := s.Heartbeat(&gssv1.HeartbeatRequest{NodeId: "n1", Sandboxes: []*gssv1.NodeSandbox{
		{Id: "sb-1", Spec: specTTL(time.Minute), Incarnation: 1},
	}})
	if !reflect.DeepEqual(hb.Destroy, []string{"sb-1"}) {
		t.Fatalf("not tombstoned: Destroy = %v", hb.Destroy)
	}
	if r := s.Tick(); len(r.Expired) != 0 {
		t.Fatalf("expired twice: %+v", r.Expired)
	}
}

func TestRemainingTTL(t *testing.T) {
	s, c := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	s.Add("sb-1", spec())
	c.advance(4 * time.Minute)
	inc, remaining, err := s.Reserve("sb-1", "n1", nil)
	if err != nil || remaining != 6*time.Minute {
		t.Fatalf("Reserve at +4m = %v, %v", remaining, err)
	}
	s.Unreserve("sb-1", inc, "refused")
	c.advance(7 * time.Minute)
	if _, _, err := s.Reserve("sb-1", "n1", nil); err == nil {
		t.Fatal("Reserve at +11m succeeded")
	}
	if n := node(t, s, "n1"); n.Allocated.CpuMillis != 0 || n.Sandboxes != 0 {
		t.Fatalf("failed reserve allocated: %v", n)
	}
}

func TestHealthEWMA(t *testing.T) {
	s, _ := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	health := func() float64 { return node(t, s, "n1").Health }
	steps := []struct {
		ok   bool
		want float64
	}{{false, 0.8}, {false, 0.64}, {true, 0.712}}
	if h := health(); h != 1 {
		t.Fatalf("initial health %v", h)
	}
	for _, st := range steps {
		s.RecordResult("n1", st.ok)
		if h := health(); math.Abs(h-st.want) > 1e-9 {
			t.Fatalf("health %v, want %v", h, st.want)
		}
	}
	s.Register(nodeInfo("n1", "a", 4000)) // re-register keeps health
	if h := health(); math.Abs(h-0.712) > 1e-9 {
		t.Fatalf("health reset on re-register: %v", h)
	}
	s.RecordResult("unknown", false) // no panic
}

func TestViewAffinity(t *testing.T) {
	s, _ := newState()
	s.Register(nodeInfo("b1", "b", 4000))
	s.Register(nodeInfo("a1", "a", 4000))
	keyed := func(k string) *gssv1.SandboxSpec { sp := spec(); sp.AffinityKey = k; return sp }
	place(t, s, "sb-1", "a1", keyed("k"))
	place(t, s, "sb-2", "a1", keyed("k"))
	place(t, s, "sb-3", "b1", keyed("k"))
	place(t, s, "sb-4", "a1", keyed("other"))
	place(t, s, "sb-5", "b1", keyed("k"))
	s.Destroy("sb-5") // not live

	nodes, aff := s.View(keyed("k"))
	if !reflect.DeepEqual(aff, map[string]int{"a": 2, "b": 1}) {
		t.Fatalf("affinity = %v", aff)
	}
	if len(nodes) != 2 || nodes[0].ID != "a1" || nodes[1].ID != "b1" {
		t.Fatalf("nodes = %+v", nodes)
	}
	a := nodes[0]
	if !a.Ready || a.Group != "a" || a.MaxSandboxes != 10 || a.Sandboxes != 3 ||
		a.Capacity.CPUMillis != 4000 || a.Allocated.CPUMillis != 1500 || a.Health != 1 {
		t.Fatalf("a1 view: %+v", a)
	}
	if _, aff := s.View(spec()); aff != nil {
		t.Fatalf("no key: affinity = %v", aff)
	}
}

func TestWaitReady(t *testing.T) {
	s, _ := newState()
	s.Register(nodeInfo("n1", "a", 4000))

	s.Add("sb-1", spec())
	inc, _, err := s.Reserve("sb-1", "n1", nil)
	if err != nil {
		t.Fatal(err)
	}
	type res struct {
		sb  *gssv1.Sandbox
		err error
	}
	done := make(chan res, 1)
	go func() {
		sb, err := s.WaitReady(context.Background(), "sb-1")
		done <- res{sb, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("returned before Commit: %v %v", r.sb, r.err)
	case <-time.After(20 * time.Millisecond):
	}
	s.Commit("sb-1", inc, gssv1.StartMode_START_MODE_COLD, time.Second)
	select {
	case r := <-done:
		if r.err != nil || r.sb.State != gssv1.SandboxState_SANDBOX_STATE_READY {
			t.Fatalf("WaitReady = %v, %v", r.sb, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitReady did not wake on Commit")
	}

	// A blocked waiter wakes with NotFound when the sandbox is destroyed.
	s.Add("sb-2", spec())
	go func() {
		sb, err := s.WaitReady(context.Background(), "sb-2")
		done <- res{sb, err}
	}()
	time.Sleep(10 * time.Millisecond)
	s.Destroy("sb-2")
	select {
	case r := <-done:
		if status.Code(r.err) != codes.NotFound {
			t.Fatalf("after Destroy: %v", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitReady did not wake on Destroy")
	}
	if _, err := s.WaitReady(context.Background(), "unknown"); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown: %v", err)
	}

	s.Add("sb-3", spec())
	s.Fail("sb-3", "no node fits")
	_, err = s.WaitReady(context.Background(), "sb-3")
	if st, _ := status.FromError(err); st.Code() != codes.FailedPrecondition || st.Message() != "no node fits" {
		t.Fatalf("after Fail: %v", err)
	}

	s.Add("sb-4", spec())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := s.WaitReady(ctx, "sb-4"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ctx deadline: %v", err)
	}
}

func TestListSorted(t *testing.T) {
	s, _ := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	for _, id := range []string{"sb-c", "sb-a", "sb-d", "sb-b", "sb-e"} {
		s.Add(id, spec())
	}
	s.Destroy("sb-d")
	s.Fail("sb-e", "boom")
	var ids []string
	for _, sb := range s.List() {
		ids = append(ids, sb.Id)
	}
	if !reflect.DeepEqual(ids, []string{"sb-a", "sb-b", "sb-c"}) {
		t.Fatalf("List = %v", ids)
	}
	// Returned values are copies.
	s.List()[0].Spec.Resources.CpuMillis = 1
	if sb := get(t, s, "sb-a"); sb.Spec.Resources.CpuMillis != 500 {
		t.Fatal("List returned an internal pointer")
	}
	got := get(t, s, "sb-a")
	got.State = gssv1.SandboxState_SANDBOX_STATE_READY
	if sb := get(t, s, "sb-a"); sb.State != gssv1.SandboxState_SANDBOX_STATE_PENDING {
		t.Fatal("Get returned an internal pointer")
	}
	s.Nodes()[0].Info.MaxSandboxes = 0
	if node(t, s, "n1").Info.MaxSandboxes != 10 {
		t.Fatal("Nodes returned an internal pointer")
	}
}

func TestNilSpecIsZeroResources(t *testing.T) {
	s, _ := newState()
	s.Register(&gssv1.NodeInfo{Id: "n1", MaxSandboxes: 1}) // nil capacity
	s.Add("sb-1", nil)
	inc, _, err := s.Reserve("sb-1", "n1", nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Commit("sb-1", inc, gssv1.StartMode_START_MODE_COLD, 0)
	s.View(nil)
	s.Heartbeat(&gssv1.HeartbeatRequest{NodeId: "n1", Sandboxes: []*gssv1.NodeSandbox{nil, {Id: "sb-x"}}})
	s.Destroy("sb-1")
	if n := node(t, s, "n1"); n.Allocated.CpuMillis != 0 {
		t.Fatalf("node: %v", n)
	}
}

func TestFailOnlyFromPendingOrLost(t *testing.T) {
	s, c := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	s.Register(nodeInfo("n2", "a", 4000))

	s.Add("pending", spec())
	if !s.Fail("pending", "no fit") {
		t.Fatal("PENDING: Fail false")
	}
	if sb := get(t, s, "pending"); sb.State != gssv1.SandboxState_SANDBOX_STATE_FAILED {
		t.Fatalf("PENDING: state %v", sb.State)
	}

	s.Add("starting", spec())
	if _, _, err := s.Reserve("starting", "n2", nil); err != nil {
		t.Fatal(err)
	}
	if s.Fail("starting", "stale") {
		t.Fatal("STARTING: Fail true")
	}
	if sb := get(t, s, "starting"); sb.State != gssv1.SandboxState_SANDBOX_STATE_STARTING {
		t.Fatalf("STARTING: state %v", sb.State)
	}
	if n := node(t, s, "n2"); n.Allocated.CpuMillis != 500 || n.Sandboxes != 1 {
		t.Fatalf("STARTING: allocation changed: %v", n)
	}

	place(t, s, "ready", "n2", spec())
	if s.Fail("ready", "stale") {
		t.Fatal("READY: Fail true")
	}
	if sb := get(t, s, "ready"); sb.State != gssv1.SandboxState_SANDBOX_STATE_READY {
		t.Fatalf("READY: state %v", sb.State)
	}

	place(t, s, "lost", "n1", spec())
	c.advance(3100 * time.Millisecond)
	s.Heartbeat(report("n2", reported("starting", 1), reported("ready", 1)))
	s.Tick()
	if !s.Fail("lost", "no fit") {
		t.Fatal("LOST: Fail false")
	}
	if sb := get(t, s, "lost"); sb.State != gssv1.SandboxState_SANDBOX_STATE_FAILED {
		t.Fatalf("LOST: state %v", sb.State)
	}
	if s.Fail("unknown", "x") || s.Fail("lost", "again") {
		t.Fatal("unknown or FAILED: Fail true")
	}
}

func TestNodeTimeoutStarting(t *testing.T) {
	s, c := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	s.Add("sb-1", spec())
	if _, _, err := s.Reserve("sb-1", "n1", nil); err != nil {
		t.Fatal(err)
	}
	c.advance(3100 * time.Millisecond)
	if r := s.Tick(); !reflect.DeepEqual(r.Lost, []string{"sb-1"}) {
		t.Fatalf("Lost = %v", r.Lost)
	}
	if sb := get(t, s, "sb-1"); sb.State != gssv1.SandboxState_SANDBOX_STATE_LOST {
		t.Fatalf("state %v", sb.State)
	}
	if n := node(t, s, "n1"); n.Allocated.CpuMillis != 0 || n.Allocated.MemoryBytes != 0 || n.Sandboxes != 0 {
		t.Fatalf("node: %v", n)
	}
}

func TestTTLExpiryAllStates(t *testing.T) {
	s, c := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	s.Register(nodeInfo("n2", "a", 4000))
	s.Add("pending", specTTL(time.Minute))
	s.Add("starting", specTTL(time.Minute))
	startInc, _, err := s.Reserve("starting", "n2", nil)
	if err != nil {
		t.Fatal(err)
	}
	place(t, s, "lost", "n1", specTTL(time.Minute))
	c.advance(3100 * time.Millisecond)
	s.Heartbeat(report("n2"))
	if r := s.Tick(); !reflect.DeepEqual(r.Lost, []string{"lost"}) {
		t.Fatalf("Lost = %v", r.Lost)
	}
	c.advance(time.Minute)
	s.Heartbeat(report("n2"))
	r := s.Tick()
	want := []Expired{{ID: "lost", NodeID: "n1"}, {ID: "pending"}, {ID: "starting", NodeID: "n2"}}
	if !reflect.DeepEqual(r.Expired, want) {
		t.Fatalf("Expired = %+v", r.Expired)
	}
	for _, id := range []string{"pending", "starting", "lost"} {
		if sb := get(t, s, id); sb.State != gssv1.SandboxState_SANDBOX_STATE_DESTROYED || sb.Message != "ttl expired" {
			t.Fatalf("%s: %v", id, sb)
		}
	}
	if s.Commit("starting", startInc, gssv1.StartMode_START_MODE_COLD, time.Second) {
		t.Fatal("Commit after expiry returned true")
	}
	s.Tick()
	for _, id := range []string{"n1", "n2"} {
		if n := node(t, s, id); n.Allocated.CpuMillis != 0 || n.Allocated.MemoryBytes != 0 || n.Sandboxes != 0 {
			t.Fatalf("%s: %v", id, n)
		}
	}
	// Released exactly once: a sandbox placed afterwards is counted in full.
	place(t, s, "after", "n2", spec())
	if n := node(t, s, "n2"); n.Allocated.CpuMillis != 500 || n.Sandboxes != 1 {
		t.Fatalf("n2 after new placement: %v", n)
	}
}

// lockedClock is a fake clock safe for concurrent use.
type lockedClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *lockedClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *lockedClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// checkAllocation asserts that each node's allocation equals the sum over
// its STARTING and READY sandboxes.
func checkAllocation(t *testing.T, s *State) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	type sum struct {
		cpu, mem int64
		count    int
	}
	want := map[string]sum{}
	for _, e := range s.sandboxes {
		st := e.sb.State
		if st == gssv1.SandboxState_SANDBOX_STATE_STARTING || st == gssv1.SandboxState_SANDBOX_STATE_READY {
			r := resourcesOf(e.sb.Spec)
			w := want[e.sb.NodeId]
			w.cpu += r.CPUMillis
			w.mem += r.MemoryBytes
			w.count++
			want[e.sb.NodeId] = w
		}
	}
	for id, n := range s.nodes {
		w := want[id]
		if n.allocated.CPUMillis != w.cpu || n.allocated.MemoryBytes != w.mem || n.count != w.count {
			t.Fatalf("node %s: allocated %+v count %d, sandboxes sum %+v", id, n.allocated, n.count, w)
		}
	}
}

func TestConcurrentInvariant(t *testing.T) {
	c := &lockedClock{t: time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)}
	s := New(DefaultConfig(), c.now)
	nodes := []string{"n1", "n2"}
	for _, id := range nodes {
		s.Register(nodeInfo(id, "a", 4000))
	}
	const workers, ops = 8, 500
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			id := func() string { return "sb-" + strconv.Itoa(rng.Intn(40)) }
			for i := 0; i < ops; i++ {
				nodeID := nodes[rng.Intn(len(nodes))]
				switch rng.Intn(8) {
				case 0:
					s.Add(id(), spec())
				case 1, 2:
					sbID := id()
					inc, _, err := s.Reserve(sbID, nodeID, nil)
					if err != nil {
						continue
					}
					switch rng.Intn(3) {
					case 0:
						s.Unreserve(sbID, inc, "refused")
					default:
						s.Commit(sbID, inc, gssv1.StartMode_START_MODE_WARM, time.Millisecond)
					}
				case 3:
					s.Destroy(id())
				case 4:
					var rep []*gssv1.NodeSandbox
					for _, sb := range s.List() {
						if sb.NodeId == nodeID && rng.Intn(2) == 0 {
							rep = append(rep, &gssv1.NodeSandbox{Id: sb.Id, Spec: sb.Spec, Incarnation: sb.Incarnation})
						}
					}
					rep = append(rep, &gssv1.NodeSandbox{Id: "adopt-" + strconv.Itoa(rng.Intn(10)), Spec: spec(), Incarnation: 1})
					s.Heartbeat(&gssv1.HeartbeatRequest{NodeId: nodeID, Sandboxes: rep, WarmReady: 1})
				case 5:
					s.Tick()
				case 6:
					s.Fail(id(), "gave up")
				case 7:
					c.advance(time.Duration(rng.Intn(500)) * time.Millisecond)
				}
			}
		}(w)
	}
	wg.Wait()
	checkAllocation(t, s)
	for _, sb := range s.List() {
		s.Destroy(sb.Id)
	}
	checkAllocation(t, s)
	for _, n := range s.Nodes() {
		if n.Allocated.CpuMillis != 0 || n.Allocated.MemoryBytes != 0 || n.Sandboxes != 0 {
			t.Fatalf("after destroying all: %v", n)
		}
	}
}
