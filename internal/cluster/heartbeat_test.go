package cluster

import (
	"reflect"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
)

func report(nodeID string, sbs ...*gssv1.NodeSandbox) *gssv1.HeartbeatRequest {
	return &gssv1.HeartbeatRequest{NodeId: nodeID, Sandboxes: sbs}
}

func reported(id string, inc int32) *gssv1.NodeSandbox {
	return &gssv1.NodeSandbox{Id: id, Spec: spec(), Incarnation: inc, StartedAs: gssv1.StartMode_START_MODE_COLD}
}

func TestHeartbeatUnknownNode(t *testing.T) {
	s, _ := newState()
	if r := s.Heartbeat(report("n1")); r.Known {
		t.Fatal("unknown node is Known")
	}
	if _, ok := s.NodeAddress("n1"); ok {
		t.Fatal("unknown node has an address")
	}
	s.Register(nodeInfo("n1", "a", 4000))
	if r := s.Heartbeat(report("n1")); !r.Known {
		t.Fatal("registered node not Known")
	}
	if addr, ok := s.NodeAddress("n1"); !ok || addr != "n1:7000" {
		t.Fatalf("NodeAddress = %q, %v", addr, ok)
	}
}

func TestHeartbeatAdopts(t *testing.T) {
	s, c := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	ns := &gssv1.NodeSandbox{Id: "sb-x", Spec: specTTL(5 * time.Minute), Incarnation: 3, StartedAs: gssv1.StartMode_START_MODE_RESTORE}
	r := s.Heartbeat(&gssv1.HeartbeatRequest{NodeId: "n1", Sandboxes: []*gssv1.NodeSandbox{ns}, WarmReady: 4})
	if !r.Known || len(r.Destroy) != 0 || len(r.Lost) != 0 {
		t.Fatalf("Heartbeat = %+v", r)
	}
	sb := get(t, s, "sb-x")
	if sb.State != gssv1.SandboxState_SANDBOX_STATE_READY || sb.NodeId != "n1" || sb.NodeGroup != "a" ||
		sb.Incarnation != 3 || sb.StartedAs != gssv1.StartMode_START_MODE_RESTORE ||
		!sb.ExpiresAt.AsTime().Equal(c.now().Add(5*time.Minute)) {
		t.Fatalf("adopted: %v", sb)
	}
	n := node(t, s, "n1")
	if n.Allocated.CpuMillis != 500 || n.Allocated.MemoryBytes != 256*mib || n.Sandboxes != 1 || n.WarmReady != 4 {
		t.Fatalf("node: %v", n)
	}
	// The same report again changes nothing.
	s.Heartbeat(&gssv1.HeartbeatRequest{NodeId: "n1", Sandboxes: []*gssv1.NodeSandbox{ns}})
	if n := node(t, s, "n1"); n.Allocated.CpuMillis != 500 || n.Sandboxes != 1 {
		t.Fatalf("node after repeat: %v", n)
	}
	// Mutating the request afterwards does not reach the state.
	ns.Spec.Resources.CpuMillis = 1
	if sb := get(t, s, "sb-x"); sb.Spec.Resources.CpuMillis != 500 {
		t.Fatal("adopted spec aliases the request")
	}
}

func TestHeartbeatTombstoned(t *testing.T) {
	s, _ := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	place(t, s, "sb-1", "n1", spec())
	s.Destroy("sb-1")
	r := s.Heartbeat(report("n1", reported("sb-1", 1)))
	if !reflect.DeepEqual(r.Destroy, []string{"sb-1"}) {
		t.Fatalf("Destroy = %v", r.Destroy)
	}
	if sb := get(t, s, "sb-1"); sb.State != gssv1.SandboxState_SANDBOX_STATE_DESTROYED {
		t.Fatalf("adopted a tombstoned sandbox: %v", sb)
	}
	if n := node(t, s, "n1"); n.Allocated.CpuMillis != 0 || n.Sandboxes != 0 {
		t.Fatalf("node: %v", n)
	}
}

func TestHeartbeatMovedSandbox(t *testing.T) {
	s, _ := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	s.Register(nodeInfo("n2", "a", 4000))
	place(t, s, "sb-1", "n1", spec())
	s.Heartbeat(report("n1"))
	if r := s.Heartbeat(report("n1")); !reflect.DeepEqual(r.Lost, []string{"sb-1"}) {
		t.Fatalf("Lost = %v", r.Lost)
	}
	inc, _, err := s.Reserve("sb-1", "n2", nil)
	if err != nil || inc != 2 {
		t.Fatalf("Reserve = %d, %v", inc, err)
	}
	s.Commit("sb-1", inc, gssv1.StartMode_START_MODE_COLD, time.Second)
	r := s.Heartbeat(report("n1", reported("sb-1", 1)))
	if !reflect.DeepEqual(r.Destroy, []string{"sb-1"}) {
		t.Fatalf("Destroy = %v", r.Destroy)
	}
	sb := get(t, s, "sb-1")
	if sb.NodeId != "n2" || sb.Incarnation != 2 || sb.State != gssv1.SandboxState_SANDBOX_STATE_READY {
		t.Fatalf("sandbox: %v", sb)
	}
	if n := node(t, s, "n1"); n.Allocated.CpuMillis != 0 || n.Sandboxes != 0 {
		t.Fatalf("n1: %v", n)
	}
	// n2 reporting the same ID at the old incarnation is told to destroy it as well.
	if r := s.Heartbeat(report("n2", reported("sb-1", 1))); !reflect.DeepEqual(r.Destroy, []string{"sb-1"}) {
		t.Fatalf("n2 old incarnation: Destroy = %v", r.Destroy)
	}
}

func TestHeartbeatMissing(t *testing.T) {
	s, _ := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	place(t, s, "sb-1", "n1", spec())
	place(t, s, "sb-2", "n1", spec())
	r := s.Heartbeat(report("n1", reported("sb-2", 1)))
	if len(r.Lost) != 0 || get(t, s, "sb-1").State != gssv1.SandboxState_SANDBOX_STATE_READY {
		t.Fatalf("one miss: %+v", r)
	}
	// Being reported again resets the counter.
	s.Heartbeat(report("n1", reported("sb-1", 1), reported("sb-2", 1)))
	s.Heartbeat(report("n1", reported("sb-2", 1)))
	if get(t, s, "sb-1").State != gssv1.SandboxState_SANDBOX_STATE_READY {
		t.Fatal("miss counter not reset")
	}
	r = s.Heartbeat(report("n1", reported("sb-2", 1)))
	if !reflect.DeepEqual(r.Lost, []string{"sb-1"}) {
		t.Fatalf("Lost = %v", r.Lost)
	}
	if sb := get(t, s, "sb-1"); sb.State != gssv1.SandboxState_SANDBOX_STATE_LOST {
		t.Fatalf("state %v", sb.State)
	}
	if n := node(t, s, "n1"); n.Allocated.CpuMillis != 500 || n.Sandboxes != 1 {
		t.Fatalf("node: %v", n)
	}
	if got := s.NeedsPlacement(); !reflect.DeepEqual(got, []string{"sb-1"}) {
		t.Fatalf("NeedsPlacement = %v", got)
	}
}

func TestHeartbeatStartingNotMissing(t *testing.T) {
	s, _ := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	s.Add("sb-1", spec())
	if _, _, err := s.Reserve("sb-1", "n1", nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if r := s.Heartbeat(report("n1")); len(r.Lost) != 0 || len(r.Destroy) != 0 {
			t.Fatalf("heartbeat %d: %+v", i, r)
		}
	}
	if sb := get(t, s, "sb-1"); sb.State != gssv1.SandboxState_SANDBOX_STATE_STARTING {
		t.Fatalf("state %v", sb.State)
	}
	if n := node(t, s, "n1"); n.Sandboxes != 1 {
		t.Fatalf("node: %v", n)
	}
}

func TestDeadNodeReturns(t *testing.T) {
	s, c := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	s.Register(nodeInfo("n2", "a", 4000))
	place(t, s, "sb-1", "n1", spec())
	c.advance(3100 * time.Millisecond)
	s.Heartbeat(report("n2"))
	if r := s.Tick(); !reflect.DeepEqual(r.DeadNodes, []string{"n1"}) {
		t.Fatalf("DeadNodes = %v", r.DeadNodes)
	}
	inc, _, err := s.Reserve("sb-1", "n2", nil)
	if err != nil || inc != 2 {
		t.Fatalf("Reserve = %d, %v", inc, err)
	}
	s.Commit("sb-1", inc, gssv1.StartMode_START_MODE_COLD, time.Second)

	r := s.Heartbeat(report("n1", reported("sb-1", 1)))
	if !r.Known || !reflect.DeepEqual(r.Destroy, []string{"sb-1"}) {
		t.Fatalf("Heartbeat = %+v", r)
	}
	n := node(t, s, "n1")
	if n.State != gssv1.NodeState_NODE_STATE_READY || n.Allocated.CpuMillis != 0 || n.Allocated.MemoryBytes != 0 || n.Sandboxes != 0 {
		t.Fatalf("n1: %v", n)
	}
	if !n.LastHeartbeat.AsTime().Equal(c.now()) {
		t.Fatalf("last heartbeat %v", n.LastHeartbeat.AsTime())
	}
	if n2 := node(t, s, "n2"); n2.Allocated.CpuMillis != 500 || n2.Sandboxes != 1 {
		t.Fatalf("n2: %v", n2)
	}
}

func TestTombstoneExpiry(t *testing.T) {
	s, c := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	place(t, s, "sb-1", "n1", spec())
	s.Destroy("sb-1")
	c.advance(9 * time.Minute)
	s.Tick()
	if r := s.Heartbeat(report("n1", reported("sb-1", 1))); !reflect.DeepEqual(r.Destroy, []string{"sb-1"}) {
		t.Fatalf("at +9m: Destroy = %v", r.Destroy)
	}
	c.advance(time.Minute + time.Second)
	s.Tick()
	if _, ok := s.Get("sb-1"); ok {
		t.Fatal("destroyed sandbox still in table after tombstone expiry")
	}
	r := s.Heartbeat(report("n1", reported("sb-1", 1)))
	if len(r.Destroy) != 0 {
		t.Fatalf("after 10m: Destroy = %v", r.Destroy)
	}
	if sb := get(t, s, "sb-1"); sb.State != gssv1.SandboxState_SANDBOX_STATE_READY {
		t.Fatalf("not adopted: %v", sb)
	}
}

func TestHeartbeatRestoresLost(t *testing.T) {
	s, c := newState()
	s.Register(nodeInfo("n1", "a", 4000))
	place(t, s, "sb-1", "n1", spec())
	c.advance(3100 * time.Millisecond)
	s.Tick() // n1 dead, sb-1 LOST, not yet rescheduled
	r := s.Heartbeat(report("n1", reported("sb-1", 1)))
	if len(r.Destroy) != 0 {
		t.Fatalf("Destroy = %v", r.Destroy)
	}
	if sb := get(t, s, "sb-1"); sb.State != gssv1.SandboxState_SANDBOX_STATE_READY || sb.Incarnation != 1 {
		t.Fatalf("sandbox: %v", sb)
	}
	if n := node(t, s, "n1"); n.Allocated.CpuMillis != 500 || n.Sandboxes != 1 {
		t.Fatalf("node: %v", n)
	}
	if got := s.NeedsPlacement(); len(got) != 0 {
		t.Fatalf("NeedsPlacement = %v", got)
	}
}
