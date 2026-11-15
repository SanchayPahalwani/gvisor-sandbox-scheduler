package gateway

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func isState(st gssv1.SandboxState, want ...gssv1.SandboxState) bool {
	return slices.Contains(want, st)
}

func TestNodeDeathReschedules(t *testing.T) {
	e := newEnv(t, nil).run()
	e.register("n1")
	e.register("n2")
	a, b := e.mustCreate(nil), e.mustCreate(nil)
	if a.GetNodeId() != "n1" || b.GetNodeId() != "n1" {
		t.Fatalf("placed on %s and %s, want both on n1 (bin-packing)", a.GetNodeId(), b.GetNodeId())
	}
	e.kill("n2")
	for _, sb := range []*gssv1.Sandbox{a, b} {
		eventually(t, sb.GetId()+" READY on n2", func() bool {
			got := e.get(sb.GetId())
			return got.GetState() == gssv1.SandboxState_SANDBOX_STATE_READY && got.GetNodeId() == "n2"
		})
		if got := e.get(sb.GetId()); got.GetIncarnation() != 2 || got.GetSpec().GetResources().GetCpuMillis() != 500 {
			t.Errorf("%s = %v, want incarnation 2 with the same spec", sb.GetId(), got)
		}
	}
	var onN2 []string
	for _, c := range e.nodes.createCalls() {
		if c.addr == addr("n2") {
			if c.req.GetIncarnation() != 2 {
				t.Errorf("Create on n2 for %s at incarnation %d, want 2", c.req.GetSandboxId(), c.req.GetIncarnation())
			}
			onN2 = append(onN2, c.req.GetSandboxId())
		}
	}
	slices.Sort(onN2)
	if want := []string{a.GetId(), b.GetId()}; !slices.Equal(onN2, want) {
		t.Errorf("Creates on n2 for %q, want %q", onN2, want)
	}
	if n := e.node("n1"); n.GetState() != gssv1.NodeState_NODE_STATE_DEAD || n.GetSandboxes() != 0 {
		t.Errorf("n1 = %v, want DEAD with nothing allocated", n)
	}
	if n := e.node("n2"); n.GetSandboxes() != 2 {
		t.Errorf("n2 holds %d sandboxes, want 2", n.GetSandboxes())
	}
}

func TestRescheduleConcurrencyCap(t *testing.T) {
	e := newEnv(t, nil).run()
	e.registerNode(nodeInfo("n1", "g1", 64000))
	e.registerNode(nodeInfo("n2", "g1", 64000))
	var ids []string
	for range 30 {
		sb := e.mustCreate(spec(500, 256*mib))
		if sb.GetNodeId() != "n1" {
			t.Fatalf("placed on %s, want n1", sb.GetNodeId())
		}
		ids = append(ids, sb.GetId())
	}
	release := make(chan struct{})
	e.nodes.setCreate(func(ctx context.Context, a string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &gssv1.NodeCreateResponse{StartedAs: gssv1.StartMode_START_MODE_COLD}, nil
	})
	e.nodes.mu.Lock()
	e.nodes.maxInflight = 0
	e.nodes.mu.Unlock()
	e.kill("n2")
	inflight := func() int {
		e.nodes.mu.Lock()
		defer e.nodes.mu.Unlock()
		return e.nodes.inflight
	}
	eventually(t, "8 Creates in flight", func() bool { return inflight() == 8 })
	time.Sleep(100 * time.Millisecond) // several retry intervals: none may add a ninth
	e.nodes.mu.Lock()
	peak := e.nodes.maxInflight
	e.nodes.mu.Unlock()
	if peak != 8 {
		t.Errorf("at most %d Creates in flight, want 8", peak)
	}
	close(release)
	for _, id := range ids {
		eventually(t, id+" READY on n2", func() bool {
			got := e.get(id)
			return got.GetState() == gssv1.SandboxState_SANDBOX_STATE_READY && got.GetNodeId() == "n2"
		})
	}
	// Never two placements of one sandbox: each was started once on n2.
	seen := map[string]int{}
	for _, c := range e.nodes.createCalls() {
		if c.addr == addr("n2") {
			seen[c.req.GetSandboxId()]++
		}
	}
	for _, id := range ids {
		if seen[id] != 1 {
			t.Errorf("%s started %d times on n2, want once", id, seen[id])
		}
	}
}

func TestRescheduleBlockedByGroup(t *testing.T) {
	e := newEnv(t, nil).run()
	e.registerNode(nodeInfo("na", "a", 4000))
	e.registerNode(nodeInfo("nb", "b", 4000))
	sb := e.mustCreate(&gssv1.SandboxSpec{NodeGroup: "b"})
	if sb.GetNodeId() != "nb" {
		t.Fatalf("placed on %s, want nb", sb.GetNodeId())
	}
	e.kill("na")
	eventually(t, "the sandbox to be LOST", func() bool {
		return e.get(sb.GetId()).GetState() == gssv1.SandboxState_SANDBOX_STATE_LOST
	})
	time.Sleep(10 * e.srv.cfg.RetryInterval)
	if st := e.get(sb.GetId()).GetState(); !isState(st, gssv1.SandboxState_SANDBOX_STATE_LOST, gssv1.SandboxState_SANDBOX_STATE_PENDING) {
		t.Errorf("state = %v, want LOST or PENDING", st)
	}
	for _, c := range e.nodes.createCalls() {
		if c.addr == addr("na") {
			t.Errorf("Create on na: %v", c.req)
		}
	}
	// It is still being retried: once group b is back it is placed there.
	if _, err := e.cc.Heartbeat(ctx(t), &gssv1.HeartbeatRequest{NodeId: "nb"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the sandbox READY on nb again", func() bool {
		got := e.get(sb.GetId())
		return got.GetState() == gssv1.SandboxState_SANDBOX_STATE_READY && got.GetNodeId() == "nb" && got.GetIncarnation() == 2
	})
}

func TestRescheduleWhenCapacityAppears(t *testing.T) {
	const retry = 200 * time.Millisecond
	e := newEnv(t, func(c *Config) { c.RetryInterval = retry }).run()
	e.register("n1")
	sb := e.mustCreate(nil)
	e.kill()
	eventually(t, "the sandbox to be LOST", func() bool {
		return e.get(sb.GetId()).GetState() == gssv1.SandboxState_SANDBOX_STATE_LOST
	})
	time.Sleep(2 * retry) // retried at least once with no node to go to
	begin := time.Now()
	e.register("n2")
	eventually(t, "the sandbox READY on n2", func() bool {
		got := e.get(sb.GetId())
		return got.GetState() == gssv1.SandboxState_SANDBOX_STATE_READY && got.GetNodeId() == "n2"
	})
	if d := time.Since(begin); d > retry+300*time.Millisecond {
		t.Errorf("placed %v after the node registered, want within one retry interval (%v)", d, retry)
	}
}

func TestExpiredSandboxDestroyedOnNode(t *testing.T) {
	e := newEnv(t, nil).run()
	e.register("n1")
	sb := e.mustCreate(&gssv1.SandboxSpec{Ttl: durationpb.New(time.Minute)})
	e.clock.advance(61 * time.Second)
	want := addr("n1") + " " + sb.GetId() + " 0"
	eventually(t, "the node Destroy of the expired sandbox", func() bool {
		return slices.Contains(e.nodes.destroyCalls(), want)
	})
	if got := e.get(sb.GetId()); got.GetState() != gssv1.SandboxState_SANDBOX_STATE_DESTROYED {
		t.Errorf("state = %v, want DESTROYED", got.GetState())
	}
}

func TestReturningNodeToldToDestroy(t *testing.T) {
	e := newEnv(t, nil).run()
	e.register("n1")
	e.register("n2")
	sb := e.mustCreate(nil)
	e.kill("n2")
	eventually(t, "the sandbox READY on n2", func() bool {
		got := e.get(sb.GetId())
		return got.GetState() == gssv1.SandboxState_SANDBOX_STATE_READY && got.GetNodeId() == "n2"
	})
	resp, err := e.cc.Heartbeat(ctx(t), &gssv1.HeartbeatRequest{NodeId: "n1", Sandboxes: []*gssv1.NodeSandbox{
		{Id: sb.GetId(), Spec: sb.GetSpec(), Incarnation: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(resp.GetDestroySandboxIds(), []string{sb.GetId()}) {
		t.Errorf("destroy list = %q, want [%s]", resp.GetDestroySandboxIds(), sb.GetId())
	}
	if n := e.node("n1"); n.GetState() != gssv1.NodeState_NODE_STATE_READY || n.GetSandboxes() != 0 {
		t.Errorf("n1 = %v, want READY again with nothing allocated", n)
	}
	if got := e.get(sb.GetId()); got.GetNodeId() != "n2" || got.GetIncarnation() != 2 {
		t.Errorf("sandbox on %s at %d, want n2 at 2", got.GetNodeId(), got.GetIncarnation())
	}
}

func TestRescheduleSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	e := newEnv(t, nil).run()
	e.register("n1")
	e.register("n2")
	sb := e.mustCreate(nil)
	e.kill("n2")
	eventually(t, "the sandbox READY on n2", func() bool {
		got := e.get(sb.GetId())
		return got.GetState() == gssv1.SandboxState_SANDBOX_STATE_READY && got.GetNodeId() == "n2"
	})
	var resched sdktrace.ReadOnlySpan
	eventually(t, "the gateway.reschedule span", func() bool {
		for _, s := range sr.Ended() {
			if s.Name() == "gateway.reschedule" {
				resched = s
				return true
			}
		}
		return false
	})
	attrs := map[attribute.Key]attribute.Value{}
	for _, kv := range resched.Attributes() {
		attrs[kv.Key] = kv.Value
	}
	if attrs["incarnation"].AsInt64() != 2 || attrs["sandbox.id"].AsString() != sb.GetId() || attrs["node.id"].AsString() != "n2" {
		t.Errorf("gateway.reschedule attributes = %v, want incarnation 2 of %s on n2", resched.Attributes(), sb.GetId())
	}
	var placed bool
	for _, s := range sr.Ended() {
		if s.Name() == "sched.place" && s.Parent().SpanID() == resched.SpanContext().SpanID() {
			placed = true
		}
	}
	if !placed {
		t.Error("no sched.place span under gateway.reschedule")
	}
}

// TestDestroyDuringReschedule: the client destroys a sandbox while its
// reschedule is starting it on another node. The new copy is destroyed by
// its incarnation and nothing places the sandbox again.
func TestDestroyDuringReschedule(t *testing.T) {
	e := newEnv(t, nil).run()
	e.register("n1")
	e.register("n2")
	sb := e.mustCreate(nil)
	var derr error
	e.nodes.setCreate(func(ctx context.Context, a string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
		_, derr = e.srv.DestroySandbox(context.Background(), &gssv1.DestroySandboxRequest{SandboxId: req.GetSandboxId()})
		return &gssv1.NodeCreateResponse{StartedAs: gssv1.StartMode_START_MODE_COLD}, nil
	})
	e.kill("n2")
	want := []string{addr("n2") + " " + sb.GetId() + " 0", addr("n2") + " " + sb.GetId() + " 2"}
	eventually(t, "both node Destroys on n2", func() bool { return slices.Equal(e.nodes.destroyCalls(), want) })
	if derr != nil {
		t.Fatalf("DestroySandbox during the reschedule: %v", derr)
	}
	time.Sleep(5 * e.srv.cfg.RetryInterval)
	if got := e.get(sb.GetId()); got.GetState() != gssv1.SandboxState_SANDBOX_STATE_DESTROYED {
		t.Errorf("state = %v, want DESTROYED", got.GetState())
	}
	if n := e.node("n2"); n.GetAllocated().GetCpuMillis() != 0 || n.GetSandboxes() != 0 {
		t.Errorf("n2 allocation = %v, %d sandboxes; want none", n.GetAllocated(), n.GetSandboxes())
	}
	var onN2 int
	for _, c := range e.nodes.createCalls() {
		if c.addr == addr("n2") {
			onN2++
		}
	}
	if onN2 != 1 {
		t.Errorf("%d Creates on n2, want 1: a destroyed sandbox is not placed again", onN2)
	}
}

// drainEnv is a running gateway with a sandbox on n1 whose reschedule to n2
// is in flight: n2's Create waits for release (or for its context to end).
type drainEnv struct {
	*env
	sb       *gssv1.Sandbox
	release  chan struct{}
	createOK chan error // what each Create after setup saw, the reschedule's first
	cancel   context.CancelFunc
	done     chan struct{} // closed when Run returned
}

func newDrainEnv(t *testing.T) *drainEnv {
	e := newEnv(t, nil)
	d := &drainEnv{env: e, release: make(chan struct{}), createOK: make(chan error, 4), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	go func() {
		e.srv.Run(ctx)
		close(d.done)
	}()
	t.Cleanup(func() {
		cancel()
		<-d.done
	})
	e.register("n1")
	e.register("n2")
	d.sb = e.mustCreate(nil)
	started := make(chan struct{})
	var once sync.Once
	e.nodes.setCreate(func(ctx context.Context, a string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
		once.Do(func() { close(started) })
		select {
		case <-d.release:
			d.createOK <- nil
			return &gssv1.NodeCreateResponse{StartedAs: gssv1.StartMode_START_MODE_COLD}, nil
		case <-ctx.Done():
			d.createOK <- ctx.Err()
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	})
	e.kill("n2")
	<-started
	return d
}

// TestDrainLetsReschedulesFinish: Drain stops failure detection and new
// reschedules at once, while a reschedule already running finishes with its
// normal result.
func TestDrainLetsReschedulesFinish(t *testing.T) {
	d := newDrainEnv(t)
	drained := d.srv.Drain()
	// No heartbeat gets through while a server drains: nothing may be declared dead.
	d.clock.advance(10 * time.Second)
	time.Sleep(10 * d.srv.cfg.TickInterval)
	if n := d.node("n2"); n.GetState() != gssv1.NodeState_NODE_STATE_READY {
		t.Errorf("n2 is %v during the drain, want READY", n.GetState())
	}
	select {
	case <-drained:
		t.Fatal("Drain's channel closed with a reschedule still running")
	default:
	}
	close(d.release)
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("Drain's channel not closed after the reschedule finished")
	}
	if err := <-d.createOK; err != nil {
		t.Errorf("the reschedule's node Create saw %v, want it left to finish", err)
	}
	if got := d.get(d.sb.GetId()); got.GetState() != gssv1.SandboxState_SANDBOX_STATE_READY || got.GetNodeId() != "n2" {
		t.Errorf("sandbox = %v, want READY on n2", got)
	}
	// Nothing new is started during the drain: a sandbox lost now waits.
	other := d.mustCreate(nil) // n2, the only node left
	d.clock.advance(5 * time.Second)
	d.state.Tick() // as if the detector had run once more: n2 dead, other LOST
	if _, err := d.cc.Register(ctx(t), &gssv1.RegisterRequest{Node: nodeInfo("n3", "g1", 4000)}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * d.srv.cfg.RetryInterval)
	if got := d.get(other.GetId()); got.GetState() != gssv1.SandboxState_SANDBOX_STATE_LOST {
		t.Errorf("sandbox lost during the drain is %v, want still LOST (no reschedule starts)", got.GetState())
	}
	d.cancel()
	<-d.done
}

// TestDrainThenCancel: cancelling Run's context after Drain (the drain
// timeout passed) ends the reschedules still running, and Run returns.
func TestDrainThenCancel(t *testing.T) {
	d := newDrainEnv(t)
	drained := d.srv.Drain()
	d.cancel()
	select {
	case <-d.done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its context ended")
	}
	if err := <-d.createOK; !errors.Is(err, context.Canceled) {
		t.Errorf("the reschedule's node Create saw %v, want it cancelled", err)
	}
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Error("Drain's channel not closed after the cancelled reschedule ended")
	}
}
