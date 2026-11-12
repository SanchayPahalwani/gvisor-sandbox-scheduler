package gateway

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/cluster"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/nodeagent"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/sched"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	mib = int64(1 << 20)
	gib = int64(1 << 30)
)

var t0 = time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)

// clock is the cluster state's clock; tests move it by hand.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func addr(nodeID string) string { return nodeID + ":7101" }

type createCall struct {
	addr string
	req  *gssv1.NodeCreateRequest
}

// fakeNodes is a NodeClient that records every call. By default Create
// succeeds as a cold start, Exec ends at once and Destroy succeeds.
type fakeNodes struct {
	mu          sync.Mutex
	creates     []createCall
	destroys    []string // "<addr> <id> <incarnation>"
	execs       []string // "<addr> <id>"
	createFn    func(ctx context.Context, addr string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error)
	execFn      func(ctx context.Context, addr string, req *gssv1.ExecRequest) (grpc.ServerStreamingClient[gssv1.ExecEvent], error)
	destroyErr  error
	forgets     []string // addresses whose connection was dropped
	inflight    int      // Creates running now
	maxInflight int
}

func (f *fakeNodes) Forget(addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forgets = append(f.forgets, addr)
}

func (f *fakeNodes) Create(ctx context.Context, addr string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
	f.mu.Lock()
	f.creates = append(f.creates, createCall{addr, proto.Clone(req).(*gssv1.NodeCreateRequest)})
	f.inflight++
	f.maxInflight = max(f.maxInflight, f.inflight)
	fn := f.createFn
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.inflight--
		f.mu.Unlock()
	}()
	if fn != nil {
		return fn(ctx, addr, req)
	}
	return &gssv1.NodeCreateResponse{StartedAs: gssv1.StartMode_START_MODE_COLD, ReadyLatency: durationpb.New(40 * time.Millisecond)}, nil
}

func (f *fakeNodes) Exec(ctx context.Context, addr string, req *gssv1.ExecRequest) (grpc.ServerStreamingClient[gssv1.ExecEvent], error) {
	f.mu.Lock()
	f.execs = append(f.execs, addr+" "+req.GetSandboxId())
	fn := f.execFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, addr, req)
	}
	return &fakeStream{ctx: ctx}, nil
}

func (f *fakeNodes) Destroy(ctx context.Context, addr, id string, incarnation int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.destroys = append(f.destroys, fmt.Sprintf("%s %s %d", addr, id, incarnation))
	return f.destroyErr
}

func (f *fakeNodes) setCreate(fn func(ctx context.Context, addr string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createFn = fn
}

func (f *fakeNodes) setExec(fn func(ctx context.Context, addr string, req *gssv1.ExecRequest) (grpc.ServerStreamingClient[gssv1.ExecEvent], error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execFn = fn
}

func (f *fakeNodes) createCalls() []createCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.creates)
}

func (f *fakeNodes) destroyCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.destroys)
}

func (f *fakeNodes) execCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.execs)
}

// fakeStream is a node's Exec stream: it returns events, then end (io.EOF
// when nil). With hold it blocks after the events until ctx ends.
type fakeStream struct {
	grpc.ClientStream // only Recv is used
	ctx               context.Context
	events            []*gssv1.ExecEvent
	end               error
	hold              bool
}

func (f *fakeStream) Recv() (*gssv1.ExecEvent, error) {
	if len(f.events) > 0 {
		ev := f.events[0]
		f.events = f.events[1:]
		return ev, nil
	}
	if f.hold {
		<-f.ctx.Done()
		return nil, status.FromContextError(f.ctx.Err()).Err()
	}
	if f.end != nil {
		return nil, f.end
	}
	return nil, io.EOF
}

func stdout(s string) *gssv1.ExecEvent {
	return &gssv1.ExecEvent{Event: &gssv1.ExecEvent_Stdout{Stdout: []byte(s)}}
}

func stderr(s string) *gssv1.ExecEvent {
	return &gssv1.ExecEvent{Event: &gssv1.ExecEvent_Stderr{Stderr: []byte(s)}}
}

func exit(code int32) *gssv1.ExecEvent {
	return &gssv1.ExecEvent{Event: &gssv1.ExecEvent_Exit{Exit: &gssv1.ExitStatus{Code: code}}}
}

type env struct {
	t     *testing.T
	clock *clock
	state *cluster.State
	nodes *fakeNodes
	srv   *Server
	sc    gssv1.SandboxServiceClient
	cc    gssv1.ClusterServiceClient
	ids   atomic.Int64
}

// newEnv returns a gateway on a fake clock and fake nodes, served over an
// in-memory connection, with sandbox IDs sb-000000000001, sb-000000000002...
// Run is not started; see run. mod may change the config.
func newEnv(t *testing.T, mod func(*Config)) *env {
	t.Helper()
	c := &clock{t: t0}
	st := cluster.New(cluster.DefaultConfig(), c.now)
	cfg := DefaultConfig()
	cfg.TickInterval = 5 * time.Millisecond
	cfg.RetryInterval = 20 * time.Millisecond
	if mod != nil {
		mod(&cfg)
	}
	e := &env{t: t, clock: c, state: st, nodes: &fakeNodes{}}
	e.srv = New(st, sched.DefaultPolicy(sched.DefaultWeights), e.nodes, cfg)
	e.srv.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	e.srv.newID = func() (string, error) { return fmt.Sprintf("sb-%012d", e.ids.Add(1)), nil }

	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer(ServerOptions()...)
	gssv1.RegisterSandboxServiceServer(g, e.srv)
	gssv1.RegisterClusterServiceServer(g, e.srv)
	go func() { _ = g.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		g.Stop()
	})
	e.sc = gssv1.NewSandboxServiceClient(conn)
	e.cc = gssv1.NewClusterServiceClient(conn)
	return e
}

// run starts Run until the test ends.
func (e *env) run() *env {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		e.srv.Run(ctx)
		close(done)
	}()
	e.t.Cleanup(func() {
		cancel()
		<-done
	})
	return e
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func nodeInfo(id, group string, cpu int64) *gssv1.NodeInfo {
	return &gssv1.NodeInfo{
		Id: id, Group: group, Address: addr(id),
		Capacity:     &gssv1.Resources{CpuMillis: cpu, MemoryBytes: 8 * gib},
		MaxSandboxes: 100,
	}
}

// register registers node id in group g1 with 4000m and 8 GiB.
func (e *env) register(id string) { e.registerNode(nodeInfo(id, "g1", 4000)) }

func (e *env) registerNode(n *gssv1.NodeInfo) {
	e.t.Helper()
	if _, err := e.cc.Register(ctx(e.t), &gssv1.RegisterRequest{Node: n}); err != nil {
		e.t.Fatalf("Register %s: %v", n.GetId(), err)
	}
}

// beat sends a heartbeat from a healthy node: it lists what the state has
// starting or ready on it.
func (e *env) beat(nodeID string) {
	e.t.Helper()
	var sbs []*gssv1.NodeSandbox
	for _, sb := range e.state.List() {
		if sb.GetNodeId() == nodeID && (sb.GetState() == gssv1.SandboxState_SANDBOX_STATE_READY || sb.GetState() == gssv1.SandboxState_SANDBOX_STATE_STARTING) {
			sbs = append(sbs, &gssv1.NodeSandbox{Id: sb.GetId(), Spec: sb.GetSpec(), Incarnation: sb.GetIncarnation()})
		}
	}
	if _, err := e.cc.Heartbeat(ctx(e.t), &gssv1.HeartbeatRequest{NodeId: nodeID, Sandboxes: sbs}); err != nil {
		e.t.Fatalf("Heartbeat %s: %v", nodeID, err)
	}
}

// kill lets 3.3 s pass with only the alive nodes heartbeating, so every
// other node is past the node timeout at the next tick.
func (e *env) kill(alive ...string) {
	e.t.Helper()
	e.clock.advance(2 * time.Second)
	for _, id := range alive {
		e.beat(id)
	}
	e.clock.advance(1300 * time.Millisecond)
	for _, id := range alive {
		e.beat(id)
	}
}

func spec(cpu, mem int64) *gssv1.SandboxSpec {
	return &gssv1.SandboxSpec{Resources: &gssv1.Resources{CpuMillis: cpu, MemoryBytes: mem}}
}

func (e *env) create(s *gssv1.SandboxSpec) (*gssv1.Sandbox, error) {
	return e.sc.CreateSandbox(ctx(e.t), &gssv1.CreateSandboxRequest{Spec: s})
}

func (e *env) mustCreate(s *gssv1.SandboxSpec) *gssv1.Sandbox {
	e.t.Helper()
	sb, err := e.create(s)
	if err != nil {
		e.t.Fatalf("CreateSandbox: %v", err)
	}
	return sb
}

func (e *env) get(id string) *gssv1.Sandbox {
	e.t.Helper()
	sb, ok := e.state.Get(id)
	if !ok {
		e.t.Fatalf("sandbox %s not in the state", id)
	}
	return sb
}

func (e *env) node(id string) *gssv1.Node {
	e.t.Helper()
	for _, n := range e.state.Nodes() {
		if n.GetInfo().GetId() == id {
			return n
		}
	}
	e.t.Fatalf("node %s not in the state", id)
	return nil
}

func wantCode(t *testing.T, err error, want codes.Code, what string) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("%s: code %v (err %v), want %v", what, got, err, want)
	}
}

// eventually polls cond for up to 5 s.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// recvAll reads an Exec stream to its end and returns its events as
// "stdout ...", "stderr ..." and "exit <code>", and the status that ended
// the stream (nil for a clean end).
func recvAll(stream grpc.ServerStreamingClient[gssv1.ExecEvent]) ([]string, error) {
	var evs []string
	for {
		ev, err := stream.Recv()
		if err == io.EOF {
			return evs, nil
		}
		if err != nil {
			return evs, err
		}
		switch {
		case ev.GetExit() != nil:
			evs = append(evs, fmt.Sprintf("exit %d", ev.GetExit().GetCode()))
		case ev.GetStdout() != nil:
			evs = append(evs, "stdout "+string(ev.GetStdout()))
		default:
			evs = append(evs, "stderr "+string(ev.GetStderr()))
		}
	}
}

func (e *env) exec(id string) ([]string, error) {
	stream, err := e.sc.Exec(ctx(e.t), &gssv1.ExecRequest{SandboxId: id, Command: &gssv1.ExecRequest_PythonCode{PythonCode: "print(1)"}})
	if err != nil {
		return nil, err
	}
	return recvAll(stream)
}

func TestCreatePlacesAndStarts(t *testing.T) {
	e := newEnv(t, nil)
	e.register("n1")
	e.register("n2")
	sb := e.mustCreate(&gssv1.SandboxSpec{Ttl: durationpb.New(5 * time.Minute)})
	if sb.GetState() != gssv1.SandboxState_SANDBOX_STATE_READY {
		t.Errorf("state = %v, want READY", sb.GetState())
	}
	if sb.GetId() != "sb-000000000001" || sb.GetNodeId() != "n1" || sb.GetNodeGroup() != "g1" || sb.GetIncarnation() != 1 {
		t.Errorf("sandbox = %v, want sb-000000000001 on n1 (g1) at incarnation 1", sb)
	}
	if sb.GetStartedAs() != gssv1.StartMode_START_MODE_COLD || sb.GetStartLatency().AsDuration() != 40*time.Millisecond {
		t.Errorf("started as %v in %v, want COLD in 40ms", sb.GetStartedAs(), sb.GetStartLatency().AsDuration())
	}
	p := sb.GetPlacement()
	if p.GetNodesTotal() != 2 || p.GetNodesFeasible() != 2 || len(p.GetTop()) == 0 || p.GetTop()[0].GetNodeId() != "n1" {
		t.Errorf("placement = %v, want 2 of 2 feasible, n1 on top", p)
	}
	if len(p.GetTop()[0].GetPlugins()) == 0 {
		t.Errorf("placement has no per-plugin scores: %v", p)
	}
	// Defaults applied, the remaining TTL and the incarnation sent.
	calls := e.nodes.createCalls()
	if len(calls) != 1 || calls[0].addr != addr("n1") {
		t.Fatalf("Create calls = %v, want one to n1", calls)
	}
	r := calls[0].req
	if r.GetSandboxId() != sb.GetId() || r.GetIncarnation() != 1 ||
		r.GetSpec().GetResources().GetCpuMillis() != 500 || r.GetSpec().GetResources().GetMemoryBytes() != 256*mib ||
		r.GetSpec().GetTtl().AsDuration() != 5*time.Minute {
		t.Errorf("node Create request = %v", r)
	}
	if got, err := e.sc.GetSandbox(ctx(t), &gssv1.GetSandboxRequest{SandboxId: sb.GetId()}); err != nil || !proto.Equal(got, sb) {
		t.Errorf("GetSandbox = %v, %v; want %v", got, err, sb)
	}
	list, err := e.sc.ListSandboxes(ctx(t), &gssv1.ListSandboxesRequest{})
	if err != nil || len(list.GetSandboxes()) != 1 || list.GetSandboxes()[0].GetId() != sb.GetId() {
		t.Errorf("ListSandboxes = %v, %v", list, err)
	}
	nodes, err := e.sc.ListNodes(ctx(t), &gssv1.ListNodesRequest{})
	if err != nil || len(nodes.GetNodes()) != 2 || nodes.GetNodes()[0].GetAllocated().GetCpuMillis() != 500 {
		t.Errorf("ListNodes = %v, %v; want 2 nodes, 500m on n1", nodes, err)
	}
	_, err = e.sc.GetSandbox(ctx(t), &gssv1.GetSandboxRequest{SandboxId: "sb-nope"})
	wantCode(t, err, codes.NotFound, "GetSandbox of an unknown ID")
}

func TestCreateInvalid(t *testing.T) {
	e := newEnv(t, nil)
	e.register("n1")
	_, err := e.create(spec(50, 256*mib))
	wantCode(t, err, codes.InvalidArgument, "cpu_millis 50")
	if !strings.Contains(err.Error(), "cpu_millis") {
		t.Errorf("error %v does not name cpu_millis", err)
	}
	_, err = e.create(&gssv1.SandboxSpec{StartMode: 9})
	wantCode(t, err, codes.InvalidArgument, "start_mode 9")
	if got := e.state.List(); len(got) != 0 {
		t.Errorf("state.List = %v, want empty", got)
	}
	if got := e.nodes.createCalls(); len(got) != 0 {
		t.Errorf("node Create calls = %v, want none", got)
	}
}

func TestCreateNoFit(t *testing.T) {
	e := newEnv(t, nil)
	e.register("n1")
	e.register("n2")
	_, err := e.create(spec(64000, 256*mib))
	wantCode(t, err, codes.ResourceExhausted, "64 CPUs")
	if !strings.Contains(err.Error(), "rejected by Capacity") {
		t.Errorf("error %v does not say rejected by Capacity", err)
	}
	if sb := e.get("sb-000000000001"); sb.GetState() != gssv1.SandboxState_SANDBOX_STATE_FAILED {
		t.Errorf("sandbox state = %v, want FAILED", sb.GetState())
	}
	for _, n := range e.state.Nodes() {
		if n.GetAllocated().GetCpuMillis() != 0 || n.GetSandboxes() != 0 {
			t.Errorf("node %s allocation = %v, %d sandboxes", n.GetInfo().GetId(), n.GetAllocated(), n.GetSandboxes())
		}
	}
	if got := e.nodes.createCalls(); len(got) != 0 {
		t.Errorf("node Create calls = %v, want none", got)
	}
}

func TestCreateUnknownGroup(t *testing.T) {
	e := newEnv(t, nil)
	e.register("n1")
	_, err := e.create(&gssv1.SandboxSpec{NodeGroup: "zz"})
	wantCode(t, err, codes.ResourceExhausted, "group zz")
	if !strings.Contains(err.Error(), "NodeGroup") {
		t.Errorf("error %v does not name NodeGroup", err)
	}
}

func TestCreateRetriesOtherNode(t *testing.T) {
	e := newEnv(t, nil)
	e.register("n1")
	e.register("n2")
	e.nodes.setCreate(func(ctx context.Context, a string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
		if a == addr("n1") {
			return nil, status.Error(codes.Internal, "runsc create failed")
		}
		return &gssv1.NodeCreateResponse{StartedAs: gssv1.StartMode_START_MODE_RESTORE}, nil
	})
	sb := e.mustCreate(nil)
	if sb.GetNodeId() != "n2" || sb.GetState() != gssv1.SandboxState_SANDBOX_STATE_READY || sb.GetStartedAs() != gssv1.StartMode_START_MODE_RESTORE {
		t.Errorf("sandbox = %v, want READY on n2, restored", sb)
	}
	calls := e.nodes.createCalls()
	if len(calls) != 2 || calls[0].addr != addr("n1") || calls[1].addr != addr("n2") {
		t.Fatalf("Create calls = %v, want n1 then n2", calls)
	}
	// Unreserve keeps the incarnation, so the second attempt is the second one.
	if calls[0].req.GetIncarnation() != 1 || calls[1].req.GetIncarnation() != 2 || sb.GetIncarnation() != 2 {
		t.Errorf("incarnations %d, %d, sandbox %d; want 1, 2, 2", calls[0].req.GetIncarnation(), calls[1].req.GetIncarnation(), sb.GetIncarnation())
	}
	n1 := e.node("n1")
	if h := n1.GetHealth(); h < 0.799 || h > 0.801 {
		t.Errorf("n1 health = %v, want 0.8", h)
	}
	if n1.GetAllocated().GetCpuMillis() != 0 || n1.GetSandboxes() != 0 {
		t.Errorf("n1 allocation = %v, %d sandboxes; want none", n1.GetAllocated(), n1.GetSandboxes())
	}
	if e.node("n2").GetHealth() != 1 {
		t.Errorf("n2 health = %v, want 1", e.node("n2").GetHealth())
	}
}

func TestCreateGivesUpAfterThree(t *testing.T) {
	e := newEnv(t, nil)
	for _, id := range []string{"n1", "n2", "n3", "n4"} {
		e.register(id)
	}
	e.nodes.setCreate(func(ctx context.Context, a string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
		return nil, status.Errorf(codes.Internal, "broken %s", a)
	})
	_, err := e.create(nil)
	wantCode(t, err, codes.Unavailable, "every node failing")
	var addrs []string
	for _, c := range e.nodes.createCalls() {
		addrs = append(addrs, c.addr)
	}
	if want := []string{addr("n1"), addr("n2"), addr("n3")}; !slices.Equal(addrs, want) {
		t.Errorf("Create calls to %q, want %q", addrs, want)
	}
	if !strings.Contains(err.Error(), "broken "+addr("n3")) {
		t.Errorf("error %v does not carry the last node error", err)
	}
	if sb := e.get("sb-000000000001"); sb.GetState() != gssv1.SandboxState_SANDBOX_STATE_FAILED {
		t.Errorf("sandbox state = %v, want FAILED", sb.GetState())
	}
	for _, n := range e.state.Nodes() {
		if n.GetAllocated().GetCpuMillis() != 0 || n.GetSandboxes() != 0 {
			t.Errorf("node %s allocation = %v, %d sandboxes", n.GetInfo().GetId(), n.GetAllocated(), n.GetSandboxes())
		}
	}

	// With fewer nodes than attempts the create fails the same way.
	e2 := newEnv(t, nil)
	e2.register("n1")
	e2.nodes.setCreate(func(ctx context.Context, a string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
		return nil, status.Error(codes.Internal, "broken")
	})
	_, err = e2.create(nil)
	wantCode(t, err, codes.Unavailable, "the only node failing")
	if sb := e2.get("sb-000000000001"); sb.GetState() != gssv1.SandboxState_SANDBOX_STATE_FAILED {
		t.Errorf("sandbox state = %v, want FAILED", sb.GetState())
	}
}

func TestCreateForcedModeUnavailable(t *testing.T) {
	e := newEnv(t, nil)
	e.register("n1")
	e.register("n2")
	e.nodes.setCreate(func(ctx context.Context, a string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
		return nil, status.Error(codes.FailedPrecondition, "warm pool empty")
	})
	_, err := e.create(&gssv1.SandboxSpec{StartMode: gssv1.StartMode_START_MODE_WARM})
	wantCode(t, err, codes.FailedPrecondition, "forced warm")
	if status.Convert(err).Message() != "warm pool empty" {
		t.Errorf("message %q, want the node's", status.Convert(err).Message())
	}
	if got := e.nodes.createCalls(); len(got) != 1 {
		t.Errorf("Create calls = %v, want one", got)
	}
	if sb := e.get("sb-000000000001"); sb.GetState() != gssv1.SandboxState_SANDBOX_STATE_FAILED {
		t.Errorf("sandbox state = %v, want FAILED", sb.GetState())
	}
	if n := e.node("n1"); n.GetAllocated().GetCpuMillis() != 0 {
		t.Errorf("n1 allocation = %v, want none", n.GetAllocated())
	}
}

func TestCreateConcurrentNoOvercommit(t *testing.T) {
	e := newEnv(t, nil)
	e.registerNode(nodeInfo("n1", "g1", 2000))
	e.nodes.setCreate(func(ctx context.Context, a string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
		time.Sleep(10 * time.Millisecond)
		return &gssv1.NodeCreateResponse{StartedAs: gssv1.StartMode_START_MODE_COLD}, nil
	})
	var wg sync.WaitGroup
	var ok, exhausted atomic.Int32
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.create(spec(500, 256*mib))
			switch status.Code(err) {
			case codes.OK:
				ok.Add(1)
			case codes.ResourceExhausted:
				exhausted.Add(1)
			default:
				t.Errorf("create: %v", err)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 4 || exhausted.Load() != 6 {
		t.Errorf("%d created, %d ResourceExhausted; want 4 and 6", ok.Load(), exhausted.Load())
	}
	if n := e.node("n1"); n.GetAllocated().GetCpuMillis() != 2000 || n.GetSandboxes() != 4 {
		t.Errorf("n1 allocation = %v, %d sandboxes; want 2000m, 4", n.GetAllocated(), n.GetSandboxes())
	}
	if got := len(e.nodes.createCalls()); got != 4 {
		t.Errorf("%d node Create calls, want 4", got)
	}
}

func TestExecProxies(t *testing.T) {
	e := newEnv(t, nil)
	e.register("n1")
	sb := e.mustCreate(nil)
	e.nodes.setExec(func(ctx context.Context, a string, req *gssv1.ExecRequest) (grpc.ServerStreamingClient[gssv1.ExecEvent], error) {
		if req.GetPythonCode() != "print(1)" {
			return nil, status.Errorf(codes.InvalidArgument, "request %v", req)
		}
		return &fakeStream{ctx: ctx, events: []*gssv1.ExecEvent{stdout("a"), stderr("b"), stdout("c"), exit(3)}}, nil
	})
	evs, err := e.exec(sb.GetId())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"stdout a", "stderr b", "stdout c", "exit 3"}; !slices.Equal(evs, want) {
		t.Errorf("events = %q, want %q", evs, want)
	}
	if got := e.nodes.execCalls(); !slices.Equal(got, []string{addr("n1") + " " + sb.GetId()}) {
		t.Errorf("node Exec calls = %q", got)
	}
}

// TestExecErrors: a status from the node passes through; a broken stream
// says the sandbox was lost; no exit event is made up.
func TestExecErrors(t *testing.T) {
	e := newEnv(t, nil)
	e.register("n1")
	sb := e.mustCreate(nil)
	for _, tc := range []struct {
		end      error
		code     codes.Code
		contains string
	}{
		{status.Error(codes.Internal, "exec in sb: boom"), codes.Internal, "exec in sb: boom"},
		{status.Error(codes.InvalidArgument, "argv is empty"), codes.InvalidArgument, "argv is empty"},
		{status.Error(codes.Unavailable, "error reading from server: EOF"), codes.Unavailable, "sandbox " + sb.GetId() + " lost on node n1"},
		// The agent restarted and has no such sandbox: lost, to be rescheduled.
		{status.Error(codes.NotFound, "sandbox not found"), codes.Unavailable, "sandbox " + sb.GetId() + " lost on node n1: the node no longer has it"},
	} {
		e.nodes.setExec(func(ctx context.Context, a string, req *gssv1.ExecRequest) (grpc.ServerStreamingClient[gssv1.ExecEvent], error) {
			return &fakeStream{ctx: ctx, events: []*gssv1.ExecEvent{stdout("partial")}, end: tc.end}, nil
		})
		evs, err := e.exec(sb.GetId())
		wantCode(t, err, tc.code, fmt.Sprintf("node stream ending with %v", tc.end))
		if !strings.Contains(err.Error(), tc.contains) {
			t.Errorf("error %v does not contain %q", err, tc.contains)
		}
		if !slices.Equal(evs, []string{"stdout partial"}) {
			t.Errorf("events = %q, want only the stdout", evs)
		}
	}
}

func TestExecUnknown(t *testing.T) {
	e := newEnv(t, nil)
	_, err := e.exec("sb-nope")
	wantCode(t, err, codes.NotFound, "Exec of an unknown ID")
}

func TestExecWaitsForReschedule(t *testing.T) {
	e := newEnv(t, nil).run()
	e.register("n1")
	e.register("n2")
	sb := e.mustCreate(nil)
	if sb.GetNodeId() != "n1" {
		t.Fatalf("placed on %s, want n1", sb.GetNodeId())
	}
	e.nodes.setCreate(func(ctx context.Context, a string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
		time.Sleep(100 * time.Millisecond)
		return &gssv1.NodeCreateResponse{StartedAs: gssv1.StartMode_START_MODE_COLD}, nil
	})
	e.nodes.setExec(func(ctx context.Context, a string, req *gssv1.ExecRequest) (grpc.ServerStreamingClient[gssv1.ExecEvent], error) {
		return &fakeStream{ctx: ctx, events: []*gssv1.ExecEvent{stdout("on " + a), exit(0)}}, nil
	})
	e.kill("n2")
	eventually(t, "the sandbox to leave READY", func() bool {
		return e.get(sb.GetId()).GetState() != gssv1.SandboxState_SANDBOX_STATE_READY
	})
	evs, err := e.exec(sb.GetId())
	if err != nil {
		t.Fatalf("Exec during the reschedule: %v", err)
	}
	if want := []string{"stdout on " + addr("n2"), "exit 0"}; !slices.Equal(evs, want) {
		t.Errorf("events = %q, want %q", evs, want)
	}
	if got := e.get(sb.GetId()); got.GetNodeId() != "n2" || got.GetIncarnation() != 2 {
		t.Errorf("sandbox on %s at incarnation %d, want n2 at 2", got.GetNodeId(), got.GetIncarnation())
	}
}

func TestExecWaitCap(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.ExecWaitCap = 200 * time.Millisecond }).run()
	e.register("n1")
	sb := e.mustCreate(nil)
	e.kill() // no node left to move it to
	eventually(t, "the sandbox to be LOST", func() bool {
		return e.get(sb.GetId()).GetState() == gssv1.SandboxState_SANDBOX_STATE_LOST
	})
	begin := time.Now()
	_, err := e.exec(sb.GetId())
	wantCode(t, err, codes.DeadlineExceeded, "Exec of a LOST sandbox")
	if d := time.Since(begin); d < 200*time.Millisecond || d > 2*time.Second {
		t.Errorf("Exec returned after %v, want about 200ms", d)
	}
	if !strings.Contains(err.Error(), "LOST") {
		t.Errorf("error %v does not say the sandbox is LOST", err)
	}
	if got := e.nodes.execCalls(); len(got) != 0 {
		t.Errorf("node Exec calls = %q, want none", got)
	}
}

// TestExecEndsWhenSandboxLost: an Exec stream to a node that stops
// heartbeating ends with Unavailable once the node is declared dead, even
// if the connection never breaks.
func TestExecEndsWhenSandboxLost(t *testing.T) {
	e := newEnv(t, nil).run()
	e.register("n1")
	sb := e.mustCreate(nil)
	e.nodes.setExec(func(ctx context.Context, a string, req *gssv1.ExecRequest) (grpc.ServerStreamingClient[gssv1.ExecEvent], error) {
		return &fakeStream{ctx: ctx, events: []*gssv1.ExecEvent{stdout("started")}, hold: true}, nil
	})
	stream, err := e.sc.Exec(ctx(t), &gssv1.ExecRequest{SandboxId: sb.GetId()})
	if err != nil {
		t.Fatal(err)
	}
	if ev, err := stream.Recv(); err != nil || string(ev.GetStdout()) != "started" {
		t.Fatalf("first event %v, %v", ev, err)
	}
	e.kill()
	evs, err := recvAll(stream)
	wantCode(t, err, codes.Unavailable, "Exec on a dead node")
	if want := "sandbox " + sb.GetId() + " lost on node n1: marked LOST by the scheduler"; status.Convert(err).Message() != want {
		t.Errorf("message %q, want %q", status.Convert(err).Message(), want)
	}
	if len(evs) != 0 {
		t.Errorf("events after the loss = %q, want none", evs)
	}
}

func TestDestroy(t *testing.T) {
	e := newEnv(t, nil)
	e.register("n1")
	sb := e.mustCreate(nil)
	if _, err := e.sc.DestroySandbox(ctx(t), &gssv1.DestroySandboxRequest{SandboxId: sb.GetId()}); err != nil {
		t.Fatal(err)
	}
	if got, want := e.nodes.destroyCalls(), []string{addr("n1") + " " + sb.GetId() + " 0"}; !slices.Equal(got, want) {
		t.Errorf("node Destroy calls = %q, want %q", got, want)
	}
	if got := e.get(sb.GetId()); got.GetState() != gssv1.SandboxState_SANDBOX_STATE_DESTROYED {
		t.Errorf("state = %v, want DESTROYED", got.GetState())
	}
	if n := e.node("n1"); n.GetAllocated().GetCpuMillis() != 0 {
		t.Errorf("n1 allocation = %v, want none", n.GetAllocated())
	}
	if _, err := e.sc.DestroySandbox(ctx(t), &gssv1.DestroySandboxRequest{SandboxId: sb.GetId()}); err != nil {
		t.Errorf("second Destroy: %v", err)
	}
	if _, err := e.sc.DestroySandbox(ctx(t), &gssv1.DestroySandboxRequest{SandboxId: "sb-nope"}); err != nil {
		t.Errorf("Destroy of an unknown ID: %v", err)
	}
	if got := e.nodes.destroyCalls(); len(got) != 1 {
		t.Errorf("node Destroy calls = %q, want only the first", got)
	}
	_, err := e.exec(sb.GetId())
	wantCode(t, err, codes.NotFound, "Exec of a destroyed sandbox")
}

func TestDestroyNodeUnreachable(t *testing.T) {
	e := newEnv(t, nil)
	e.register("n1")
	sb := e.mustCreate(nil)
	e.nodes.mu.Lock()
	e.nodes.destroyErr = status.Error(codes.Unavailable, "connection refused")
	e.nodes.mu.Unlock()
	if _, err := e.sc.DestroySandbox(ctx(t), &gssv1.DestroySandboxRequest{SandboxId: sb.GetId()}); err != nil {
		t.Fatalf("Destroy with the node unreachable: %v", err)
	}
	resp, err := e.cc.Heartbeat(ctx(t), &gssv1.HeartbeatRequest{NodeId: "n1", Sandboxes: []*gssv1.NodeSandbox{
		{Id: sb.GetId(), Spec: sb.GetSpec(), Incarnation: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(resp.GetDestroySandboxIds(), []string{sb.GetId()}) {
		t.Errorf("destroy list = %q, want [%s]", resp.GetDestroySandboxIds(), sb.GetId())
	}
}

func TestRegister(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.HeartbeatInterval = 700 * time.Millisecond })
	resp, err := e.cc.Register(ctx(t), &gssv1.RegisterRequest{Node: nodeInfo("n1", "a", 4000)})
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.GetHeartbeatInterval().AsDuration(); got != 700*time.Millisecond {
		t.Errorf("heartbeat interval = %v, want 700ms", got)
	}
	if n := e.node("n1"); n.GetState() != gssv1.NodeState_NODE_STATE_READY || n.GetInfo().GetGroup() != "a" {
		t.Errorf("node = %v, want READY in group a", n)
	}
	for _, n := range []*gssv1.NodeInfo{nil, {Address: "x:1"}, {Id: "n2"}} {
		_, err := e.cc.Register(ctx(t), &gssv1.RegisterRequest{Node: n})
		wantCode(t, err, codes.InvalidArgument, fmt.Sprintf("Register %v", n))
	}
}

func TestHeartbeatUnknownNodeNotFound(t *testing.T) {
	e := newEnv(t, nil)
	_, err := e.cc.Heartbeat(ctx(t), &gssv1.HeartbeatRequest{NodeId: "n1"})
	wantCode(t, err, codes.NotFound, "Heartbeat from an unregistered node")
	e.register("n1")
	if _, err := e.cc.Heartbeat(ctx(t), &gssv1.HeartbeatRequest{NodeId: "n1"}); err != nil {
		t.Errorf("Heartbeat after Register: %v", err)
	}
}

func TestCreateSpans(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	e := newEnv(t, nil)
	e.register("n1")
	e.register("n2")
	e.mustCreate(nil)
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range sr.Ended() {
		byName[s.Name()] = s
	}
	create, place := byName["gateway.create"], byName["sched.place"]
	if create == nil || place == nil {
		t.Fatalf("spans %v, want gateway.create and sched.place", byName)
	}
	if place.Parent().SpanID() != create.SpanContext().SpanID() {
		t.Error("sched.place is not a child of gateway.create")
	}
	attrs := map[attribute.Key]attribute.Value{}
	for _, kv := range place.Attributes() {
		attrs[kv.Key] = kv.Value
	}
	if attrs["nodes.total"].AsInt64() != 2 || attrs["nodes.feasible"].AsInt64() != 2 || attrs["node.chosen"].AsString() != "n1" {
		t.Errorf("sched.place attributes = %v", place.Attributes())
	}
	for _, plugin := range []string{"BinPack", "WarmPool", "GroupAffinity", "NodeHealth"} {
		if _, ok := attrs[attribute.Key("score."+plugin)]; !ok {
			t.Errorf("sched.place has no score.%s: %v", plugin, place.Attributes())
		}
	}
	if attrs["score.NodeHealth"].AsFloat64() != 100 {
		t.Errorf("score.NodeHealth = %v, want 100 (weight 1, health 1)", attrs["score.NodeHealth"])
	}
}

// fakeNodeService is a NodeService for the gRPC node client test.
type fakeNodeService struct {
	gssv1.UnimplementedNodeServiceServer
	mu       sync.Mutex
	destroys []string
}

func (f *fakeNodeService) CreateSandbox(ctx context.Context, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
	if req.GetIncarnation() != 4 {
		return nil, status.Errorf(codes.InvalidArgument, "incarnation %d", req.GetIncarnation())
	}
	return &gssv1.NodeCreateResponse{StartedAs: gssv1.StartMode_START_MODE_WARM}, nil
}

func (f *fakeNodeService) Exec(req *gssv1.ExecRequest, stream grpc.ServerStreamingServer[gssv1.ExecEvent]) error {
	big := strings.Repeat("x", 17<<20) // over gRPC's default 4 MiB, under nodeagent.MaxMessageBytes
	if err := stream.Send(stdout(big)); err != nil {
		return err
	}
	if err := stream.Send(exit(0)); err != nil {
		return err
	}
	return status.Error(codes.Internal, "after exit")
}

func (f *fakeNodeService) DestroySandbox(ctx context.Context, req *gssv1.DestroySandboxRequest) (*gssv1.DestroySandboxResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.destroys = append(f.destroys, fmt.Sprintf("%s %d", req.GetSandboxId(), req.GetIncarnation()))
	return &gssv1.DestroySandboxResponse{}, nil
}

func TestGRPCNodes(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	svc := &fakeNodeService{}
	g := grpc.NewServer(nodeagent.ServerOptions()...)
	gssv1.RegisterNodeServiceServer(g, svc)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)

	nodes := NewGRPCNodes()
	t.Cleanup(func() {
		if err := nodes.(io.Closer).Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	a := lis.Addr().String()
	resp, err := nodes.Create(ctx(t), a, &gssv1.NodeCreateRequest{SandboxId: "sb-1", Incarnation: 4})
	if err != nil || resp.GetStartedAs() != gssv1.StartMode_START_MODE_WARM {
		t.Fatalf("Create = %v, %v", resp, err)
	}
	stream, err := nodes.Exec(ctx(t), a, &gssv1.ExecRequest{SandboxId: "sb-1"})
	if err != nil {
		t.Fatal(err)
	}
	evs, err := recvAll(stream)
	wantCode(t, err, codes.Internal, "the status after the exit")
	if len(evs) != 2 || len(evs[0]) != len("stdout ")+17<<20 || evs[1] != "exit 0" {
		t.Errorf("%d events, want a 17 MiB stdout and the exit", len(evs))
	}
	if err := nodes.Destroy(ctx(t), a, "sb-1", 3); err != nil {
		t.Fatal(err)
	}
	svc.mu.Lock()
	destroys := slices.Clone(svc.destroys)
	svc.mu.Unlock()
	if !slices.Equal(destroys, []string{"sb-1 3"}) {
		t.Errorf("node saw destroys %q, want [sb-1 3]", destroys)
	}
	if err := nodes.Destroy(ctx(t), "127.0.0.1:1", "sb-1", 0); status.Code(err) != codes.Unavailable {
		t.Errorf("Destroy on a closed port = %v, want Unavailable", err)
	}

	g2 := nodes.(*grpcNodes)
	g2.mu.Lock()
	conn := g2.conns[a]
	g2.mu.Unlock()
	nodes.(interface{ Forget(string) }).Forget(a)
	g2.mu.Lock()
	_, kept := g2.conns[a]
	g2.mu.Unlock()
	if kept || conn.GetState() != connectivity.Shutdown {
		t.Errorf("after Forget: cached %v, connection %v; want dropped and shut down", kept, conn.GetState())
	}
	if err := nodes.Destroy(ctx(t), a, "sb-1", 1); err != nil {
		t.Errorf("Destroy after Forget (a new connection): %v", err)
	}
}

// TestExecExitThenLost: once the exit event is through, a loss seen before
// the stream's end does not add an error after it (spec D34).
func TestExecExitThenLost(t *testing.T) {
	e := newEnv(t, nil).run()
	e.register("n1")
	sb := e.mustCreate(nil)
	e.nodes.setExec(func(ctx context.Context, a string, req *gssv1.ExecRequest) (grpc.ServerStreamingClient[gssv1.ExecEvent], error) {
		return &fakeStream{ctx: ctx, events: []*gssv1.ExecEvent{stdout("4\n"), exit(0)}, hold: true}, nil
	})
	stream, err := e.sc.Exec(ctx(t), &gssv1.ExecRequest{SandboxId: sb.GetId()})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := stream.Recv(); err != nil {
			t.Fatal(err)
		}
	}
	e.kill()
	evs, err := recvAll(stream)
	if err != nil || len(evs) != 0 {
		t.Errorf("after the exit: events %q, %v; want a clean end", evs, err)
	}
}

// TestExecEndsWhenSandboxDestroyed: a destroy ends a proxied Exec with
// NotFound even if the node's stream does not end.
func TestExecEndsWhenSandboxDestroyed(t *testing.T) {
	e := newEnv(t, nil)
	e.register("n1")
	sb := e.mustCreate(nil)
	e.nodes.setExec(func(ctx context.Context, a string, req *gssv1.ExecRequest) (grpc.ServerStreamingClient[gssv1.ExecEvent], error) {
		return &fakeStream{ctx: ctx, events: []*gssv1.ExecEvent{stdout("started")}, hold: true}, nil
	})
	stream, err := e.sc.Exec(ctx(t), &gssv1.ExecRequest{SandboxId: sb.GetId()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.sc.DestroySandbox(ctx(t), &gssv1.DestroySandboxRequest{SandboxId: sb.GetId()}); err != nil {
		t.Fatal(err)
	}
	_, err = recvAll(stream)
	wantCode(t, err, codes.NotFound, "Exec of a sandbox destroyed meanwhile")
}

// TestWatchEnd covers every way the state can end a proxied Exec.
func TestWatchEnd(t *testing.T) {
	was := &gssv1.Sandbox{Id: "sb-1", NodeId: "n1", Incarnation: 1, State: gssv1.SandboxState_SANDBOX_STATE_READY}
	with := func(f func(*gssv1.Sandbox)) *gssv1.Sandbox {
		sb := proto.Clone(was).(*gssv1.Sandbox)
		f(sb)
		return sb
	}
	for _, tc := range []struct {
		name string
		cur  *gssv1.Sandbox // nil: gone from the state
		code codes.Code
		msg  string
	}{
		{"unchanged", was, codes.OK, ""},
		{"lost", with(func(sb *gssv1.Sandbox) { sb.State = gssv1.SandboxState_SANDBOX_STATE_LOST }),
			codes.Unavailable, "sandbox sb-1 lost on node n1: marked LOST by the scheduler"},
		{"moved", with(func(sb *gssv1.Sandbox) { sb.NodeId, sb.Incarnation = "n2", 2 }),
			codes.Unavailable, "sandbox sb-1 lost on node n1: moved to node n2 (incarnation 2)"},
		{"restarted", with(func(sb *gssv1.Sandbox) { sb.Incarnation, sb.State = 2, gssv1.SandboxState_SANDBOX_STATE_STARTING }),
			codes.Unavailable, "sandbox sb-1 lost on node n1: moved to node n1 (incarnation 2)"},
		{"destroyed", with(func(sb *gssv1.Sandbox) {
			sb.State, sb.Message = gssv1.SandboxState_SANDBOX_STATE_DESTROYED, "ttl expired"
		}), codes.NotFound, "sandbox sb-1 was destroyed (ttl expired)"},
		{"failed", with(func(sb *gssv1.Sandbox) {
			sb.State, sb.Message = gssv1.SandboxState_SANDBOX_STATE_FAILED, "no node fits"
		}), codes.FailedPrecondition, "no node fits"},
		{"gone", nil, codes.NotFound, "sandbox sb-1 not found"},
	} {
		err := watchEnd(was, tc.cur, tc.cur != nil)
		if got := status.Convert(err); got.Code() != tc.code || got.Message() != tc.msg {
			t.Errorf("%s: %v, want %v %q", tc.name, err, tc.code, tc.msg)
		}
	}
}

// TestRegisterNewAddressDropsConn: a node that registers again at another
// address (an agent restarted on a new port) has its old connection dropped.
func TestRegisterNewAddressDropsConn(t *testing.T) {
	e := newEnv(t, nil)
	e.register("n1")
	e.register("n1") // same address: kept
	moved := nodeInfo("n1", "g1", 4000)
	moved.Address = "n1:7999"
	e.registerNode(moved)
	e.nodes.mu.Lock()
	defer e.nodes.mu.Unlock()
	if !slices.Equal(e.nodes.forgets, []string{addr("n1")}) {
		t.Errorf("dropped connections %q, want [%s]", e.nodes.forgets, addr("n1"))
	}
}

// TestDestroyLostSandbox: destroying a LOST sandbox does not wait on its
// dead node; the node's next heartbeat answer lists the ID.
func TestDestroyLostSandbox(t *testing.T) {
	e := newEnv(t, nil).run()
	e.register("n1")
	sb := e.mustCreate(nil)
	e.kill()
	eventually(t, "the sandbox LOST", func() bool {
		return e.get(sb.GetId()).GetState() == gssv1.SandboxState_SANDBOX_STATE_LOST
	})
	if _, err := e.sc.DestroySandbox(ctx(t), &gssv1.DestroySandboxRequest{SandboxId: sb.GetId()}); err != nil {
		t.Fatal(err)
	}
	if got := e.nodes.destroyCalls(); len(got) != 0 {
		t.Errorf("node Destroy calls = %q, want none for a LOST sandbox", got)
	}
	resp, err := e.cc.Heartbeat(ctx(t), &gssv1.HeartbeatRequest{NodeId: "n1", Sandboxes: []*gssv1.NodeSandbox{
		{Id: sb.GetId(), Spec: sb.GetSpec(), Incarnation: 1},
	}})
	if err != nil || !slices.Equal(resp.GetDestroySandboxIds(), []string{sb.GetId()}) {
		t.Errorf("returning node's heartbeat = %v, %v; want %s on the destroy list", resp, err, sb.GetId())
	}
}

// The race paths of a create: the state changes while the node Create is in
// flight, so Commit refuses and the node's copy is destroyed by incarnation.

func TestCommitRefusedAfterDestroy(t *testing.T) {
	e := newEnv(t, nil)
	e.register("n1")
	var derr error
	e.nodes.setCreate(func(ctx context.Context, a string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
		_, derr = e.srv.DestroySandbox(context.Background(), &gssv1.DestroySandboxRequest{SandboxId: req.GetSandboxId()})
		return &gssv1.NodeCreateResponse{StartedAs: gssv1.StartMode_START_MODE_COLD}, nil
	})
	_, err := e.create(nil)
	if derr != nil {
		t.Fatalf("DestroySandbox during the create: %v", derr)
	}
	wantCode(t, err, codes.Aborted, "a create whose sandbox was destroyed meanwhile")
	id := "sb-000000000001"
	// DestroySandbox's own call (whatever is live), then the stale copy by its incarnation.
	if got, want := e.nodes.destroyCalls(), []string{addr("n1") + " " + id + " 0", addr("n1") + " " + id + " 1"}; !slices.Equal(got, want) {
		t.Errorf("node Destroy calls = %q, want %q", got, want)
	}
	if sb := e.get(id); sb.GetState() != gssv1.SandboxState_SANDBOX_STATE_DESTROYED {
		t.Errorf("state = %v, want DESTROYED", sb.GetState())
	}
	if n := e.node("n1"); n.GetAllocated().GetCpuMillis() != 0 || n.GetSandboxes() != 0 {
		t.Errorf("n1 allocation = %v, %d sandboxes; want none", n.GetAllocated(), n.GetSandboxes())
	}
}

func TestCommitRefusedAfterExpiry(t *testing.T) {
	e := newEnv(t, nil).run()
	e.register("n1")
	e.nodes.setCreate(func(ctx context.Context, a string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
		e.clock.advance(61 * time.Second)
		if !waitState(e.state, req.GetSandboxId(), gssv1.SandboxState_SANDBOX_STATE_DESTROYED) {
			return nil, status.Error(codes.Internal, "the tick did not expire the sandbox")
		}
		return &gssv1.NodeCreateResponse{StartedAs: gssv1.StartMode_START_MODE_COLD}, nil
	})
	_, err := e.create(&gssv1.SandboxSpec{Ttl: durationpb.New(time.Minute)})
	wantCode(t, err, codes.Aborted, "a create whose sandbox expired meanwhile")
	id := "sb-000000000001"
	eventually(t, "the node Destroys", func() bool {
		got := e.nodes.destroyCalls()
		return slices.Contains(got, addr("n1")+" "+id+" 1") && slices.Contains(got, addr("n1")+" "+id+" 0")
	})
	if sb := e.get(id); sb.GetState() != gssv1.SandboxState_SANDBOX_STATE_DESTROYED || sb.GetMessage() != "ttl expired" {
		t.Errorf("sandbox = %v, want DESTROYED by ttl", sb)
	}
	if n := e.node("n1"); n.GetAllocated().GetCpuMillis() != 0 || n.GetSandboxes() != 0 {
		t.Errorf("n1 allocation = %v, %d sandboxes; want none", n.GetAllocated(), n.GetSandboxes())
	}
}

func TestCommitRefusedAfterNodeDeath(t *testing.T) {
	e := newEnv(t, nil).run()
	e.register("n1")
	e.register("n2")
	e.nodes.setCreate(func(ctx context.Context, a string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
		if a == addr("n1") {
			// n1 stops heartbeating while its Create runs.
			e.clock.advance(2 * time.Second)
			e.state.Heartbeat(&gssv1.HeartbeatRequest{NodeId: "n2"})
			e.clock.advance(1300 * time.Millisecond)
			e.state.Heartbeat(&gssv1.HeartbeatRequest{NodeId: "n2"})
			if !waitState(e.state, req.GetSandboxId(), gssv1.SandboxState_SANDBOX_STATE_LOST) {
				return nil, status.Error(codes.Internal, "the tick did not lose the sandbox")
			}
		}
		return &gssv1.NodeCreateResponse{StartedAs: gssv1.StartMode_START_MODE_COLD}, nil
	})
	sb, err := e.create(nil)
	if err != nil {
		t.Fatalf("create on a node that died meanwhile: %v, want the LOST sandbox", err)
	}
	if sb.GetState() != gssv1.SandboxState_SANDBOX_STATE_LOST || sb.GetIncarnation() != 1 {
		t.Errorf("response = %v, want LOST at incarnation 1", sb)
	}
	if got := e.nodes.destroyCalls(); !slices.Contains(got, addr("n1")+" "+sb.GetId()+" 1") {
		t.Errorf("node Destroy calls = %q, want the stale copy on n1 at incarnation 1", got)
	}
	if n := e.node("n1"); n.GetAllocated().GetCpuMillis() != 0 || n.GetSandboxes() != 0 {
		t.Errorf("n1 allocation = %v, %d sandboxes; want none", n.GetAllocated(), n.GetSandboxes())
	}
	eventually(t, "the sandbox READY on n2 at incarnation 2", func() bool {
		got := e.get(sb.GetId())
		return got.GetState() == gssv1.SandboxState_SANDBOX_STATE_READY && got.GetNodeId() == "n2" && got.GetIncarnation() == 2
	})
}

// TestCreateCancelled: a client that gives up during the node Create leaves
// the sandbox FAILED with nothing reserved, and the node is not blamed.
func TestCreateCancelled(t *testing.T) {
	e := newEnv(t, nil)
	e.register("n1")
	e.register("n2")
	started := make(chan struct{})
	e.nodes.setCreate(func(ctx context.Context, a string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
		close(started)
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	})
	cctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := e.sc.CreateSandbox(cctx, &gssv1.CreateSandboxRequest{})
		done <- err
	}()
	<-started
	cancel()
	wantCode(t, <-done, codes.Canceled, "a cancelled create")
	id := "sb-000000000001"
	eventually(t, "the sandbox FAILED", func() bool {
		return e.get(id).GetState() == gssv1.SandboxState_SANDBOX_STATE_FAILED
	})
	if got := e.nodes.createCalls(); len(got) != 1 {
		t.Errorf("Create calls = %d, want 1: no other node after a cancel", len(got))
	}
	for _, n := range e.state.Nodes() {
		if n.GetAllocated().GetCpuMillis() != 0 || n.GetSandboxes() != 0 || n.GetHealth() != 1 {
			t.Errorf("node %s = %v, want nothing allocated and health 1", n.GetInfo().GetId(), n)
		}
	}
}

// waitState polls the state for up to 5 s; it is for fakes, which run on
// the gateway's goroutines and must not stop the test.
func waitState(st *cluster.State, id string, want gssv1.SandboxState) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if sb, ok := st.Get(id); ok && sb.GetState() == want {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}
