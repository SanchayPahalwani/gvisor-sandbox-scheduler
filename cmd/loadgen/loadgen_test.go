package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bench"
)

// fake is an in-process SandboxService. Every hook runs with mu held, so a hook may
// read and change the fake's fields but must not call its methods.
type fake struct {
	gssv1.UnimplementedSandboxServiceServer

	mu        sync.Mutex
	creates   int                  // CreateSandbox calls so far
	specs     []*gssv1.SandboxSpec // the spec of every CreateSandbox call, in order
	live      map[string]*gssv1.Sandbox
	nodes     []*gssv1.Node
	execs     int
	destroyed int

	createDelay time.Duration
	execDelay   time.Duration

	// tick runs at the start of every call; tests use it to change state over time.
	tick func(f *fake)
	// createErr fails a create; live is the number of live sandboxes before it.
	createErr func(n, live int, spec *gssv1.SandboxSpec) error
	// startedAs is the start path reported; default: the forced mode, WARM for AUTO.
	startedAs func(n int, spec *gssv1.SandboxSpec) gssv1.StartMode
	// readyLatency is the start_latency reported; default 10 ms.
	readyLatency func(n, live int, spec *gssv1.SandboxSpec) time.Duration
	// place picks the node and group; default a1 in group a.
	place func(n int) (node, group string)
	// execErr fails an Exec on a live sandbox.
	execErr func(sb *gssv1.Sandbox) error
	// execDelayFor replaces execDelay for one Exec; the delay comes before an error too,
	// so an Exec can block and then fail.
	execDelayFor func(sb *gssv1.Sandbox) time.Duration
	// destroyErr fails a DestroySandbox; the sandbox stays live.
	destroyErr func(id string) error
	// getErr fails a GetSandbox.
	getErr func(id string) error
}

func newFake(t *testing.T) (*fake, gssv1.SandboxServiceClient) {
	t.Helper()
	f := &fake{live: map[string]*gssv1.Sandbox{}}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	gssv1.RegisterSandboxServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
	})
	return f, gssv1.NewSandboxServiceClient(conn)
}

// serveTCP serves f on a loopback TCP port, for tests that run the command line.
func serveTCP(t *testing.T, f *fake) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	gssv1.RegisterSandboxServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func node(id, group string, state gssv1.NodeState, warm int32) *gssv1.Node {
	return &gssv1.Node{Info: &gssv1.NodeInfo{Id: id, Group: group}, State: state, WarmReady: warm}
}

func (f *fake) lockTick() {
	f.mu.Lock()
	if f.tick != nil {
		f.tick(f)
	}
}

func (f *fake) CreateSandbox(_ context.Context, req *gssv1.CreateSandboxRequest) (*gssv1.Sandbox, error) {
	f.lockTick()
	n := f.creates
	f.creates++
	spec := proto.Clone(req.GetSpec()).(*gssv1.SandboxSpec)
	f.specs = append(f.specs, spec)
	if f.createErr != nil {
		if err := f.createErr(n, len(f.live), spec); err != nil {
			f.mu.Unlock()
			return nil, err
		}
	}
	mode := spec.GetStartMode()
	if f.startedAs != nil {
		mode = f.startedAs(n, spec)
	} else if mode == gssv1.StartMode_START_MODE_AUTO {
		mode = gssv1.StartMode_START_MODE_WARM
	}
	lat := 10 * time.Millisecond
	if f.readyLatency != nil {
		lat = f.readyLatency(n, len(f.live), spec)
	}
	nodeID, group := "a1", "a"
	if f.place != nil {
		nodeID, group = f.place(n)
	}
	sb := &gssv1.Sandbox{
		Id: fmt.Sprintf("sb-%d", n), Spec: spec, State: gssv1.SandboxState_SANDBOX_STATE_READY,
		NodeId: nodeID, NodeGroup: group, StartedAs: mode, Incarnation: 1, StartLatency: durationpb.New(lat),
	}
	f.live[sb.Id] = sb
	out := proto.Clone(sb).(*gssv1.Sandbox)
	delay := f.createDelay
	f.mu.Unlock()
	time.Sleep(delay)
	return out, nil
}

func (f *fake) Exec(req *gssv1.ExecRequest, stream grpc.ServerStreamingServer[gssv1.ExecEvent]) error {
	f.lockTick()
	f.execs++
	sb := f.live[req.GetSandboxId()]
	var err error
	if sb == nil {
		err = status.Errorf(codes.NotFound, "sandbox %s not found", req.GetSandboxId())
	} else if f.execErr != nil {
		err = f.execErr(sb)
	}
	delay := f.execDelay
	if sb != nil && f.execDelayFor != nil {
		delay = f.execDelayFor(sb)
	}
	f.mu.Unlock()
	time.Sleep(delay)
	if err != nil {
		return err
	}
	if err := stream.Send(&gssv1.ExecEvent{Event: &gssv1.ExecEvent_Stdout{Stdout: []byte("hello\n")}}); err != nil {
		return err
	}
	return stream.Send(&gssv1.ExecEvent{Event: &gssv1.ExecEvent_Exit{Exit: &gssv1.ExitStatus{Code: 0}}})
}

func (f *fake) DestroySandbox(_ context.Context, req *gssv1.DestroySandboxRequest) (*gssv1.DestroySandboxResponse, error) {
	f.lockTick()
	defer f.mu.Unlock()
	if _, ok := f.live[req.GetSandboxId()]; !ok {
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", req.GetSandboxId())
	}
	if f.destroyErr != nil {
		if err := f.destroyErr(req.GetSandboxId()); err != nil {
			return nil, err
		}
	}
	delete(f.live, req.GetSandboxId())
	f.destroyed++
	return &gssv1.DestroySandboxResponse{}, nil
}

func (f *fake) GetSandbox(_ context.Context, req *gssv1.GetSandboxRequest) (*gssv1.Sandbox, error) {
	f.lockTick()
	defer f.mu.Unlock()
	if f.getErr != nil {
		if err := f.getErr(req.GetSandboxId()); err != nil {
			return nil, err
		}
	}
	sb, ok := f.live[req.GetSandboxId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", req.GetSandboxId())
	}
	return proto.Clone(sb).(*gssv1.Sandbox), nil
}

func (f *fake) ListSandboxes(context.Context, *gssv1.ListSandboxesRequest) (*gssv1.ListSandboxesResponse, error) {
	f.lockTick()
	defer f.mu.Unlock()
	resp := &gssv1.ListSandboxesResponse{}
	for _, sb := range f.live {
		resp.Sandboxes = append(resp.Sandboxes, proto.Clone(sb).(*gssv1.Sandbox))
	}
	return resp, nil
}

func (f *fake) ListNodes(context.Context, *gssv1.ListNodesRequest) (*gssv1.ListNodesResponse, error) {
	f.lockTick()
	defer f.mu.Unlock()
	resp := &gssv1.ListNodesResponse{}
	for _, n := range f.nodes {
		resp.Nodes = append(resp.Nodes, proto.Clone(n).(*gssv1.Node))
	}
	return resp, nil
}

func (f *fake) liveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.live)
}

func seriesByName(t *testing.T, r *bench.Run, name string) bench.Series {
	t.Helper()
	for _, s := range r.Series {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("run %s has no series %q; has %v", r.Name, name, seriesNames(r))
	return bench.Series{}
}

func seriesNames(r *bench.Run) []string {
	var names []string
	for _, s := range r.Series {
		names = append(names, s.Name)
	}
	return names
}

var (
	cold    = gssv1.StartMode_START_MODE_COLD
	restore = gssv1.StartMode_START_MODE_RESTORE
	warm    = gssv1.StartMode_START_MODE_WARM
	auto    = gssv1.StartMode_START_MODE_AUTO
)

func TestStartRecordsPerMode(t *testing.T) {
	f, c := newFake(t)
	f.nodes = []*gssv1.Node{
		node("a1", "a", gssv1.NodeState_NODE_STATE_READY, 2),
		node("b1", "b", gssv1.NodeState_NODE_STATE_READY, 2),
	}
	f.createDelay = 2 * time.Millisecond
	ready := map[gssv1.StartMode]time.Duration{cold: 40 * time.Millisecond, restore: 20 * time.Millisecond, warm: 5 * time.Millisecond}
	f.readyLatency = func(_, _ int, spec *gssv1.SandboxSpec) time.Duration { return ready[spec.GetStartMode()] }

	run, err := runStart(context.Background(), c, newStartConfig(3, []gssv1.StartMode{cold, restore, warm}, "a"), io.Discard)
	if err != nil {
		t.Fatalf("runStart: %v", err)
	}
	if run.Name != "start" || run.Errors != 0 {
		t.Fatalf("name %q errors %d, want start and 0; notes %v", run.Name, run.Errors, run.Notes)
	}
	for mode, want := range map[string]float64{"cold": 40, "restore": 20, "warm": 5} {
		for _, kind := range []string{"ready", "create", "first_result"} {
			s := seriesByName(t, run, mode+"_"+kind+"_ms")
			if s.Count != 2 {
				t.Errorf("%s count = %d, want 2 (n=3 with the warm-up dropped)", s.Name, s.Count)
			}
			if s.Unit != "ms" {
				t.Errorf("%s unit = %q, want ms", s.Name, s.Unit)
			}
		}
		if got := seriesByName(t, run, mode+"_ready_ms").P50; got != want {
			t.Errorf("%s_ready_ms p50 = %v, want the reported start_latency %v", mode, got, want)
		}
		create := seriesByName(t, run, mode+"_create_ms")
		first := seriesByName(t, run, mode+"_first_result_ms")
		if create.Min < 2 || first.Min < create.Min {
			t.Errorf("%s: create min %v (want >= the fake's 2 ms delay), first result min %v (want >= create)", mode, create.Min, first.Min)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	wantModes := []gssv1.StartMode{cold, cold, cold, restore, restore, restore, warm, warm, warm}
	if len(f.specs) != len(wantModes) {
		t.Fatalf("got %d creates, want %d", len(f.specs), len(wantModes))
	}
	for i, s := range f.specs {
		if s.GetStartMode() != wantModes[i] || s.GetNodeGroup() != "a" {
			t.Errorf("create %d: mode %v group %q, want %v and a", i, s.GetStartMode(), s.GetNodeGroup(), wantModes[i])
		}
	}
	if len(f.live) != 0 || f.execs != 9 {
		t.Errorf("live %d execs %d after the run, want 0 and 9", len(f.live), f.execs)
	}
}

func TestStartRetriesStaleWarm(t *testing.T) {
	f, c := newFake(t)
	f.nodes = []*gssv1.Node{node("a1", "a", gssv1.NodeState_NODE_STATE_READY, 1)}
	failed := false
	f.createErr = func(n, _ int, spec *gssv1.SandboxSpec) error {
		if n == 1 && !failed {
			failed = true
			return status.Error(codes.FailedPrecondition, "no warm sandbox on the chosen node")
		}
		return nil
	}
	run, err := runStart(context.Background(), c, newStartConfig(3, []gssv1.StartMode{warm}, "a"), io.Discard)
	if err != nil {
		t.Fatalf("runStart: %v", err)
	}
	if run.Errors != 0 {
		t.Errorf("errors = %d, want 0 (a FailedPrecondition is retried); notes %v", run.Errors, run.Notes)
	}
	if s := seriesByName(t, run, "warm_ready_ms"); s.Count != 2 {
		t.Errorf("warm_ready_ms count = %d, want 2: the failed attempt is not a sample", s.Count)
	}
	if got := run.Scalars["warm_retries"]; got != 1 {
		t.Errorf("warm_retries = %v, want 1", got)
	}
	if f.creates != 4 {
		t.Errorf("creates = %d, want 4 (3 cycles and 1 retry)", f.creates)
	}
}

func TestThroughputShares(t *testing.T) {
	f, c := newFake(t)
	f.nodes = []*gssv1.Node{node("a1", "a", gssv1.NodeState_NODE_STATE_READY, 2)}
	f.createDelay = 2 * time.Millisecond
	f.startedAs = func(n int, _ *gssv1.SandboxSpec) gssv1.StartMode {
		if n%2 == 0 {
			return warm
		}
		return restore
	}
	run, err := runThroughput(context.Background(), c, throughputConfig{Workers: 2, Duration: 300 * time.Millisecond}, io.Discard)
	if err != nil {
		t.Fatalf("runThroughput: %v", err)
	}
	if run.Errors != 0 {
		t.Fatalf("errors = %d, want 0; notes %v", run.Errors, run.Notes)
	}
	n := seriesByName(t, run, "first_result_ms").Count
	if n < 2 || seriesByName(t, run, "ready_ms").Count != n {
		t.Fatalf("first_result_ms count %d, ready_ms count %d: want the same, at least 2", n, seriesByName(t, run, "ready_ms").Count)
	}
	sw, sr, sc := run.Scalars["share_warm"], run.Scalars["share_restore"], run.Scalars["share_cold"]
	// Creates alternate warm/restore, so the shares are 0.5/0.5 up to one odd cycle.
	if math.Abs(sw-0.5) > 0.5/float64(n)+1e-9 || math.Abs(sw+sr-1) > 1e-9 || sc != 0 {
		t.Errorf("shares warm %v restore %v cold %v over %d cycles, want 0.5/0.5/0", sw, sr, sc, n)
	}
	sps := run.Scalars["sandboxes_per_sec"]
	if sps <= 0 || math.Abs(sps-float64(n)/run.Seconds) > 1e-6 {
		t.Errorf("sandboxes_per_sec = %v, want %d cycles / %v s", sps, n, run.Seconds)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, s := range f.specs {
		if s.GetStartMode() != auto {
			t.Errorf("create %d forced mode %v, want AUTO", i, s.GetStartMode())
		}
	}
	if len(f.live) != 0 {
		t.Errorf("%d sandboxes left live", len(f.live))
	}
}

// The warm-up is the first cycle that succeeds, not the first cycle tried.
func TestStartWarmUpIsFirstSuccess(t *testing.T) {
	f, c := newFake(t)
	f.nodes = []*gssv1.Node{node("a1", "a", gssv1.NodeState_NODE_STATE_READY, 2)}
	f.createErr = func(n, _ int, _ *gssv1.SandboxSpec) error {
		if n == 0 {
			return status.Error(codes.Unavailable, "scheduler not ready")
		}
		return nil
	}
	run, err := runStart(context.Background(), c, newStartConfig(3, []gssv1.StartMode{cold}, "a"), io.Discard)
	if err != nil {
		t.Fatalf("runStart: %v", err)
	}
	if run.Errors != 1 {
		t.Errorf("errors = %d, want 1 (the failed first cycle)", run.Errors)
	}
	if s := seriesByName(t, run, "cold_ready_ms"); s.Count != 1 {
		t.Errorf("cold_ready_ms count = %d, want 1: cycle 0 failed, cycle 1 is the warm-up, cycle 2 is recorded", s.Count)
	}
}

// A destroy that fails after a failed cycle is an error of its own.
func TestStartCountsCleanupFailure(t *testing.T) {
	f, c := newFake(t)
	f.nodes = []*gssv1.Node{node("a1", "a", gssv1.NodeState_NODE_STATE_READY, 2)}
	f.execErr = func(sb *gssv1.Sandbox) error {
		if sb.Id == "sb-1" {
			return status.Error(codes.Internal, "exec broke")
		}
		return nil
	}
	f.destroyErr = func(id string) error {
		if id == "sb-1" {
			return status.Error(codes.Unavailable, "destroy failed")
		}
		return nil
	}
	run, err := runStart(context.Background(), c, newStartConfig(3, []gssv1.StartMode{cold}, "a"), io.Discard)
	if err != nil {
		t.Fatalf("runStart: %v", err)
	}
	if run.Errors != 2 {
		t.Errorf("errors = %d, want 2 (the failed Exec and the failed destroy after it); notes %q", run.Errors, run.Notes)
	}
	if !strings.Contains(strings.Join(run.Notes, "\n"), "destroy failed") {
		t.Errorf("notes do not list the failed destroy: %q", run.Notes)
	}
	if s := seriesByName(t, run, "cold_ready_ms"); s.Count != 1 {
		t.Errorf("cold_ready_ms count = %d, want 1", s.Count)
	}
}

func TestThroughputCountsCleanupFailure(t *testing.T) {
	f, c := newFake(t)
	f.nodes = []*gssv1.Node{node("a1", "a", gssv1.NodeState_NODE_STATE_READY, 2)}
	f.execErr = func(sb *gssv1.Sandbox) error {
		if sb.Id == "sb-0" {
			return status.Error(codes.Internal, "exec broke")
		}
		return nil
	}
	f.destroyErr = func(id string) error {
		if id == "sb-0" {
			return status.Error(codes.Unavailable, "destroy failed")
		}
		return nil
	}
	run, err := runThroughput(context.Background(), c, throughputConfig{Workers: 1, Duration: 150 * time.Millisecond}, io.Discard)
	if err != nil {
		t.Fatalf("runThroughput: %v", err)
	}
	if run.Errors != 2 {
		t.Errorf("errors = %d, want 2 (the failed cycle and the failed destroy after it); notes %q", run.Errors, run.Notes)
	}
}

func densityFake(t *testing.T) (*fake, gssv1.SandboxServiceClient) {
	f, c := newFake(t)
	f.nodes = []*gssv1.Node{node("a1", "a", gssv1.NodeState_NODE_STATE_READY, 0)}
	return f, c
}

func densityConfigFor(steps []int, perStep int) densityConfig {
	return densityConfig{Steps: steps, PerStep: perStep, MemAvailableMiB: func() (float64, error) { return 1000, nil }}
}

func TestDensityStopsOnFailure(t *testing.T) {
	f, c := densityFake(t)
	f.createErr = func(_, live int, _ *gssv1.SandboxSpec) error {
		if live > 10 {
			return status.Error(codes.ResourceExhausted, "node a1 is full")
		}
		return nil
	}
	run, err := runDensity(context.Background(), c, densityConfigFor([]int{5, 10, 20, 30}, 3), io.Discard)
	if err != nil {
		t.Fatalf("runDensity: %v", err)
	}
	if len(run.Rows) != 3 {
		t.Fatalf("got %d rows, want 3 (steps 5, 10 and the failing 20): %v", len(run.Rows), run.Rows)
	}
	for i, step := range []int{5, 10, 20} {
		row := run.Rows[i]
		if row["step"] != step {
			t.Errorf("row %d step = %v, want %d", i, row["step"], step)
		}
		if row["node_mem_available_mib"] != 1000.0 {
			t.Errorf("row %d node_mem_available_mib = %v, want 1000", i, row["node_mem_available_mib"])
		}
		for _, k := range []string{"live", "start_p50_ms", "start_p95_ms", "exec_p50_ms", "exec_p95_ms", "failed"} {
			if _, ok := row[k]; !ok {
				t.Errorf("row %d has no %q: %v", i, k, row)
			}
		}
	}
	if run.Rows[0]["live"] != 5 || run.Rows[1]["live"] != 10 || run.Rows[0]["failed"] != 0 || run.Rows[1]["failed"] != 0 {
		t.Errorf("rows 0 and 1 = %v, %v: want live 5 and 10 with no failures", run.Rows[0], run.Rows[1])
	}
	if failed, _ := run.Rows[2]["failed"].(int); failed == 0 {
		t.Errorf("row for step 20 has no failures: %v", run.Rows[2])
	}
	if run.Rows[2]["live"] != 11 {
		t.Errorf("row for step 20 live = %v, want 11 (creates fail once 11 are live)", run.Rows[2]["live"])
	}
	if got := run.Scalars["max_live_within_2x"]; got != 10 {
		t.Errorf("max_live_within_2x = %v, want 10", got)
	}
	if run.Errors == 0 || !strings.Contains(strings.Join(run.Notes, "\n"), "step 20") {
		t.Errorf("errors %d notes %q: want errors counted and a note naming step 20", run.Errors, run.Notes)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, s := range f.specs {
		if s.GetStartMode() != restore || s.GetResources().GetCpuMillis() != 250 ||
			s.GetResources().GetMemoryBytes() != 192<<20 || s.GetTtl().AsDuration() != 30*time.Minute {
			t.Fatalf("create %d spec %v: want forced RESTORE, 250m, 192 MiB, TTL 30m", i, s)
		}
	}
	if len(f.live) != 0 {
		t.Errorf("%d sandboxes left live after the run", len(f.live))
	}
}

func TestDensityTwoXRule(t *testing.T) {
	f, c := densityFake(t)
	f.readyLatency = func(_, live int, _ *gssv1.SandboxSpec) time.Duration {
		switch {
		case live <= 5:
			return 10 * time.Millisecond
		case live <= 10:
			return 15 * time.Millisecond
		default:
			return 25 * time.Millisecond
		}
	}
	run, err := runDensity(context.Background(), c, densityConfigFor([]int{5, 10, 20}, 3), io.Discard)
	if err != nil {
		t.Fatalf("runDensity: %v", err)
	}
	if len(run.Rows) != 3 || run.Errors != 0 {
		t.Fatalf("rows %d errors %d, want 3 and 0; notes %v", len(run.Rows), run.Errors, run.Notes)
	}
	for i, want := range []float64{10, 15, 25} {
		if got := run.Rows[i]["start_p95_ms"]; got != want {
			t.Errorf("row %d start_p95_ms = %v, want %v", i, got, want)
		}
	}
	if got := run.Scalars["baseline_start_p95_ms"]; got != 10 {
		t.Errorf("baseline_start_p95_ms = %v, want 10", got)
	}
	if got := run.Scalars["max_live_within_2x"]; got != 10 {
		t.Errorf("max_live_within_2x = %v, want 10 (25 ms at step 20 is over 2 x 10 ms)", got)
	}
	if f.liveCount() != 0 {
		t.Errorf("%d sandboxes left live after the run", f.liveCount())
	}
}

func TestDensityNeedsOneNode(t *testing.T) {
	f, c := densityFake(t)
	f.nodes = append(f.nodes, node("a2", "a", gssv1.NodeState_NODE_STATE_READY, 0))
	_, err := runDensity(context.Background(), c, densityConfigFor([]int{5}, 1), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "exactly one node") {
		t.Fatalf("err = %v, want a message that density needs exactly one node", err)
	}
	if f.creates != 0 {
		t.Errorf("%d creates before the check failed", f.creates)
	}
}

// A kept sandbox that the scheduler restarted (a new incarnation) is lost even though
// Execs on it keep working: the step fails and the ramp stops.
func TestDensityCountsLostSandbox(t *testing.T) {
	f, c := densityFake(t)
	bumped := false
	f.tick = func(f *fake) {
		// Step 10's first restore start makes 11 live: then sb-0 (kept since step 5)
		// is restarted, as after an OOM-killed sentry.
		if sb := f.live["sb-0"]; sb != nil && len(f.live) >= 11 && !bumped {
			bumped = true
			sb.Incarnation++
		}
	}
	run, err := runDensity(context.Background(), c, densityConfigFor([]int{5, 10, 20}, 3), io.Discard)
	if err != nil {
		t.Fatalf("runDensity: %v", err)
	}
	if len(run.Rows) != 2 {
		t.Fatalf("got %d rows, want 2 (step 10 lost a sandbox and stopped the ramp): %v", len(run.Rows), run.Rows)
	}
	if run.Rows[0]["lost"] != 0 || run.Rows[0]["failed"] != 0 {
		t.Errorf("step 5 row = %v, want lost 0 and failed 0", run.Rows[0])
	}
	if run.Rows[1]["lost"] != 1 || run.Rows[1]["failed"] != 1 {
		t.Errorf("step 10 row = %v, want lost 1 and failed 1", run.Rows[1])
	}
	if got := run.Scalars["max_live_within_2x"]; got != 5 {
		t.Errorf("max_live_within_2x = %v, want 5", got)
	}
	if notes := strings.Join(run.Notes, "\n"); !strings.Contains(notes, "step 10") || !strings.Contains(notes, "lost") {
		t.Errorf("notes do not say step 10 lost a sandbox: %q", run.Notes)
	}
	if f.liveCount() != 0 {
		t.Errorf("%d sandboxes left live after the run", f.liveCount())
	}
}

func TestDensityVerdict(t *testing.T) {
	ok := func(step int, p95 float64) densityStep {
		return densityStep{step: step, live: step, start: bench.Series{Count: 3, P95: p95}}
	}
	// The first step failed but its restore starts worked: a baseline, no qualifying step.
	failedFirst := ok(5, 10)
	failedFirst.failed = 2
	base, maxLive, notes := densityVerdict([]densityStep{failedFirst})
	text := strings.Join(notes, "\n")
	if base != 10 || maxLive != 0 || !strings.Contains(text, "first step had 2 failures") || strings.Contains(text, "no successful restore start") {
		t.Errorf("failed first step: baseline %v max %d notes %q", base, maxLive, notes)
	}
	// The first step had no successful restore start: no baseline at all.
	noStarts := densityStep{step: 5, live: 5, failed: 3}
	base, maxLive, notes = densityVerdict([]densityStep{noStarts})
	if base != 0 || maxLive != 0 || !strings.Contains(strings.Join(notes, "\n"), "no successful restore start") {
		t.Errorf("no starts: baseline %v max %d notes %q", base, maxLive, notes)
	}
	// A step over 2x, then one back within it: the largest qualifying step, and a note.
	base, maxLive, notes = densityVerdict([]densityStep{ok(5, 10), ok(10, 25), ok(20, 18)})
	if base != 10 || maxLive != 20 || !strings.Contains(strings.Join(notes, "\n"), "above 2 x baseline at steps [10]") {
		t.Errorf("over then within: baseline %v max %d notes %q", base, maxLive, notes)
	}
}

func TestParseMemAvailable(t *testing.T) {
	data := []byte("MemTotal:        6036980 kB\nMemFree:         1234567 kB\nMemAvailable:    4194304 kB\n")
	got, err := parseMemAvailableMiB(data)
	if err != nil || got != 4096 {
		t.Errorf("parseMemAvailableMiB = %v, %v; want 4096", got, err)
	}
	if _, err := parseMemAvailableMiB([]byte("MemTotal: 1 kB\n")); err == nil {
		t.Error("want an error when MemAvailable is missing")
	}
}

func TestCommandLine(t *testing.T) {
	got := commandLine([]string{"bin/loadgen", "grouploss", "--kill-cmd", "scripts/cluster.sh kill-group b", "--retry=false"})
	want := "bin/loadgen grouploss --kill-cmd 'scripts/cluster.sh kill-group b' --retry=false"
	if got != want {
		t.Errorf("commandLine = %q, want %q", got, want)
	}
}

func TestStartCommandWritesResults(t *testing.T) {
	f, _ := newFake(t)
	f.nodes = []*gssv1.Node{node("a1", "a", gssv1.NodeState_NODE_STATE_READY, 2)}
	addr := serveTCP(t, f)
	out := filepath.Join(t.TempDir(), "results", "start.json")
	args := []string{"bin/loadgen", "start", "--scheduler", addr, "--n", "2", "--modes", "cold,warm", "--out", out}
	var stdout bytes.Buffer
	if err := run(context.Background(), args, &stdout, io.Discard); err != nil {
		t.Fatalf("run: %v", err)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var r bench.Run
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	if r.Command != strings.Join(args, " ") || r.Name != "start" || len(r.Series) != 6 {
		t.Errorf("command %q name %q series %d: want the command line, start and 6 series", r.Command, r.Name, len(r.Series))
	}
	if !strings.Contains(stdout.String(), "FIRST RESULT P50") || !strings.Contains(stdout.String(), "warm") {
		t.Errorf("stdout has no results table:\n%s", stdout.String())
	}
	if err := run(context.Background(), []string{"bin/loadgen", "start", "--modes", "auto"}, io.Discard, io.Discard); err == nil {
		t.Error("want an error for --modes auto")
	}
}
