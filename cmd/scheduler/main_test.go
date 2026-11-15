package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/sched"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestParseFlagsDefaults(t *testing.T) {
	o, err := parseFlags(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	want := options{
		listen: ":7000", nodeTimeout: 3 * time.Second, heartbeatInterval: time.Second,
		weights:    sched.Weights{BinPack: 1, WarmPool: 1, GroupAffinity: 0.5, NodeHealth: 1},
		sampleSize: 0, rescheduleConcurrency: 8, policy: "binpack",
	}
	if *o != want {
		t.Errorf("options = %+v\nwant      %+v", *o, want)
	}
	if p := newPolicy(o); p.Name != "default" || p.SampleSize != 0 {
		t.Errorf("policy %q, sample size %d; want default, 0", p.Name, p.SampleSize)
	}

	o, err = parseFlags([]string{"--policy=spread", "--sample-size=5", "--w-warm=0", "--reschedule-concurrency=2"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if o.weights.WarmPool != 0 || o.rescheduleConcurrency != 2 {
		t.Errorf("options = %+v", *o)
	}
	if p := newPolicy(o); p.Name != "spread" || p.SampleSize != 5 {
		t.Errorf("policy %q, sample size %d; want spread, 5", p.Name, p.SampleSize)
	}
}

func TestParseFlagsRejects(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--listen", "7000"}, "--listen"},
		{[]string{"--node-timeout", "0s"}, "positive"},
		{[]string{"--heartbeat-interval", "-1s"}, "positive"},
		{[]string{"--heartbeat-interval", "3s"}, "shorter than --node-timeout"},
		{[]string{"--w-binpack", "-1"}, "--w-binpack"},
		{[]string{"--w-health", "-0.5"}, "--w-health"},
		{[]string{"--sample-size", "-1"}, "--sample-size"},
		{[]string{"--reschedule-concurrency", "0"}, "--reschedule-concurrency"},
		{[]string{"--policy", "random"}, "--policy"},
		{[]string{"extra"}, "unexpected arguments"},
	} {
		var out strings.Builder
		_, err := parseFlags(tc.args, &out)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("parseFlags(%q) = %v, want an error about %q", tc.args, err, tc.want)
			continue
		}
		if !strings.Contains(out.String(), err.Error()) {
			t.Errorf("parseFlags(%q) printed %q, not the error", tc.args, out.String())
		}
	}
}

// syncBuffer is a log destination written by run's goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

var listenRE = regexp.MustCompile(`msg="scheduler serving" listen=(\S+)`)

// TestRunServesBothServices: one server answers ClusterService and
// SandboxService, and a cancelled context is a clean stop.
func TestRunServesBothServices(t *testing.T) {
	o, err := parseFlags([]string{"--listen", "127.0.0.1:0"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopped := false
	done := make(chan error, 1)
	go func() { done <- run(ctx, func() { stopped = true }, o) }()
	var addr string
	deadline := time.Now().Add(5 * time.Second)
	for addr == "" {
		if m := listenRE.FindStringSubmatch(logs.String()); m != nil {
			addr = m[1]
		} else if time.Now().After(deadline) {
			t.Fatalf("no serving line in %q", logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
	defer rcancel()
	node := &gssv1.NodeInfo{Id: "n1", Group: "a", Address: "127.0.0.1:7101",
		Capacity: &gssv1.Resources{CpuMillis: 4000, MemoryBytes: 2 << 30}, MaxSandboxes: 16}
	resp, err := gssv1.NewClusterServiceClient(conn).Register(rctx, &gssv1.RegisterRequest{Node: node})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetHeartbeatInterval().AsDuration() != time.Second {
		t.Errorf("heartbeat interval = %v, want 1s", resp.GetHeartbeatInterval().AsDuration())
	}
	nodes, err := gssv1.NewSandboxServiceClient(conn).ListNodes(rctx, &gssv1.ListNodesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes.GetNodes()) != 1 || nodes.GetNodes()[0].GetState() != gssv1.NodeState_NODE_STATE_READY {
		t.Errorf("ListNodes = %v, want n1 READY", nodes)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("run after cancel = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after its context ended")
	}
	if !stopped {
		t.Error("run did not restore the default signal handling")
	}
	for _, want := range []string{`msg="node registered" node=n1 group=a`, `msg="scheduler stopped"`} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log has no %q:\n%s", want, logs.String())
		}
	}
}

// blockingNode is a NodeService whose CreateSandbox waits for release
// (closed at once for a node that answers at once).
type blockingNode struct {
	gssv1.UnimplementedNodeServiceServer
	started chan struct{} // closed when the first Create arrives
	release chan struct{}
	once    sync.Once
	result  chan error // what each Create ended with
}

func newBlockingNode() *blockingNode {
	return &blockingNode{started: make(chan struct{}), release: make(chan struct{}), result: make(chan error, 8)}
}

func (b *blockingNode) CreateSandbox(ctx context.Context, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
	b.once.Do(func() { close(b.started) })
	select {
	case <-b.release:
	case <-ctx.Done():
		b.result <- ctx.Err()
		return nil, ctx.Err()
	}
	b.result <- nil
	return &gssv1.NodeCreateResponse{StartedAs: gssv1.StartMode_START_MODE_COLD}, nil
}

// serveNode serves b on a loopback port until the test ends and returns
// the address.
func serveNode(t *testing.T, b *blockingNode) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	gssv1.RegisterNodeServiceServer(g, b)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	return lis.Addr().String()
}

func nodeInfo(id, addr string) *gssv1.NodeInfo {
	return &gssv1.NodeInfo{Id: id, Group: "a", Address: addr,
		Capacity: &gssv1.Resources{CpuMillis: 4000, MemoryBytes: 2 << 30}, MaxSandboxes: 16}
}

// TestShutdownStopsFailureDetection: while the server drains an RPC in
// flight no heartbeat gets through, so failure detection must already be
// off: the node is not declared dead and the create in flight completes.
func TestShutdownStopsFailureDetection(t *testing.T) {
	fake := newBlockingNode()
	nodeAddr := serveNode(t, fake)

	// A node is dead after 200 ms without a heartbeat.
	o, err := parseFlags([]string{"--listen", "127.0.0.1:0", "--node-timeout", "200ms", "--heartbeat-interval", "100ms"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, func() {}, o) }()
	addr := waitListen(t, logs)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rcancel()
	if _, err := gssv1.NewClusterServiceClient(conn).Register(rctx, &gssv1.RegisterRequest{Node: nodeInfo("n1", nodeAddr)}); err != nil {
		t.Fatal(err)
	}
	created := make(chan *gssv1.Sandbox, 1)
	go func() {
		sb, err := gssv1.NewSandboxServiceClient(conn).CreateSandbox(rctx, &gssv1.CreateSandboxRequest{})
		if err != nil {
			t.Errorf("CreateSandbox: %v", err)
		}
		created <- sb
	}()
	<-fake.started
	// The node is alive when shutdown starts, however slow the test was so far.
	if _, err := gssv1.NewClusterServiceClient(conn).Heartbeat(rctx, &gssv1.HeartbeatRequest{NodeId: "n1"}); err != nil {
		t.Fatal(err)
	}
	cancel() // the signal: shutdown starts with the create still in flight
	time.Sleep(time.Second)
	close(fake.release)
	if sb := <-created; sb.GetState() != gssv1.SandboxState_SANDBOX_STATE_READY {
		t.Errorf("the create drained during shutdown returned %v, want READY", sb)
	}
	if err := <-done; err != nil {
		t.Errorf("run = %v", err)
	}
	if strings.Contains(logs.String(), `msg="node dead"`) {
		t.Errorf("a node was declared dead during the drain:\n%s", logs.String())
	}
}

// TestShutdownLetsReschedulesFinish: a reschedule running when shutdown
// starts is not cancelled; it finishes within the drain timeout with its
// normal result.
func TestShutdownLetsReschedulesFinish(t *testing.T) {
	n1, n2 := newBlockingNode(), newBlockingNode()
	close(n1.release) // n1 answers at once; n2 holds its Create
	a1, a2 := serveNode(t, n1), serveNode(t, n2)

	o, err := parseFlags([]string{"--listen", "127.0.0.1:0", "--node-timeout", "200ms", "--heartbeat-interval", "100ms"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, func() {}, o) }()
	conn, err := grpc.NewClient(waitListen(t, logs), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cluster := gssv1.NewClusterServiceClient(conn)
	rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rcancel()
	for _, n := range []*gssv1.NodeInfo{nodeInfo("n1", a1), nodeInfo("n2", a2)} {
		if _, err := cluster.Register(rctx, &gssv1.RegisterRequest{Node: n}); err != nil {
			t.Fatal(err)
		}
	}
	sb, err := gssv1.NewSandboxServiceClient(conn).CreateSandbox(rctx, &gssv1.CreateSandboxRequest{})
	if err != nil || sb.GetNodeId() != "n1" {
		t.Fatalf("CreateSandbox = %v, %v; want it on n1", sb, err)
	}

	// Only n2 heartbeats: n1 is declared dead and the sandbox moves to n2.
	beat := func() error {
		_, err := cluster.Heartbeat(rctx, &gssv1.HeartbeatRequest{NodeId: "n2"})
		return err
	}
	stopBeats := make(chan struct{})
	var beats sync.WaitGroup
	beats.Add(1)
	go func() {
		defer beats.Done()
		for {
			select {
			case <-stopBeats:
				return
			case <-time.After(50 * time.Millisecond):
				_ = beat()
			}
		}
	}()
	select {
	case <-n2.started:
	case <-time.After(5 * time.Second):
		t.Fatal("no reschedule reached n2")
	}
	close(stopBeats)
	beats.Wait()
	if err := beat(); err != nil { // n2 alive when shutdown starts
		t.Fatal(err)
	}
	cancel() // the signal, with the reschedule's Create on n2 in flight
	time.Sleep(time.Second)
	close(n2.release)
	if err := <-done; err != nil {
		t.Errorf("run = %v", err)
	}
	if err := <-n2.result; err != nil {
		t.Errorf("the reschedule's Create on n2 ended with %v, want it left to finish", err)
	}
	if want := `msg="sandbox rescheduled" sandbox=` + sb.GetId() + ` node=n2 incarnation=2`; !strings.Contains(logs.String(), want) {
		t.Errorf("log has no %q:\n%s", want, logs.String())
	}
	if strings.Contains(logs.String(), `msg="node dead" node=n2`) {
		t.Errorf("n2 was declared dead during the drain:\n%s", logs.String())
	}
}

// waitListen returns the address from run's serving line.
func waitListen(t *testing.T, logs *syncBuffer) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if m := listenRE.FindStringSubmatch(logs.String()); m != nil {
			return m[1]
		}
		if time.Now().After(deadline) {
			t.Fatalf("no serving line in %q", logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
}
