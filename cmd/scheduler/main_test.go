package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
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
