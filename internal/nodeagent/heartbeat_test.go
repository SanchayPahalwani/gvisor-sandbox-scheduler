package nodeagent

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// fakeCluster is the scheduler's ClusterService as RunHeartbeats sees it.
// Register fails with each of registerErrs in turn, then answers interval.
// Heartbeat answers with beatFn, or with an empty response.
type fakeCluster struct {
	mu           sync.Mutex
	registers    []*gssv1.RegisterRequest
	beats        []*gssv1.HeartbeatRequest
	registerErrs []error
	interval     time.Duration
	beatFn       func(n int, req *gssv1.HeartbeatRequest) (*gssv1.HeartbeatResponse, error) // n counts from 1
}

func (f *fakeCluster) Register(ctx context.Context, in *gssv1.RegisterRequest, _ ...grpc.CallOption) (*gssv1.RegisterResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registers = append(f.registers, proto.Clone(in).(*gssv1.RegisterRequest))
	if len(f.registerErrs) > 0 {
		err := f.registerErrs[0]
		f.registerErrs = f.registerErrs[1:]
		return nil, err
	}
	return &gssv1.RegisterResponse{HeartbeatInterval: durationpb.New(f.interval)}, nil
}

func (f *fakeCluster) Heartbeat(ctx context.Context, in *gssv1.HeartbeatRequest, _ ...grpc.CallOption) (*gssv1.HeartbeatResponse, error) {
	f.mu.Lock()
	f.beats = append(f.beats, proto.Clone(in).(*gssv1.HeartbeatRequest))
	n, fn := len(f.beats), f.beatFn
	f.mu.Unlock()
	if fn != nil {
		return fn(n, in)
	}
	return &gssv1.HeartbeatResponse{}, nil
}

func (f *fakeCluster) counts() (registers, beats int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.registers), len(f.beats)
}

// fakeSource is a HeartbeatSource with a fixed sandbox list that records
// Destroy calls as "id/incarnation".
type fakeSource struct {
	mu        sync.Mutex
	sandboxes []*gssv1.NodeSandbox
	warm      int
	destroyed []string
}

func (s *fakeSource) List() []*gssv1.NodeSandbox {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*gssv1.NodeSandbox, len(s.sandboxes))
	for i, sb := range s.sandboxes {
		out[i] = proto.Clone(sb).(*gssv1.NodeSandbox)
	}
	return out
}

func (s *fakeSource) WarmReady() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.warm
}

func (s *fakeSource) Destroy(ctx context.Context, id string, incarnation int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.destroyed = append(s.destroyed, fmt.Sprintf("%s/%d", id, incarnation))
	return nil
}

func (s *fakeSource) Destroyed() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.destroyed)
}

var hbNode = &gssv1.NodeInfo{
	Id: "n1", Group: "g1", Address: "127.0.0.1:7101",
	Capacity: &gssv1.Resources{CpuMillis: 4000, MemoryBytes: 8 << 30}, MaxSandboxes: 16,
}

// runHeartbeats runs RunHeartbeats until the test ends, retrying Register
// every 5 ms, and returns its log.
func runHeartbeats(t *testing.T, c gssv1.ClusterServiceClient, src HeartbeatSource) *syncBuffer {
	t.Helper()
	prev, prevLog := registerRetryInterval, slog.Default()
	registerRetryInterval = 5 * time.Millisecond
	logs := &syncBuffer{}
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunHeartbeats(ctx, c, hbNode, src)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("RunHeartbeats did not return after its context ended")
		}
		registerRetryInterval = prev
		slog.SetDefault(prevLog)
	})
	return logs
}

func TestHeartbeatRegistersThenBeats(t *testing.T) {
	c := &fakeCluster{interval: 10 * time.Millisecond, registerErrs: []error{status.Error(codes.Unavailable, "connection refused")}}
	src := &fakeSource{warm: 2, sandboxes: []*gssv1.NodeSandbox{{Id: "sb-1", Incarnation: 3, StartedAs: gssv1.StartMode_START_MODE_COLD}}}
	runHeartbeats(t, c, src)
	eventually(t, "three heartbeats", func() bool {
		_, beats := c.counts()
		return beats >= 3
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.registers) != 2 {
		t.Errorf("%d Register calls, want 2 (one failure, one success)", len(c.registers))
	}
	for _, r := range c.registers {
		if !proto.Equal(r.GetNode(), hbNode) {
			t.Errorf("Register sent %v, want %v", r.GetNode(), hbNode)
		}
	}
	want := &gssv1.HeartbeatRequest{NodeId: "n1", WarmReady: 2, Sandboxes: src.List()}
	for _, b := range c.beats {
		if !proto.Equal(b, want) {
			t.Errorf("heartbeat %v, want %v", b, want)
		}
	}
}

func TestHeartbeatReRegistersOnNotFound(t *testing.T) {
	c := &fakeCluster{interval: 10 * time.Millisecond}
	c.beatFn = func(n int, req *gssv1.HeartbeatRequest) (*gssv1.HeartbeatResponse, error) {
		switch n {
		case 2:
			return nil, status.Error(codes.NotFound, "node n1 unknown")
		case 3:
			return nil, status.Error(codes.Unavailable, "scheduler restarting") // logged, retried next tick
		}
		return &gssv1.HeartbeatResponse{}, nil
	}
	logs := runHeartbeats(t, c, &fakeSource{})
	eventually(t, "heartbeats after the second Register", func() bool {
		_, beats := c.counts()
		return beats >= 5
	})
	if registers, _ := c.counts(); registers != 2 {
		t.Errorf("%d Register calls, want 2: one at start, one after NotFound", registers)
	}
	for _, want := range []string{
		`msg="the scheduler does not know this node; registering again" node=n1`,
		`msg="heartbeat failed; retrying every tick" node=n1 code=Unavailable err="scheduler restarting"`,
		`msg="heartbeats are getting through again" node=n1`,
	} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log has no %q:\n%s", want, logs)
		}
	}
}

// TestHeartbeatReportsRightAfterReRegister: after a NotFound the node
// registers and reports at once, so a restarted scheduler adopts its
// sandboxes without waiting a heartbeat interval.
func TestHeartbeatReportsRightAfterReRegister(t *testing.T) {
	c := &fakeCluster{interval: 10 * time.Millisecond}
	c.beatFn = func(n int, req *gssv1.HeartbeatRequest) (*gssv1.HeartbeatResponse, error) {
		if n == 1 {
			c.mu.Lock()
			c.interval = time.Hour // the second Register's answer: no tick during the test
			c.mu.Unlock()
			return nil, status.Error(codes.NotFound, "node n1 unknown")
		}
		return &gssv1.HeartbeatResponse{}, nil
	}
	runHeartbeats(t, c, &fakeSource{sandboxes: []*gssv1.NodeSandbox{{Id: "sb-1", Incarnation: 1}}})
	eventually(t, "the heartbeat after the second Register", func() bool {
		registers, beats := c.counts()
		return registers == 2 && beats == 2
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	if got := c.beats[1].GetSandboxes(); len(got) != 1 || got[0].GetId() != "sb-1" {
		t.Errorf("report after Register = %v, want sb-1", got)
	}
}

func TestHeartbeatDestroysListed(t *testing.T) {
	src := &fakeSource{sandboxes: []*gssv1.NodeSandbox{{Id: "sb-1", Incarnation: 3}, {Id: "sb-2", Incarnation: 1}}}
	c := &fakeCluster{interval: 10 * time.Millisecond}
	c.beatFn = func(n int, req *gssv1.HeartbeatRequest) (*gssv1.HeartbeatResponse, error) {
		if n == 1 {
			// sb-9 was not reported: its incarnation is unknown, so it is left alone.
			return &gssv1.HeartbeatResponse{DestroySandboxIds: []string{"sb-1", "sb-9"}}, nil
		}
		return &gssv1.HeartbeatResponse{}, nil
	}
	runHeartbeats(t, c, src)
	eventually(t, "the destroy of sb-1", func() bool { return len(src.Destroyed()) > 0 })
	eventually(t, "two more heartbeats", func() bool {
		_, beats := c.counts()
		return beats >= 3
	})
	if got := src.Destroyed(); !slices.Equal(got, []string{"sb-1/3"}) {
		t.Errorf("Destroy calls = %q, want [sb-1/3]", got)
	}
}

// managerSource is a real Manager whose Destroy calls are recorded.
type managerSource struct {
	*Manager
	mu        sync.Mutex
	destroyed []string
}

func (s *managerSource) Destroy(ctx context.Context, id string, incarnation int32) error {
	err := s.Manager.Destroy(ctx, id, incarnation)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.destroyed = append(s.destroyed, fmt.Sprintf("%s/%d err=%v", id, incarnation, err))
	return err
}

func (s *managerSource) Destroyed() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.destroyed)
}

func TestHeartbeatDestroyDoesNotHitNewerIncarnation(t *testing.T) {
	e := startEnv(t, nil)
	e.mustCreate(t, "sb-1", 1, nil)
	src := &managerSource{Manager: e.m}
	var createErr error
	c := &fakeCluster{interval: 10 * time.Millisecond}
	c.beatFn = func(n int, req *gssv1.HeartbeatRequest) (*gssv1.HeartbeatResponse, error) {
		if n != 1 {
			return &gssv1.HeartbeatResponse{}, nil
		}
		if len(req.GetSandboxes()) != 1 || req.GetSandboxes()[0].GetIncarnation() != 1 {
			return nil, status.Errorf(codes.Internal, "first heartbeat reported %v", req.GetSandboxes())
		}
		// The scheduler moved sb-1 back to this node as incarnation 2 while
		// this heartbeat, which reported incarnation 1, was in flight.
		_, createErr = e.create("sb-1", 2, nil)
		return &gssv1.HeartbeatResponse{DestroySandboxIds: []string{"sb-1"}}, nil
	}
	runHeartbeats(t, c, src)
	eventually(t, "the destroy of sb-1", func() bool { return len(src.Destroyed()) > 0 })
	if createErr != nil {
		t.Fatalf("Create sb-1 incarnation 2: %v", createErr)
	}
	if got := src.Destroyed(); !slices.Equal(got, []string{"sb-1/1 err=<nil>"}) {
		t.Errorf("Destroy calls = %q, want [sb-1/1 err=<nil>]", got)
	}
	if got := e.listed(); !slices.Equal(got, []string{"sb-1/2"}) {
		t.Errorf("List = %q, want [sb-1/2]", got)
	}
	if got := e.rt.Containers(); len(got) != 1 || got["sb-1_2_"] != "running" {
		t.Errorf("containers = %v, want only sb-1_2_ running", got)
	}
}

func TestManagerIsHeartbeatSource(t *testing.T) {
	e := startEnv(t, nil)
	var src HeartbeatSource = e.m
	if src.WarmReady() != 0 {
		t.Errorf("WarmReady = %d, want 0 with the warm pool off", src.WarmReady())
	}
}
