// Package gateway is the scheduler's gRPC face (spec 4 and 8): SandboxService
// for clients and ClusterService for node agents. It places sandboxes with
// sched, records them in cluster, starts them on node agents, proxies Exec
// (spec D8) and restarts the sandboxes of dead nodes elsewhere (spec 8.4).
package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/cluster"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/nodeagent"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/sched"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Config tunes the gateway. New replaces zero or negative fields with
// DefaultConfig's.
type Config struct {
	MaxAttempts           int           // node Create calls per placement before giving up
	RescheduleConcurrency int           // reschedules in flight at once (spec 8.4)
	ExecWaitCap           time.Duration // longest an Exec waits for its sandbox to be READY
	TickInterval          time.Duration // failure detection and TTL expiry period
	RetryInterval         time.Duration // how often sandboxes without a node are placed again
	HeartbeatInterval     time.Duration // told to nodes at Register
	CreateTimeout         time.Duration // per node Create call
}

// DefaultConfig returns the values from the spec.
func DefaultConfig() Config {
	return Config{
		MaxAttempts:           3,
		RescheduleConcurrency: 8,
		ExecWaitCap:           15 * time.Second,
		TickInterval:          250 * time.Millisecond,
		RetryInterval:         time.Second,
		HeartbeatInterval:     time.Second,
		CreateTimeout:         60 * time.Second,
	}
}

// destroyTimeout bounds each node Destroy the gateway sends. A Destroy that
// fails is finished by the node's next heartbeat, whose answer lists the ID.
const destroyTimeout = 5 * time.Second

// Server serves SandboxService and ClusterService over one cluster.State.
type Server struct {
	gssv1.UnimplementedSandboxServiceServer
	gssv1.UnimplementedClusterServiceServer

	state  *cluster.State
	policy *sched.Policy
	nodes  NodeClient
	cfg    Config
	log    *slog.Logger
	newID  func() (string, error)

	mu      sync.Mutex
	busy    map[string]bool // IDs being placed (a create or a reschedule) or queued to be: one placement per ID at a time
	queue   []string        // reschedules waiting for a slot, in arrival order
	running int             // reschedules in flight
	runCtx  context.Context // Run's context while Run runs, else nil
	wg      sync.WaitGroup  // goroutines Run waits for: reschedules and destroys of expired sandboxes
}

// New returns a gateway. policy is shared by every placement (it
// serialises its own calls). The server logs to slog's default logger as
// it is when New is called. Run must run for failure detection, TTL expiry
// and rescheduling.
func New(state *cluster.State, policy *sched.Policy, nodes NodeClient, cfg Config) *Server {
	d := DefaultConfig()
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = d.MaxAttempts
	}
	if cfg.RescheduleConcurrency <= 0 {
		cfg.RescheduleConcurrency = d.RescheduleConcurrency
	}
	for _, f := range []struct{ v, d *time.Duration }{
		{&cfg.ExecWaitCap, &d.ExecWaitCap}, {&cfg.TickInterval, &d.TickInterval}, {&cfg.RetryInterval, &d.RetryInterval},
		{&cfg.HeartbeatInterval, &d.HeartbeatInterval}, {&cfg.CreateTimeout, &d.CreateTimeout},
	} {
		if *f.v <= 0 {
			*f.v = *f.d
		}
	}
	return &Server{
		state: state, policy: policy, nodes: nodes, cfg: cfg,
		log: slog.Default(), newID: newSandboxID,
		busy: map[string]bool{},
	}
}

// ServerOptions are the scheduler's gRPC server options: the otelgrpc stats
// handler (a span per RPC, continuing the caller's trace) and the node
// agent's message limits, nodeagent.MaxMessageBytes, since the scheduler
// relays the same requests and output on both hops.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(nodeagent.MaxMessageBytes),
		grpc.MaxSendMsgSize(nodeagent.MaxMessageBytes),
	}
}

// newSandboxID returns "sb-" and 12 random lowercase hex characters, which
// the node agent accepts as a sandbox ID.
func newSandboxID() (string, error) {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "sb-" + hex.EncodeToString(b), nil
}

func startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	// Looked up on each use so that a provider set later takes effect.
	return otel.Tracer("gss").Start(ctx, name, trace.WithAttributes(attrs...))
}

// endSpan records err, if any, on the span and ends it.
func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
	}
	span.End()
}

// stateName is a sandbox state without its enum prefix: READY, LOST...
func stateName(st gssv1.SandboxState) string {
	return strings.TrimPrefix(st.String(), "SANDBOX_STATE_")
}

func needsNode(st gssv1.SandboxState) bool {
	return st == gssv1.SandboxState_SANDBOX_STATE_PENDING || st == gssv1.SandboxState_SANDBOX_STATE_LOST
}

func live(st gssv1.SandboxState) bool {
	return st != gssv1.SandboxState_SANDBOX_STATE_DESTROYED && st != gssv1.SandboxState_SANDBOX_STATE_FAILED
}

// claimNewID returns a new sandbox ID, held in s.busy.
func (s *Server) claimNewID() (string, error) {
	for range 3 {
		id, err := s.newID()
		if err != nil {
			return "", err
		}
		if _, taken := s.state.Get(id); taken {
			continue
		}
		s.mu.Lock()
		taken := s.busy[id]
		s.busy[id] = true
		s.mu.Unlock()
		if !taken {
			return id, nil
		}
	}
	return "", errors.New("no unused sandbox ID after 3 tries")
}

func (s *Server) unclaim(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.busy, id)
}

// CreateSandbox records a new sandbox and starts it on a node.
func (s *Server) CreateSandbox(ctx context.Context, req *gssv1.CreateSandboxRequest) (_ *gssv1.Sandbox, err error) {
	ctx, span := startSpan(ctx, "gateway.create")
	defer func() { endSpan(span, err) }()

	spec := &gssv1.SandboxSpec{}
	if req.GetSpec() != nil {
		spec = proto.Clone(req.GetSpec()).(*gssv1.SandboxSpec)
	}
	gssv1.ApplyDefaults(spec)
	if err := gssv1.ValidateSpec(spec); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if _, ok := gssv1.StartMode_name[int32(spec.GetStartMode())]; !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown start_mode %d", spec.GetStartMode())
	}
	id, err := s.claimNewID()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "sandbox ID: %v", err)
	}
	defer s.unclaim(id)
	span.SetAttributes(attribute.String("sandbox.id", id))
	s.state.Add(id, spec)
	return s.placeAndStart(ctx, id, true)
}

// placeAndStart places sandbox id and starts it on the chosen node (spec
// 8.3, binding), trying up to MaxAttempts nodes. fresh marks a client's
// create: a sandbox that cannot be started is then FAILED, and the error is
// the RPC's. For a reschedule the state is left for the retry loop. The
// caller holds id in s.busy. Every error is a gRPC status: Aborted when the
// sandbox no longer needs a node, ResourceExhausted when no node fits,
// Unavailable when the node Creates failed.
func (s *Server) placeAndStart(ctx context.Context, id string, fresh bool) (*gssv1.Sandbox, error) {
	begin := time.Now()
	exclude := map[string]bool{}
	var failed []string  // "<node>: <error>" per failed node Create
	var refused []string // Reserve errors
	for {
		if err := ctx.Err(); err != nil {
			return nil, s.fail(id, fresh, status.FromContextError(err).Err())
		}
		sb, ok := s.state.Get(id)
		if !ok || !needsNode(sb.GetState()) {
			st := "gone"
			if ok {
				st = stateName(sb.GetState())
			}
			return nil, status.Errorf(codes.Aborted, "sandbox %s is %s", id, st)
		}
		d, err := s.place(ctx, sb, exclude)
		if err != nil {
			if len(failed) > 0 {
				return nil, s.createFailed(id, fresh, failed)
			}
			msg := err.Error()
			if len(refused) > 0 {
				msg += " (" + strings.Join(refused, "; ") + ")"
			}
			return nil, s.fail(id, fresh, status.Error(codes.ResourceExhausted, msg))
		}
		inc, ttl, err := s.state.Reserve(id, d.NodeID, placement(d))
		if err != nil {
			// The state changed since the view: another create took the room,
			// the node died, or the sandbox was destroyed (seen at the top).
			refused = append(refused, err.Error())
			exclude[d.NodeID] = true
			continue
		}
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("node.id", d.NodeID), attribute.Int("incarnation", int(inc)))
		addr, _ := s.state.NodeAddress(d.NodeID)
		resp, err := s.start(ctx, addr, id, sb.GetSpec(), inc, ttl)
		if err != nil {
			code, msg := status.Code(err), status.Convert(err).Message()
			s.state.Unreserve(id, inc, fmt.Sprintf("create on node %s failed: %s", d.NodeID, msg))
			if ctx.Err() != nil {
				// The caller went away; the node did not fail.
				return nil, s.fail(id, fresh, status.FromContextError(ctx.Err()).Err())
			}
			if code == codes.FailedPrecondition && fresh {
				// A forced start mode the node cannot serve: no other node is
				// tried, and the node is not at fault.
				return nil, s.fail(id, fresh, err)
			}
			if code != codes.FailedPrecondition {
				s.state.RecordResult(d.NodeID, false)
			}
			s.log.Warn("node create failed", "sandbox", id, "node", d.NodeID, "incarnation", inc, "code", code, "err", msg)
			failed = append(failed, d.NodeID+": "+msg)
			exclude[d.NodeID] = true
			if len(failed) >= s.cfg.MaxAttempts {
				return nil, s.createFailed(id, fresh, failed)
			}
			continue
		}
		s.state.RecordResult(d.NodeID, true)
		if !s.state.Commit(id, inc, resp.GetStartedAs(), resp.GetReadyLatency().AsDuration()) {
			// Destroyed, expired or lost while it started: this copy is
			// stale. A newer incarnation on the same node is left alone.
			s.destroyOnNode(ctx, d.NodeID, addr, id, inc)
			sb, ok := s.state.Get(id)
			if !ok || !live(sb.GetState()) {
				return nil, status.Errorf(codes.Aborted, "sandbox %s was destroyed while it started", id)
			}
			return sb, nil
		}
		msg := "sandbox placed"
		if !fresh {
			msg = "sandbox rescheduled"
		}
		s.log.Info(msg, "sandbox", id, "node", d.NodeID, "incarnation", inc,
			"mode", modeName(resp.GetStartedAs()), "ms", time.Since(begin).Milliseconds(),
			"ready_ms", resp.GetReadyLatency().AsDuration().Milliseconds())
		sb, _ = s.state.Get(id)
		return sb, nil
	}
}

// fail ends a placement with err. A client's create also moves the sandbox
// to FAILED with err's message.
func (s *Server) fail(id string, fresh bool, err error) error {
	if fresh {
		s.state.Fail(id, status.Convert(err).Message())
	}
	return err
}

// createFailed is the error once node Creates failed on every node tried.
func (s *Server) createFailed(id string, fresh bool, failed []string) error {
	return s.fail(id, fresh, status.Errorf(codes.Unavailable, "sandbox %s: create failed on %d node(s): %s",
		id, len(failed), strings.Join(failed, "; ")))
}

// place runs the policy over the current view, traced as sched.place.
func (s *Server) place(ctx context.Context, sb *gssv1.Sandbox, exclude map[string]bool) (*sched.Decision, error) {
	_, span := startSpan(ctx, "sched.place")
	spec := sb.GetSpec()
	nodes, affinity := s.state.View(spec)
	mode := spec.GetStartMode()
	req := &sched.Request{
		Resources: sched.Resources{
			CPUMillis:   spec.GetResources().GetCpuMillis(),
			MemoryBytes: spec.GetResources().GetMemoryBytes(),
		},
		NodeGroup:       spec.GetNodeGroup(),
		AffinityKey:     spec.GetAffinityKey(),
		AffinityByGroup: affinity,
		WantsWarm: len(spec.GetEgressAllow()) == 0 &&
			(mode == gssv1.StartMode_START_MODE_AUTO || mode == gssv1.StartMode_START_MODE_WARM),
		Exclude: exclude,
	}
	d, err := s.policy.Place(req, nodes)
	if err != nil {
		var nf *sched.NoFitError
		if errors.As(err, &nf) {
			span.SetAttributes(attribute.Int("nodes.total", nf.Total), attribute.Int("nodes.feasible", 0))
		}
		endSpan(span, err)
		return nil, err
	}
	attrs := []attribute.KeyValue{
		attribute.Int("nodes.total", d.Total),
		attribute.Int("nodes.feasible", d.Feasible),
		attribute.String("node.chosen", d.NodeID),
	}
	if len(d.Top) > 0 {
		names := make([]string, 0, len(d.Top[0].Plugins))
		for name := range d.Top[0].Plugins {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			attrs = append(attrs, attribute.Float64("score."+name, d.Top[0].Plugins[name]))
		}
	}
	span.SetAttributes(attrs...)
	span.End()
	return d, nil
}

// placement is the decision as the Sandbox message carries it.
func placement(d *sched.Decision) *gssv1.Placement {
	p := &gssv1.Placement{NodesTotal: int32(d.Total), NodesFeasible: int32(d.Feasible)}
	for _, c := range d.Top {
		p.Top = append(p.Top, &gssv1.NodeScore{NodeId: c.NodeID, Total: c.Total, Plugins: c.Plugins})
	}
	return p
}

// start calls the node's Create for incarnation inc, with the spec's TTL
// replaced by what is left of it.
func (s *Server) start(ctx context.Context, addr, id string, spec *gssv1.SandboxSpec, inc int32, ttl time.Duration) (*gssv1.NodeCreateResponse, error) {
	sp := &gssv1.SandboxSpec{}
	if spec != nil {
		sp = proto.Clone(spec).(*gssv1.SandboxSpec)
	}
	sp.Ttl = durationpb.New(ttl)
	cctx, cancel := context.WithTimeout(ctx, s.cfg.CreateTimeout)
	defer cancel()
	return s.nodes.Create(cctx, addr, &gssv1.NodeCreateRequest{SandboxId: id, Spec: sp, Incarnation: inc})
}

// destroyOnNode asks a node to destroy a sandbox (incarnation 0: whatever
// is live there), bounded by destroyTimeout and not by ctx's cancellation.
// A failure is logged: the node's next heartbeat answer lists the ID again.
func (s *Server) destroyOnNode(ctx context.Context, nodeID, addr, id string, inc int32) {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), destroyTimeout)
	defer cancel()
	if err := s.nodes.Destroy(dctx, addr, id, inc); err != nil {
		s.log.Warn("node destroy failed", "sandbox", id, "incarnation", inc, "node", nodeID,
			"code", status.Code(err), "err", status.Convert(err).Message())
	}
}

func modeName(m gssv1.StartMode) string {
	return strings.ToLower(strings.TrimPrefix(m.String(), "START_MODE_"))
}

// errLost ends an Exec's node stream once the scheduler has the sandbox
// LOST or elsewhere.
var errLost = errors.New("sandbox lost")

// Exec waits for the sandbox to be READY and proxies the node's Exec
// stream (spec D8, D34): events as they come; the node's status if the
// stream ends with one; Unavailable if the sandbox is lost on the way.
// No exit event is ever made up.
func (s *Server) Exec(req *gssv1.ExecRequest, stream grpc.ServerStreamingServer[gssv1.ExecEvent]) (err error) {
	ctx, span := startSpan(stream.Context(), "gateway.exec", attribute.String("sandbox.id", req.GetSandboxId()))
	defer func() { endSpan(span, err) }()

	sb, err := s.waitReady(ctx, req.GetSandboxId())
	if err != nil {
		return err
	}
	span.SetAttributes(attribute.String("node.id", sb.GetNodeId()), attribute.Int("incarnation", int(sb.GetIncarnation())))
	lost := fmt.Sprintf("sandbox %s lost on node %s", sb.GetId(), sb.GetNodeId())
	addr, ok := s.state.NodeAddress(sb.GetNodeId())
	if !ok {
		return status.Error(codes.Unavailable, lost)
	}
	nctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	go s.watchLost(nctx, cancel, sb)

	ns, err := s.nodes.Exec(nctx, addr, req)
	for err == nil {
		var ev *gssv1.ExecEvent
		if ev, err = ns.Recv(); err == nil {
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
	}
	if err == io.EOF {
		return nil
	}
	if errors.Is(context.Cause(nctx), errLost) {
		return status.Error(codes.Unavailable, lost)
	}
	st := status.Convert(err)
	if st.Code() == codes.Unavailable {
		// The node went away (the transport failed) or the node itself says
		// the sandbox stopped: either way this copy is lost.
		return status.Errorf(codes.Unavailable, "%s: %s", lost, st.Message())
	}
	return st.Err()
}

// waitReady waits for the sandbox to be READY, bounded by ctx and ExecWaitCap.
func (s *Server) waitReady(ctx context.Context, id string) (*gssv1.Sandbox, error) {
	wctx, cancel := context.WithTimeout(ctx, s.cfg.ExecWaitCap)
	defer cancel()
	sb, err := s.state.WaitReady(wctx, id)
	switch {
	case err == nil:
		return sb, nil
	case ctx.Err() != nil:
		return nil, status.FromContextError(ctx.Err()).Err()
	case wctx.Err() != nil:
		st := "gone"
		if cur, ok := s.state.Get(id); ok {
			st = stateName(cur.GetState())
		}
		return nil, status.Errorf(codes.DeadlineExceeded, "sandbox %s is %s: not ready after %v", id, st, s.cfg.ExecWaitCap)
	}
	return nil, err
}

// watchLost cancels ctx with errLost once the state no longer has sb READY
// on its node at its incarnation (LOST, or already moved). grpc-go will not
// ping a connection more often than every 10 s, so without this an Exec to
// a node that vanished without closing its connection would hang for that
// long; with it, the node timeout bounds it (spec 8.2).
func (s *Server) watchLost(ctx context.Context, cancel context.CancelCauseFunc, sb *gssv1.Sandbox) {
	t := time.NewTicker(s.cfg.TickInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		cur, ok := s.state.Get(sb.GetId())
		if !ok || cur.GetState() == gssv1.SandboxState_SANDBOX_STATE_LOST ||
			cur.GetNodeId() != sb.GetNodeId() || cur.GetIncarnation() != sb.GetIncarnation() {
			cancel(errLost)
			return
		}
	}
}

// DestroySandbox destroys a sandbox, whatever incarnation is live. The
// node's Destroy is bounded by destroyTimeout; if it fails, the node's next
// heartbeat answer lists the ID. An unknown ID is not an error.
func (s *Server) DestroySandbox(ctx context.Context, req *gssv1.DestroySandboxRequest) (*gssv1.DestroySandboxResponse, error) {
	id := req.GetSandboxId()
	nodeID, existed := s.state.Destroy(id)
	if existed && nodeID != "" {
		if addr, ok := s.state.NodeAddress(nodeID); ok {
			s.destroyOnNode(ctx, nodeID, addr, id, 0)
		}
	}
	if existed {
		s.log.Info("sandbox destroyed", "sandbox", id, "node", nodeID)
	}
	return &gssv1.DestroySandboxResponse{}, nil
}

func (s *Server) GetSandbox(ctx context.Context, req *gssv1.GetSandboxRequest) (*gssv1.Sandbox, error) {
	sb, ok := s.state.Get(req.GetSandboxId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "sandbox %s not found", req.GetSandboxId())
	}
	return sb, nil
}

func (s *Server) ListSandboxes(ctx context.Context, req *gssv1.ListSandboxesRequest) (*gssv1.ListSandboxesResponse, error) {
	return &gssv1.ListSandboxesResponse{Sandboxes: s.state.List()}, nil
}

func (s *Server) ListNodes(ctx context.Context, req *gssv1.ListNodesRequest) (*gssv1.ListNodesResponse, error) {
	return &gssv1.ListNodesResponse{Nodes: s.state.Nodes()}, nil
}

// Register adds or updates a node and tells it how often to heartbeat.
func (s *Server) Register(ctx context.Context, req *gssv1.RegisterRequest) (*gssv1.RegisterResponse, error) {
	n := req.GetNode()
	if n.GetId() == "" || n.GetAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "node id and address are required")
	}
	s.state.Register(n)
	s.log.Info("node registered", "node", n.GetId(), "group", n.GetGroup(), "address", n.GetAddress(),
		"cpu_millis", n.GetCapacity().GetCpuMillis(), "memory_bytes", n.GetCapacity().GetMemoryBytes(),
		"max_sandboxes", n.GetMaxSandboxes())
	return &gssv1.RegisterResponse{HeartbeatInterval: durationpb.New(s.cfg.HeartbeatInterval)}, nil
}

// Heartbeat reconciles a node's report (spec 8.5). An unknown node gets
// NotFound and registers again (spec D18). Sandboxes missing from the
// node's reports go to the rescheduler.
func (s *Server) Heartbeat(ctx context.Context, req *gssv1.HeartbeatRequest) (*gssv1.HeartbeatResponse, error) {
	r := s.state.Heartbeat(req)
	if !r.Known {
		return nil, status.Errorf(codes.NotFound, "node %q is not registered", req.GetNodeId())
	}
	if len(r.Lost) > 0 {
		s.log.Warn("sandboxes missing from heartbeats", "node", req.GetNodeId(), "sandboxes", r.Lost)
		s.enqueue(r.Lost...)
	}
	return &gssv1.HeartbeatResponse{DestroySandboxIds: r.Destroy}, nil
}
