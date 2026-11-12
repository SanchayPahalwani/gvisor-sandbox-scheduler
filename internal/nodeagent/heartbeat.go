package nodeagent

import (
	"context"
	"log/slog"
	"sync"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// HeartbeatSource is what RunHeartbeats reports and acts on; a Manager is one.
type HeartbeatSource interface {
	List() []*gssv1.NodeSandbox
	WarmReady() int
	Destroy(ctx context.Context, id string, incarnation int32) error
}

var _ HeartbeatSource = (*Manager)(nil)

// registerRetryInterval is how often a failed Register is retried; tests
// shorten it.
var registerRetryInterval = time.Second

const (
	// defaultHeartbeatInterval is used when Register's answer has none.
	defaultHeartbeatInterval = time.Second
	// heartbeatRPCTimeout bounds one Register or Heartbeat call.
	heartbeatRPCTimeout = 5 * time.Second
	// heartbeatDestroyTimeout bounds one destroy the scheduler asked for.
	heartbeatDestroyTimeout = time.Minute
)

// SchedulerDialOptions are the gRPC client options of a node agent's
// connection to the scheduler: plaintext (spec D7), the otelgrpc client
// handler, and a reconnect backoff of at most a second (grpc-go's default
// grows to two minutes), so that the node registers again within a
// heartbeat or two of a scheduler restart, however long it was down.
func SchedulerDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second},
			MinConnectTimeout: 5 * time.Second,
		}),
	}
}

// RunHeartbeats registers with the scheduler and heartbeats until ctx ends
// (spec 8.2). Register is retried every second until it succeeds. Each
// heartbeat reports m.List() and m.WarmReady(); a NotFound answer (the
// scheduler restarted and does not know the node) makes it register again.
// Each ID in an answer's destroy list is destroyed in the background, at
// most the incarnation that heartbeat reported, so an instruction about an
// old copy cannot remove a newer incarnation created meanwhile. Other
// errors are logged, to slog's default logger as it is when RunHeartbeats
// is called, and the next tick tries again. RunHeartbeats returns once ctx
// has ended and the destroys it started are done.
func RunHeartbeats(ctx context.Context, client gssv1.ClusterServiceClient, info *gssv1.NodeInfo, m HeartbeatSource) {
	h := &heartbeater{client: client, info: info, m: m, log: slog.Default(), destroying: map[string]bool{}}
	defer h.wg.Wait()
	interval, ok := h.register(ctx)
	if !ok {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if !h.beat(ctx) {
			// NotFound: the scheduler lost track of this node. Register,
			// then report at once so it adopts the sandboxes without
			// waiting a tick.
			if interval, ok = h.register(ctx); !ok {
				return
			}
			t.Reset(interval)
			h.beat(ctx)
		}
	}
}

type heartbeater struct {
	client  gssv1.ClusterServiceClient
	info    *gssv1.NodeInfo
	m       HeartbeatSource
	log     *slog.Logger
	failing bool // the last heartbeat failed; logged once until one succeeds

	mu         sync.Mutex
	destroying map[string]bool // IDs with a destroy in flight
	wg         sync.WaitGroup
}

// register calls Register until it succeeds, every registerRetryInterval,
// and returns the heartbeat interval. ok is false if ctx ended first.
func (h *heartbeater) register(ctx context.Context) (interval time.Duration, ok bool) {
	logged := false
	for {
		rctx, cancel := context.WithTimeout(ctx, heartbeatRPCTimeout)
		resp, err := h.client.Register(rctx, &gssv1.RegisterRequest{Node: h.info})
		cancel()
		if err == nil {
			interval = resp.GetHeartbeatInterval().AsDuration()
			if interval <= 0 {
				interval = defaultHeartbeatInterval
			}
			h.log.Info("registered with the scheduler", "node", h.info.GetId(), "heartbeat_interval", interval)
			h.failing = false
			return interval, true
		}
		if ctx.Err() != nil {
			return 0, false
		}
		if !logged {
			h.log.Warn("register with the scheduler failed; retrying", "node", h.info.GetId(),
				"code", status.Code(err), "err", status.Convert(err).Message(), "every", registerRetryInterval)
			logged = true
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-time.After(registerRetryInterval):
		}
	}
}

// beat sends one heartbeat and starts the destroys its answer asks for. It
// returns false only when the scheduler answered NotFound.
func (h *heartbeater) beat(ctx context.Context) bool {
	list := h.m.List()
	reported := make(map[string]int32, len(list))
	for _, sb := range list {
		reported[sb.GetId()] = sb.GetIncarnation()
	}
	rctx, cancel := context.WithTimeout(ctx, heartbeatRPCTimeout)
	resp, err := h.client.Heartbeat(rctx, &gssv1.HeartbeatRequest{
		NodeId: h.info.GetId(), Sandboxes: list, WarmReady: int32(h.m.WarmReady()),
	})
	cancel()
	if status.Code(err) == codes.NotFound {
		h.log.Info("the scheduler does not know this node; registering again", "node", h.info.GetId())
		return false
	}
	if err != nil {
		if !h.failing && ctx.Err() == nil {
			h.log.Warn("heartbeat failed; retrying every tick", "node", h.info.GetId(),
				"code", status.Code(err), "err", status.Convert(err).Message())
		}
		h.failing = true
		return true
	}
	if h.failing {
		h.log.Info("heartbeats are getting through again", "node", h.info.GetId())
		h.failing = false
	}
	for _, id := range resp.GetDestroySandboxIds() {
		inc, ok := reported[id]
		if !ok {
			// Not in this report: its incarnation is unknown, and a destroy
			// of any incarnation could hit one the scheduler wants.
			continue
		}
		h.destroy(ctx, id, inc)
	}
	return true
}

// destroy destroys id up to incarnation inc in the background, unless a
// destroy of id is already running.
func (h *heartbeater) destroy(ctx context.Context, id string, inc int32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.destroying[id] {
		return
	}
	h.destroying[id] = true
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), heartbeatDestroyTimeout)
		defer cancel()
		if err := h.m.Destroy(dctx, id, inc); err != nil {
			h.log.Warn("destroy asked for by the scheduler failed", "node", h.info.GetId(), "sandbox", id,
				"incarnation", inc, "code", status.Code(err), "err", status.Convert(err).Message())
		} else {
			h.log.Info("destroyed at the scheduler's request", "node", h.info.GetId(), "sandbox", id, "incarnation", inc)
		}
		h.mu.Lock()
		delete(h.destroying, id)
		h.mu.Unlock()
	}()
}
