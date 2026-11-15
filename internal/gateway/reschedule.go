package gateway

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Run drives the cluster until ctx ends: every TickInterval it runs failure
// detection and TTL expiry, handing lost sandboxes to the rescheduler and
// telling nodes to destroy expired ones; every RetryInterval it hands over
// every sandbox that still needs a node (spec 8.4). The end of ctx cancels
// the reschedules in flight; Run returns once they and the destroys it
// started are done. Run once per Server.
func (s *Server) Run(ctx context.Context) {
	s.mu.Lock()
	s.runCtx = ctx
	s.dispatchLocked() // what heartbeats queued before Run
	s.mu.Unlock()

	tick := time.NewTicker(s.cfg.TickInterval)
	defer tick.Stop()
	retry := time.NewTicker(s.cfg.RetryInterval)
	defer retry.Stop()
	for {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.runCtx = nil // no new reschedules
			s.mu.Unlock()
			s.reschedules.Wait()
			s.destroys.Wait()
			return
		case <-tick.C:
			s.tick(ctx)
		case <-retry.C:
			s.enqueue(s.state.NeedsPlacement()...)
		}
	}
}

// Drain begins a shutdown. Failure detection, TTL expiry and the start of
// reschedules stop at once: while the gRPC server drains it takes no new
// RPCs, heartbeats included, and a detector still running would declare
// healthy nodes dead. The reschedules already running go on to their
// normal result; the returned channel is closed once they are done.
// Cancelling Run's context ends them early.
func (s *Server) Drain() <-chan struct{} {
	s.tickMu.Lock() // a pass in progress finishes first
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
	s.tickMu.Unlock()
	done := make(chan struct{})
	go func() {
		s.reschedules.Wait() // none is added once draining is set
		close(done)
	}()
	return done
}

func (s *Server) tick(ctx context.Context) {
	s.tickMu.Lock()
	defer s.tickMu.Unlock()
	s.mu.Lock()
	draining := s.draining
	s.mu.Unlock()
	if draining {
		return
	}
	r := s.state.Tick()
	for _, id := range r.DeadNodes {
		s.log.Warn("node dead", "node", id)
	}
	if len(r.Lost) > 0 {
		s.log.Warn("sandboxes lost", "count", len(r.Lost), "sandboxes", r.Lost)
		s.enqueue(r.Lost...)
	}
	for _, x := range r.Expired {
		s.log.Info("sandbox expired", "sandbox", x.ID, "node", x.NodeID)
		if x.NodeID == "" {
			continue
		}
		addr, ok := s.state.NodeAddress(x.NodeID)
		if !ok {
			continue
		}
		s.destroys.Add(1)
		go func() {
			defer s.destroys.Done()
			s.destroyOnNode(ctx, x.NodeID, addr, x.ID, 0)
		}()
	}
}

// enqueue hands sandboxes to the rescheduler. An ID already being placed
// or queued is skipped, so no sandbox is ever placed twice at once.
func (s *Server) enqueue(ids ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		if s.busy[id] {
			continue
		}
		s.busy[id] = true
		s.queue = append(s.queue, id)
	}
	s.dispatchLocked()
}

// dispatchLocked starts queued reschedules while fewer than
// RescheduleConcurrency are in flight, Run runs and no Drain was called.
// Call with s.mu held.
func (s *Server) dispatchLocked() {
	for s.runCtx != nil && !s.draining && s.running < s.cfg.RescheduleConcurrency && len(s.queue) > 0 {
		id := s.queue[0]
		s.queue = s.queue[1:]
		s.running++
		s.reschedules.Add(1)
		go s.reschedule(s.runCtx, id)
	}
}

// reschedule places a LOST or PENDING sandbox again at the next
// incarnation (spec D23: a fresh sandbox with the same ID and spec). A
// sandbox with no node to go to stays as it is; RetryInterval brings it
// back.
func (s *Server) reschedule(ctx context.Context, id string) {
	defer s.reschedules.Done() // after the slot is handed on, so the Waits cover that
	defer func() {
		s.mu.Lock()
		delete(s.busy, id)
		s.running--
		s.dispatchLocked()
		s.mu.Unlock()
	}()
	ctx, span := startSpan(ctx, "gateway.reschedule", attribute.String("sandbox.id", id))
	_, err := s.placeAndStart(ctx, id, false)
	switch status.Code(err) {
	case codes.OK:
		span.End()
	case codes.Aborted:
		// Placed, destroyed or failed meanwhile: nothing to do.
		span.End()
	case codes.ResourceExhausted, codes.Canceled:
		s.log.Debug("sandbox not rescheduled", "sandbox", id, "err", status.Convert(err).Message())
		endSpan(span, err)
	default:
		s.log.Warn("sandbox not rescheduled", "sandbox", id, "code", status.Code(err), "err", status.Convert(err).Message())
		endSpan(span, err)
	}
}
