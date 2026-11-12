package cluster

import (
	"sort"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/sched"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// HeartbeatResult is the outcome of reconciling one heartbeat.
type HeartbeatResult struct {
	Known   bool     // false: node must Register again
	Destroy []string // sandbox IDs the node must destroy
	Lost    []string // sandboxes that vanished from this node's reports
}

// Heartbeat reconciles a node's report with the sandbox table (spec 8.5).
func (s *State) Heartbeat(req *gssv1.HeartbeatRequest) HeartbeatResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	nodeID := req.GetNodeId()
	n := s.nodes[nodeID]
	if n == nil {
		return HeartbeatResult{}
	}
	now := s.now()
	r := HeartbeatResult{Known: true}
	if n.state == gssv1.NodeState_NODE_STATE_DEAD {
		// Its sandboxes were released when it died.
		n.state = gssv1.NodeState_NODE_STATE_READY
		n.allocated, n.count = sched.Resources{}, 0
	}
	n.lastHB = now
	n.warm = max(0, int(req.GetWarmReady()))

	seen := map[string]bool{}
	for _, ns := range req.GetSandboxes() {
		id := ns.GetId()
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		if _, ok := s.tombstones[id]; ok {
			r.Destroy = append(r.Destroy, id)
			continue
		}
		e := s.sandboxes[id]
		if e == nil {
			s.adopt(ns, nodeID, n, now)
			continue
		}
		if !s.reportedHere(e, nodeID, ns.GetIncarnation()) && !s.readopt(e, ns, nodeID, n, now) {
			r.Destroy = append(r.Destroy, id)
		}
	}

	for _, e := range s.sandboxes {
		if e.sb.State != gssv1.SandboxState_SANDBOX_STATE_READY || e.sb.NodeId != nodeID || seen[e.sb.Id] {
			continue
		}
		e.misses++
		if e.misses >= s.cfg.MissedReports {
			s.lose(e)
			r.Lost = append(r.Lost, e.sb.Id)
		}
	}
	sort.Strings(r.Destroy)
	sort.Strings(r.Lost)
	return r
}

// adopt records a sandbox the node reports but the state does not know, as
// READY on that node. This is how the table is rebuilt after a scheduler
// restart. Call with s.mu held.
func (s *State) adopt(ns *gssv1.NodeSandbox, nodeID string, n *nodeEntry, now time.Time) {
	s.seq++
	e := &sandboxEntry{seq: s.seq, sb: &gssv1.Sandbox{
		Id:          ns.GetId(),
		Spec:        cloneSpec(ns.GetSpec()),
		State:       gssv1.SandboxState_SANDBOX_STATE_READY,
		NodeId:      nodeID,
		NodeGroup:   n.info.GetGroup(),
		StartedAs:   ns.GetStartedAs(),
		Incarnation: ns.GetIncarnation(),
		CreatedAt:   timestamppb.New(now),
		ExpiresAt:   timestamppb.New(now.Add(ttlOf(ns.GetSpec()))),
	}}
	e.adopted = true
	s.sandboxes[e.sb.Id] = e
	s.allocate(e, nodeID, n)
	s.notify()
}

// readopt moves an adopted sandbox to the node now reporting it at a higher
// incarnation. After a scheduler restart, the first node to report a
// sandbox is the one adopted; a healed node with a stale copy can report
// before the node holding the newer incarnation. The stale node is told to
// destroy its copy at its next heartbeat. A sandbox this scheduler placed
// is never moved this way. It returns false, changing nothing, when the
// rule does not apply. Call with s.mu held.
func (s *State) readopt(e *sandboxEntry, ns *gssv1.NodeSandbox, nodeID string, n *nodeEntry, now time.Time) bool {
	st := e.sb.State
	if !e.adopted || e.sb.NodeId == nodeID || ns.GetIncarnation() <= e.sb.Incarnation ||
		(st != gssv1.SandboxState_SANDBOX_STATE_READY && st != gssv1.SandboxState_SANDBOX_STATE_LOST) {
		return false
	}
	s.allocate(e, nodeID, n) // releases the old node's share first
	e.sb.State = gssv1.SandboxState_SANDBOX_STATE_READY
	e.sb.NodeId = nodeID
	e.sb.NodeGroup = n.info.GetGroup()
	e.sb.Incarnation = ns.GetIncarnation()
	e.sb.StartedAs = ns.GetStartedAs()
	e.sb.ExpiresAt = timestamppb.New(now.Add(ttlOf(ns.GetSpec())))
	e.sb.Message = ""
	e.misses = 0
	s.notify()
	return true
}

// reportedHere handles a report of a known sandbox. It returns true, and
// resets the miss counter, only when the state has the sandbox STARTING or
// READY on this node at the reported incarnation. Otherwise the node must
// destroy its copy: the sandbox is on another node, at another incarnation,
// or LOST, PENDING, FAILED or DESTROYED. A LOST sandbox is never restored
// here; it is rescheduled as a fresh incarnation (spec D19, D23), and
// destroying the old copy frees the node for it. Call with s.mu held.
func (s *State) reportedHere(e *sandboxEntry, nodeID string, incarnation int32) bool {
	if e.sb.NodeId != nodeID || e.sb.Incarnation != incarnation {
		return false
	}
	switch e.sb.State {
	case gssv1.SandboxState_SANDBOX_STATE_READY, gssv1.SandboxState_SANDBOX_STATE_STARTING:
		e.misses = 0
		return true
	}
	return false
}
