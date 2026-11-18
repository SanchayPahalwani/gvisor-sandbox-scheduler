// Package cluster is the scheduler's in-memory cluster state: the node
// registry, the sandbox table, failure detection and heartbeat
// reconciliation. It does no I/O, starts no
// goroutines and reads time only from the injected clock.
package cluster

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/sched"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Config holds the failure-detection and health parameters.
type Config struct {
	NodeTimeout   time.Duration // a READY node silent for longer than this is DEAD
	TombstoneTTL  time.Duration // how long a destroyed or failed ID is remembered
	MissedReports int           // consecutive heartbeats a READY sandbox may be absent from
	HealthAlpha   float64       // EWMA weight of the newest Create result
}

// DefaultConfig returns the values from the spec. Tombstones last the
// longest TTL plus the node reaper's 10 s grace, as long as a copy of a
// destroyed sandbox can live on a node cut off from the scheduler, so a
// report of that copy is answered with a destroy instead of being adopted.
func DefaultConfig() Config {
	return Config{
		NodeTimeout:   3 * time.Second,
		TombstoneTTL:  gssv1.MaxTTL + 10*time.Second,
		MissedReports: 2,
		HealthAlpha:   0.2,
	}
}

// TickResult is what one failure-detection pass changed.
type TickResult struct {
	DeadNodes []string
	Lost      []string
	Expired   []Expired
}

// Expired is a sandbox whose TTL ran out; NodeID is where it ran, if anywhere.
type Expired struct{ ID, NodeID string }

type nodeEntry struct {
	info      *gssv1.NodeInfo
	state     gssv1.NodeState
	allocated sched.Resources
	count     int
	warm      int
	health    float64
	lastHB    time.Time
}

type sandboxEntry struct {
	sb        *gssv1.Sandbox
	seq       uint64 // insertion order, breaks created_at ties
	misses    int
	allocNode string // node holding this sandbox's allocation, "" if none
	adopted   bool   // known from a heartbeat, not placed by this scheduler (yet)
}

// State is the cluster state. All methods are safe for concurrent use.
type State struct {
	cfg Config
	now func() time.Time

	mu         sync.Mutex
	nodes      map[string]*nodeEntry
	sandboxes  map[string]*sandboxEntry
	tombstones map[string]time.Time // ID -> when it was destroyed or failed
	seq        uint64
	changed    chan struct{} // closed and replaced on every sandbox state change
}

// New returns an empty State that reads time from now.
func New(cfg Config, now func() time.Time) *State {
	return &State{
		cfg:        cfg,
		now:        now,
		nodes:      map[string]*nodeEntry{},
		sandboxes:  map[string]*sandboxEntry{},
		tombstones: map[string]time.Time{},
		changed:    make(chan struct{}),
	}
}

// resourcesOf returns the spec's resources; a nil spec or nil resources is zero.
func resourcesOf(spec *gssv1.SandboxSpec) sched.Resources {
	r := spec.GetResources()
	return sched.Resources{CPUMillis: r.GetCpuMillis(), MemoryBytes: r.GetMemoryBytes()}
}

// ttlOf returns the spec's TTL, or the default when it is unset.
func ttlOf(spec *gssv1.SandboxSpec) time.Duration {
	if spec.GetTtl() == nil {
		return gssv1.DefaultTTL
	}
	return spec.GetTtl().AsDuration()
}

func live(st gssv1.SandboxState) bool {
	return st != gssv1.SandboxState_SANDBOX_STATE_DESTROYED && st != gssv1.SandboxState_SANDBOX_STATE_FAILED
}

// notify wakes WaitReady callers. Call with s.mu held.
func (s *State) notify() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// allocate charges e's resources to node n. Call with s.mu held.
func (s *State) allocate(e *sandboxEntry, nodeID string, n *nodeEntry) {
	s.release(e)
	r := resourcesOf(e.sb.Spec)
	n.allocated.CPUMillis += r.CPUMillis
	n.allocated.MemoryBytes += r.MemoryBytes
	n.count++
	e.allocNode = nodeID
}

// release returns e's allocation to its node, at most once. Call with s.mu held.
func (s *State) release(e *sandboxEntry) {
	if e.allocNode == "" {
		return
	}
	if n := s.nodes[e.allocNode]; n != nil {
		r := resourcesOf(e.sb.Spec)
		n.allocated.CPUMillis = max(0, n.allocated.CPUMillis-r.CPUMillis)
		n.allocated.MemoryBytes = max(0, n.allocated.MemoryBytes-r.MemoryBytes)
		n.count = max(0, n.count-1)
	}
	e.allocNode = ""
}

// finish moves e to a terminal state, releases it and tombstones it. Call with s.mu held.
func (s *State) finish(e *sandboxEntry, st gssv1.SandboxState, msg string) {
	s.release(e)
	e.sb.State = st
	e.sb.Message = msg
	e.misses = 0
	s.tombstones[e.sb.Id] = s.now()
	s.notify()
}

// lose marks e LOST and releases its allocation. Call with s.mu held.
func (s *State) lose(e *sandboxEntry) {
	s.release(e)
	e.sb.State = gssv1.SandboxState_SANDBOX_STATE_LOST
	e.misses = 0
	s.notify()
}

// Register adds or updates a node. It becomes READY with its last heartbeat
// set to now; health starts at 1 the first time the node is seen.
func (s *State) Register(n *gssv1.NodeInfo) {
	if n == nil || n.Id == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.nodes[n.Id]
	if e == nil {
		e = &nodeEntry{health: 1}
		s.nodes[n.Id] = e
	}
	if e.state == gssv1.NodeState_NODE_STATE_DEAD {
		// Everything on it was released when it died.
		e.allocated, e.count = sched.Resources{}, 0
	}
	e.info = proto.Clone(n).(*gssv1.NodeInfo)
	e.state = gssv1.NodeState_NODE_STATE_READY
	e.lastHB = s.now()
}

// NodeAddress returns the address a node registered with.
func (s *State) NodeAddress(nodeID string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.nodes[nodeID]
	if e == nil {
		return "", false
	}
	return e.info.GetAddress(), true
}

func (s *State) sortedNodeIDs() []string {
	ids := make([]string, 0, len(s.nodes))
	for id := range s.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Nodes returns copies of all nodes, sorted by ID.
func (s *State) Nodes() []*gssv1.Node {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*gssv1.Node, 0, len(s.nodes))
	for _, id := range s.sortedNodeIDs() {
		e := s.nodes[id]
		out = append(out, &gssv1.Node{
			Info:  proto.Clone(e.info).(*gssv1.NodeInfo),
			State: e.state,
			Allocated: &gssv1.Resources{
				CpuMillis: e.allocated.CPUMillis, MemoryBytes: e.allocated.MemoryBytes,
			},
			Sandboxes:     int32(e.count),
			WarmReady:     int32(e.warm),
			Health:        e.health,
			LastHeartbeat: timestamppb.New(e.lastHB),
		})
	}
	return out
}

// RecordResult folds the outcome of a Create call into the node's health.
func (s *State) RecordResult(nodeID string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.nodes[nodeID]
	if e == nil {
		return
	}
	v := 0.0
	if ok {
		v = 1
	}
	a := s.cfg.HealthAlpha
	e.health = (1-a)*e.health + a*v
}

// View returns the placement view of every node, sorted by ID, and, when
// spec has an affinity key, the number of live sandboxes with that key in
// each node group.
func (s *State) View(spec *gssv1.SandboxSpec) (nodes []*sched.NodeInfo, affinityByGroup map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nodes = make([]*sched.NodeInfo, 0, len(s.nodes))
	for _, id := range s.sortedNodeIDs() {
		e := s.nodes[id]
		nodes = append(nodes, &sched.NodeInfo{
			ID:    id,
			Group: e.info.GetGroup(),
			Ready: e.state == gssv1.NodeState_NODE_STATE_READY,
			Capacity: sched.Resources{
				CPUMillis:   e.info.GetCapacity().GetCpuMillis(),
				MemoryBytes: e.info.GetCapacity().GetMemoryBytes(),
			},
			Allocated:    e.allocated,
			MaxSandboxes: int(e.info.GetMaxSandboxes()),
			Sandboxes:    e.count,
			WarmReady:    e.warm,
			Health:       e.health,
		})
	}
	key := spec.GetAffinityKey()
	if key == "" {
		return nodes, nil
	}
	affinityByGroup = map[string]int{}
	for _, e := range s.sandboxes {
		if live(e.sb.State) && e.sb.NodeGroup != "" && e.sb.Spec.GetAffinityKey() == key {
			affinityByGroup[e.sb.NodeGroup]++
		}
	}
	return nodes, affinityByGroup
}

// Add records a new sandbox in PENDING at incarnation 0 and returns a copy.
func (s *State) Add(id string, spec *gssv1.SandboxSpec) *gssv1.Sandbox {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if old := s.sandboxes[id]; old != nil {
		s.release(old)
	}
	delete(s.tombstones, id)
	s.seq++
	e := &sandboxEntry{seq: s.seq, sb: &gssv1.Sandbox{
		Id:        id,
		Spec:      cloneSpec(spec),
		State:     gssv1.SandboxState_SANDBOX_STATE_PENDING,
		CreatedAt: timestamppb.New(now),
		ExpiresAt: timestamppb.New(now.Add(ttlOf(spec))),
	}}
	s.sandboxes[id] = e
	s.notify()
	return proto.Clone(e.sb).(*gssv1.Sandbox)
}

func cloneSpec(spec *gssv1.SandboxSpec) *gssv1.SandboxSpec {
	if spec == nil {
		return nil
	}
	return proto.Clone(spec).(*gssv1.SandboxSpec)
}

// Reserve claims room for a PENDING or LOST sandbox on a READY node and
// moves it to STARTING at the next incarnation. remainingTTL is the time
// left until the sandbox expires.
func (s *State) Reserve(id, nodeID string, p *gssv1.Placement) (incarnation int32, remainingTTL time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.sandboxes[id]
	if e == nil {
		return 0, 0, fmt.Errorf("sandbox %s not found", id)
	}
	if st := e.sb.State; st != gssv1.SandboxState_SANDBOX_STATE_PENDING && st != gssv1.SandboxState_SANDBOX_STATE_LOST {
		return 0, 0, fmt.Errorf("sandbox %s is %v, not PENDING or LOST", id, st)
	}
	remainingTTL = e.sb.ExpiresAt.AsTime().Sub(s.now())
	if remainingTTL <= 0 {
		return 0, 0, fmt.Errorf("sandbox %s has expired", id)
	}
	n := s.nodes[nodeID]
	if n == nil {
		return 0, 0, fmt.Errorf("node %s not found", nodeID)
	}
	if n.state != gssv1.NodeState_NODE_STATE_READY {
		return 0, 0, fmt.Errorf("node %s is not ready", nodeID)
	}
	r := resourcesOf(e.sb.Spec)
	capacity := n.info.GetCapacity()
	if r.CPUMillis > capacity.GetCpuMillis()-n.allocated.CPUMillis ||
		r.MemoryBytes > capacity.GetMemoryBytes()-n.allocated.MemoryBytes ||
		n.count >= int(n.info.GetMaxSandboxes()) {
		return 0, 0, fmt.Errorf("node %s has no room for sandbox %s", nodeID, id)
	}
	s.allocate(e, nodeID, n)
	e.adopted = false // placed by this scheduler from now on
	e.sb.State = gssv1.SandboxState_SANDBOX_STATE_STARTING
	e.sb.NodeId = nodeID
	e.sb.NodeGroup = n.info.GetGroup()
	e.sb.Incarnation++
	e.sb.Message = ""
	e.sb.Placement = nil
	if p != nil {
		e.sb.Placement = proto.Clone(p).(*gssv1.Placement)
	}
	e.misses = 0
	s.notify()
	return e.sb.Incarnation, remainingTTL, nil
}

// startingAt returns the entry if it is STARTING at incarnation. Call with s.mu held.
func (s *State) startingAt(id string, incarnation int32) *sandboxEntry {
	e := s.sandboxes[id]
	if e == nil || e.sb.State != gssv1.SandboxState_SANDBOX_STATE_STARTING || e.sb.Incarnation != incarnation {
		return nil
	}
	return e
}

// Commit marks a STARTING sandbox READY. It returns false, changing nothing,
// when the sandbox is no longer STARTING at that incarnation.
func (s *State) Commit(id string, incarnation int32, startedAs gssv1.StartMode, latency time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.startingAt(id, incarnation)
	if e == nil {
		return false
	}
	e.sb.State = gssv1.SandboxState_SANDBOX_STATE_READY
	e.sb.StartedAs = startedAs
	e.sb.StartLatency = durationpb.New(latency)
	e.misses = 0
	if startedAs == gssv1.StartMode_START_MODE_WARM {
		if n := s.nodes[e.sb.NodeId]; n != nil && n.warm > 0 {
			n.warm--
		}
	}
	s.notify()
	return true
}

// Unreserve returns a STARTING sandbox at incarnation to PENDING and
// releases its reservation. The incarnation is kept.
func (s *State) Unreserve(id string, incarnation int32, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.startingAt(id, incarnation)
	if e == nil {
		return
	}
	s.release(e)
	e.sb.State = gssv1.SandboxState_SANDBOX_STATE_PENDING
	e.sb.NodeId = ""
	e.sb.NodeGroup = ""
	e.sb.Message = msg
	s.notify()
}

// Fail moves a PENDING or LOST sandbox to FAILED with msg and tombstones it.
// In any other state, or for an unknown ID, it changes nothing and returns
// false, so a stale caller cannot fail a sandbox that was placed meanwhile.
func (s *State) Fail(id, msg string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.sandboxes[id]
	if e == nil {
		return false
	}
	if st := e.sb.State; st != gssv1.SandboxState_SANDBOX_STATE_PENDING && st != gssv1.SandboxState_SANDBOX_STATE_LOST {
		return false
	}
	s.finish(e, gssv1.SandboxState_SANDBOX_STATE_FAILED, msg)
	return true
}

// Destroy moves a sandbox to DESTROYED and tombstones it. nodeID is where it
// was placed, if anywhere; existed is false if it was unknown or already
// destroyed.
func (s *State) Destroy(id string) (nodeID string, existed bool) { return s.DestroyUpTo(id, 0) }

// DestroyUpTo is Destroy for incarnation and older ones (0: any), the rule
// of DestroySandboxRequest.incarnation and of the node agent's Destroy: a
// sandbox at a higher incarnation is left alone, and existed is false.
func (s *State) DestroyUpTo(id string, incarnation int32) (nodeID string, existed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.sandboxes[id]
	if e == nil || e.sb.State == gssv1.SandboxState_SANDBOX_STATE_DESTROYED ||
		(incarnation != 0 && e.sb.Incarnation > incarnation) {
		return "", false
	}
	nodeID = e.sb.NodeId
	if e.sb.State == gssv1.SandboxState_SANDBOX_STATE_FAILED {
		nodeID = ""
	}
	s.finish(e, gssv1.SandboxState_SANDBOX_STATE_DESTROYED, "destroyed")
	return nodeID, true
}

// Get returns a copy of the sandbox.
func (s *State) Get(id string) (*gssv1.Sandbox, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.sandboxes[id]
	if e == nil {
		return nil, false
	}
	return proto.Clone(e.sb).(*gssv1.Sandbox), true
}

// List returns copies of the live sandboxes (not DESTROYED or FAILED), sorted by ID.
func (s *State) List() []*gssv1.Sandbox {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []*gssv1.Sandbox{}
	for _, e := range s.sandboxes {
		if live(e.sb.State) {
			out = append(out, proto.Clone(e.sb).(*gssv1.Sandbox))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Id < out[j].Id })
	return out
}

// WaitReady blocks until the sandbox is READY and returns a copy of it. It
// returns NotFound if the sandbox is unknown or DESTROYED, FailedPrecondition
// with the sandbox's message if it FAILED, and ctx's error if ctx ends first.
func (s *State) WaitReady(ctx context.Context, id string) (*gssv1.Sandbox, error) {
	for {
		s.mu.Lock()
		e := s.sandboxes[id]
		switch {
		case e == nil || e.sb.State == gssv1.SandboxState_SANDBOX_STATE_DESTROYED:
			s.mu.Unlock()
			return nil, status.Errorf(codes.NotFound, "sandbox %s not found", id)
		case e.sb.State == gssv1.SandboxState_SANDBOX_STATE_FAILED:
			msg := e.sb.Message
			s.mu.Unlock()
			return nil, status.Error(codes.FailedPrecondition, msg)
		case e.sb.State == gssv1.SandboxState_SANDBOX_STATE_READY:
			sb := proto.Clone(e.sb).(*gssv1.Sandbox)
			s.mu.Unlock()
			return sb, nil
		}
		ch := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		}
	}
}

// Tick runs failure detection: TTL expiry, dead nodes and tombstone cleanup.
// Call it every 250ms.
func (s *State) Tick() TickResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var r TickResult

	// Expiry first, so a sandbox that is both expired and on a dead node is
	// reported once, as expired.
	for _, e := range s.sandboxes {
		if live(e.sb.State) && !e.sb.ExpiresAt.AsTime().After(now) {
			r.Expired = append(r.Expired, Expired{ID: e.sb.Id, NodeID: e.sb.NodeId})
			s.finish(e, gssv1.SandboxState_SANDBOX_STATE_DESTROYED, "ttl expired")
		}
	}
	sort.Slice(r.Expired, func(i, j int) bool { return r.Expired[i].ID < r.Expired[j].ID })

	for id, n := range s.nodes {
		if n.state == gssv1.NodeState_NODE_STATE_READY && now.Sub(n.lastHB) > s.cfg.NodeTimeout {
			n.state = gssv1.NodeState_NODE_STATE_DEAD
			r.DeadNodes = append(r.DeadNodes, id)
		}
	}
	sort.Strings(r.DeadNodes)
	if len(r.DeadNodes) > 0 {
		dead := map[string]bool{}
		for _, id := range r.DeadNodes {
			dead[id] = true
		}
		for _, e := range s.sandboxes {
			st := e.sb.State
			if dead[e.sb.NodeId] && (st == gssv1.SandboxState_SANDBOX_STATE_STARTING || st == gssv1.SandboxState_SANDBOX_STATE_READY) {
				s.lose(e)
				r.Lost = append(r.Lost, e.sb.Id)
			}
		}
		sort.Strings(r.Lost)
	}

	for id, at := range s.tombstones {
		if now.Sub(at) > s.cfg.TombstoneTTL {
			delete(s.tombstones, id)
			if e := s.sandboxes[id]; e != nil && !live(e.sb.State) {
				delete(s.sandboxes, id)
			}
		}
	}
	return r
}

// NeedsPlacement returns the IDs the rescheduler should place: LOST
// sandboxes and PENDING ones that have been placed before, oldest first.
func (s *State) NeedsPlacement() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var es []*sandboxEntry
	for _, e := range s.sandboxes {
		st := e.sb.State
		if st == gssv1.SandboxState_SANDBOX_STATE_LOST ||
			(st == gssv1.SandboxState_SANDBOX_STATE_PENDING && e.sb.Incarnation > 0) {
			es = append(es, e)
		}
	}
	sort.Slice(es, func(i, j int) bool {
		a, b := es[i].sb.CreatedAt.AsTime(), es[j].sb.CreatedAt.AsTime()
		if !a.Equal(b) {
			return a.Before(b)
		}
		return es[i].seq < es[j].seq
	})
	ids := make([]string, len(es))
	for i, e := range es {
		ids[i] = e.sb.Id
	}
	return ids
}
