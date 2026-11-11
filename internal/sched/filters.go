package sched

type excludedFilter struct{}

func (excludedFilter) Name() string { return "Excluded" }
func (excludedFilter) Filter(req *Request, n *NodeInfo) bool {
	return !req.Exclude[n.ID]
}

type nodeReadyFilter struct{}

func (nodeReadyFilter) Name() string                        { return "NodeReady" }
func (nodeReadyFilter) Filter(_ *Request, n *NodeInfo) bool { return n.Ready }

type nodeGroupFilter struct{}

func (nodeGroupFilter) Name() string { return "NodeGroup" }
func (nodeGroupFilter) Filter(req *Request, n *NodeInfo) bool {
	return req.NodeGroup == "" || req.NodeGroup == n.Group
}

type capacityFilter struct{}

func (capacityFilter) Name() string { return "Capacity" }
func (capacityFilter) Filter(req *Request, n *NodeInfo) bool {
	// Compare against the free amount; adding would overflow for huge requests.
	return req.Resources.CPUMillis <= n.Capacity.CPUMillis-n.Allocated.CPUMillis &&
		req.Resources.MemoryBytes <= n.Capacity.MemoryBytes-n.Allocated.MemoryBytes &&
		n.Sandboxes < n.MaxSandboxes
}

// Excluded rejects node IDs listed in Request.Exclude.
func Excluded() FilterPlugin { return excludedFilter{} }

// NodeReady rejects nodes that are not ready.
func NodeReady() FilterPlugin { return nodeReadyFilter{} }

// NodeGroup rejects nodes outside the requested group.
func NodeGroup() FilterPlugin { return nodeGroupFilter{} }

// Capacity rejects nodes without room for the request.
func Capacity() FilterPlugin { return capacityFilter{} }
