package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
)

// removeLeftovers deletes what an earlier agent with this node ID left
// behind (call D13: the agent keeps no state across restarts): every
// container under the runsc root, every process and cgroup below the node's
// cgroup, and every bundle and exec pid directory. Network leftovers belong
// to the network policy's own start-up.
func (m *Manager) removeLeftovers(ctx context.Context) error {
	ids, err := m.rt.List(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, id := range ids {
		// The network flag of a leftover is unknown; delete does not depend on it.
		if err := m.rt.Delete(ctx, runsc.Ref{ID: id, Network: runsc.NetworkNone}); err != nil {
			errs = append(errs, err)
		}
	}
	if err := m.cg.KillTree(nodeCgroup(m.cfg.NodeID)); err != nil {
		errs = append(errs, err)
	}
	// runsc removes, when it deletes a container, every cgroup directory it
	// created for it. If the first sandbox of a node created /gss/<node>,
	// deleting that sandbox while others live fails (EBUSY on the node's
	// cgroup). With the directory made here, runsc creates only the leaf.
	if err := m.cg.Ensure(nodeCgroup(m.cfg.NodeID)); err != nil {
		errs = append(errs, err)
	}
	if err := clearDir(m.bundlesDir()); err != nil {
		errs = append(errs, err)
	}
	if err := clearDir(m.execsDir()); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// nodeCgroup is the parent of every sandbox cgroup of a node.
func nodeCgroup(nodeID string) string { return "/gss/" + nodeID }

// clearDir creates dir if needed (mode 0700) and removes everything in it.
func clearDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var errs []error
	for _, ent := range ents {
		if err := os.RemoveAll(filepath.Join(dir, ent.Name())); err != nil {
			errs = append(errs, fmt.Errorf("remove leftover bundle: %w", err))
		}
	}
	return errors.Join(errs...)
}
