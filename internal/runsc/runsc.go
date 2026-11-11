// Package runsc drives gVisor's runsc command line (call D9). The node agent's
// sandbox manager is written against the Runtime interface; CLI is the real
// implementation and Fake an in-memory one for unit tests.
package runsc

import (
	"context"
	"errors"
	"io"
)

// Network is the runsc --network mode of a sandbox.
type Network string

const (
	NetworkNone    Network = "none"
	NetworkSandbox Network = "sandbox"
)

// Ref names one sandbox and the network mode it was created with. The mode is
// a top-level runsc flag and must be the same on every command for that sandbox.
type Ref struct {
	ID      string
	Network Network
}

// ExecSpec describes one process to run inside a sandbox.
type ExecSpec struct {
	Args            []string
	Env             []string // "K=V"
	Cwd             string   // default "/work"
	Stdin           io.Reader
	Stdout, Stderr  io.Writer
	InternalPIDFile string // if set, runsc writes the in-sandbox pid there
}

// DefaultCwd is the working directory of an Exec whose spec leaves Cwd empty.
const DefaultCwd = "/work"

// State is what `runsc state` reports. Status is "created", "running" or "stopped".
type State struct {
	ID     string
	Status string
	PID    int
}

// ErrNotFound is returned when the named container does not exist under the root.
var ErrNotFound = errors.New("runsc: no such container")

// Runtime is the set of runsc operations the node agent uses.
type Runtime interface {
	// Create creates the container; its stdout and stderr go to logPath.
	Create(ctx context.Context, ref Ref, bundleDir string, logPath string) error
	Start(ctx context.Context, ref Ref) error
	// Restore loads a checkpoint into a container made by Create. It uses --detach.
	Restore(ctx context.Context, ref Ref, bundleDir, imagePath string) error
	// Checkpoint saves the container to imagePath; the container is stopped afterwards.
	Checkpoint(ctx context.Context, ref Ref, imagePath string) error
	// Exec runs a process and waits for it. A nonzero exit is returned as the
	// exit code with a nil error. Cancelling ctx kills the runsc client.
	Exec(ctx context.Context, ref Ref, spec ExecSpec) (exitCode int, err error)
	// KillProcess sends SIGKILL to one in-sandbox process.
	KillProcess(ctx context.Context, ref Ref, internalPID int) error
	// Delete kills and force-deletes the container; nil if it is already gone.
	Delete(ctx context.Context, ref Ref) error
	// State returns ErrNotFound if the container does not exist.
	State(ctx context.Context, ref Ref) (State, error)
	// List returns the IDs of all containers under the root.
	List(ctx context.Context) ([]string, error)
	// Version returns the runsc release, e.g. "release-20261102.0".
	Version(ctx context.Context) (string, error)
	// RemoveRoot unmounts <root>/null-netns if mounted and removes the root directory.
	RemoveRoot(ctx context.Context) error
}
