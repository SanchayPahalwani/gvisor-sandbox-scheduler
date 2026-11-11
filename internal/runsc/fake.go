package runsc

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Fake is an in-memory Runtime for unit tests. Set its fields before use;
// Calls and Containers are safe to read while it is in use. The zero value
// is usable.
//
// It follows the CLI where a manager could notice: a done ctx fails every
// method before it acts, Exec in a container that is not running returns
// ErrNotRunning, a stopped container's PID is -1, and KillProcess fails for
// a pid that is not a live Exec of that container. An Exec that returns
// because its ctx ended leaves its pid alive, as the process inside a real
// sandbox keeps running; KillProcess or Delete releases it.
type Fake struct {
	// ExecFunc runs one Exec; default: exit 0. ctx is the Exec's ctx, so a
	// test can block until it ends.
	ExecFunc   func(ctx context.Context, ref Ref, spec ExecSpec) (int, error)
	Fail       map[string]error         // method name -> error to return once
	Delay      map[string]time.Duration // method name -> sleep before acting
	VersionStr string

	mu         sync.Mutex
	calls      []string
	containers map[string]*fakeContainer
	nextPID    int
}

type fakeContainer struct {
	status string
	pid    int
	execs  map[int]bool // live in-sandbox pids handed out by Exec
}

var _ Runtime = (*Fake)(nil)

// NewFake returns an empty Fake whose Exec exits 0.
func NewFake() *Fake {
	return &Fake{
		Fail:       map[string]error{},
		Delay:      map[string]time.Duration{},
		VersionStr: "release-fake",
		containers: map[string]*fakeContainer{},
	}
}

// Calls returns the calls made so far, in order, e.g. "Create id=a net=none".
func (f *Fake) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// Containers returns a copy of the container table: id -> status.
func (f *Fake) Containers() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]string, len(f.containers))
	for id, c := range f.containers {
		out[id] = c.status
	}
	return out
}

// enter records the call, fails on a done ctx, applies Delay and returns a
// one-shot Fail error.
func (f *Fake) enter(ctx context.Context, method string, ref *Ref) error {
	f.mu.Lock()
	call := method
	if ref != nil {
		call = fmt.Sprintf("%s id=%s net=%s", method, ref.ID, ref.Network)
	}
	f.calls = append(f.calls, call)
	delay := f.Delay[method]
	f.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	if delay > 0 {
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.Fail[method]; ok {
		delete(f.Fail, method)
		return err
	}
	if f.containers == nil {
		f.containers = map[string]*fakeContainer{}
	}
	return nil
}

// pid hands out a new fake pid. f.mu must be held.
func (f *Fake) pid() int {
	f.nextPID++
	return 1000 + f.nextPID
}

// get returns the container for ref. f.mu must be held.
func (f *Fake) get(method string, ref Ref) (*fakeContainer, error) {
	c, ok := f.containers[ref.ID]
	if !ok {
		return nil, fmt.Errorf("fake %s %s: %w", method, ref.ID, ErrNotFound)
	}
	return c, nil
}

// transition moves ref from status from to status to.
func (f *Fake) transition(method string, ref Ref, from, to string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, err := f.get(method, ref)
	if err != nil {
		return err
	}
	if c.status != from {
		return fmt.Errorf("fake %s %s: container is %s, want %s", method, ref.ID, c.status, from)
	}
	c.status = to
	if to == "stopped" {
		c.pid = -1
		c.execs = map[int]bool{}
	}
	return nil
}

func (f *Fake) Create(ctx context.Context, ref Ref, bundleDir string, logPath string) error {
	if err := f.enter(ctx, "Create", &ref); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.containers[ref.ID]; ok {
		return fmt.Errorf("fake Create %s: container already exists", ref.ID)
	}
	f.containers[ref.ID] = &fakeContainer{status: "created", pid: f.pid(), execs: map[int]bool{}}
	return nil
}

func (f *Fake) Start(ctx context.Context, ref Ref) error {
	if err := f.enter(ctx, "Start", &ref); err != nil {
		return err
	}
	return f.transition("Start", ref, "created", "running")
}

func (f *Fake) Restore(ctx context.Context, ref Ref, bundleDir, imagePath string) error {
	if err := f.enter(ctx, "Restore", &ref); err != nil {
		return err
	}
	return f.transition("Restore", ref, "created", "running")
}

func (f *Fake) Checkpoint(ctx context.Context, ref Ref, imagePath string) error {
	if err := f.enter(ctx, "Checkpoint", &ref); err != nil {
		return err
	}
	return f.transition("Checkpoint", ref, "running", "stopped")
}

// Exec allocates an in-sandbox pid, writes it to InternalPIDFile when set,
// and runs ExecFunc. The pid is released when ExecFunc returns, unless the
// Exec's ctx has ended by then: Exec then returns the ctx error and the pid
// stays alive until KillProcess or Delete.
func (f *Fake) Exec(ctx context.Context, ref Ref, spec ExecSpec) (int, error) {
	if err := f.enter(ctx, "Exec", &ref); err != nil {
		return -1, err
	}
	f.mu.Lock()
	c, err := f.get("Exec", ref)
	if err == nil && c.status != "running" {
		err = fmt.Errorf("fake Exec %s: container is %s: %w", ref.ID, c.status, ErrNotRunning)
	}
	var pid int
	if err == nil {
		pid = f.pid()
		c.execs[pid] = true
	}
	f.mu.Unlock()
	if err != nil {
		return -1, err
	}
	release := func() {
		f.mu.Lock()
		delete(c.execs, pid)
		f.mu.Unlock()
	}
	if spec.InternalPIDFile != "" {
		if err := os.WriteFile(spec.InternalPIDFile, []byte(strconv.Itoa(pid)), 0o644); err != nil {
			release()
			return -1, fmt.Errorf("fake Exec %s: %w", ref.ID, err)
		}
	}
	code, err := 0, error(nil)
	if f.ExecFunc != nil {
		code, err = f.ExecFunc(ctx, ref, spec)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return -1, fmt.Errorf("fake Exec %s: %w", ref.ID, ctxErr)
	}
	release()
	return code, err
}

// KillProcess fails unless internalPID is a live Exec of a running
// container; the pid is dead afterwards (ExecFunc keeps running).
func (f *Fake) KillProcess(ctx context.Context, ref Ref, internalPID int) error {
	if err := f.enter(ctx, "KillProcess", &ref); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	c, err := f.get("KillProcess", ref)
	if err != nil {
		return err
	}
	if c.status != "running" {
		return fmt.Errorf("fake KillProcess %s: container is %s", ref.ID, c.status)
	}
	if !c.execs[internalPID] {
		return fmt.Errorf("fake KillProcess %s: no such process with PID %d", ref.ID, internalPID)
	}
	delete(c.execs, internalPID)
	return nil
}

func (f *Fake) Delete(ctx context.Context, ref Ref) error {
	if err := f.enter(ctx, "Delete", &ref); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.containers, ref.ID)
	return nil
}

func (f *Fake) State(ctx context.Context, ref Ref) (State, error) {
	if err := f.enter(ctx, "State", &ref); err != nil {
		return State{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	c, err := f.get("State", ref)
	if err != nil {
		return State{}, err
	}
	return State{ID: ref.ID, Status: c.status, PID: c.pid}, nil
}

func (f *Fake) List(ctx context.Context) ([]string, error) {
	if err := f.enter(ctx, "List", nil); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, 0, len(f.containers))
	for id := range f.containers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func (f *Fake) Version(ctx context.Context) (string, error) {
	if err := f.enter(ctx, "Version", nil); err != nil {
		return "", err
	}
	return f.VersionStr, nil
}

func (f *Fake) RemoveRoot(ctx context.Context) error {
	if err := f.enter(ctx, "RemoveRoot", nil); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.containers = map[string]*fakeContainer{}
	return nil
}
