package runsc

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Fake is an in-memory Runtime for unit tests. Set its fields before use;
// Calls and Containers are safe to read while it is in use.
type Fake struct {
	ExecFunc   func(ref Ref, spec ExecSpec) (int, error) // default: exit 0
	Fail       map[string]error                          // method name -> error to return once
	Delay      map[string]time.Duration                  // method name -> sleep before acting
	VersionStr string

	mu         sync.Mutex
	calls      []string
	containers map[string]string // id -> status
	pids       map[string]int    // id -> pid while running
	nextPID    int
}

var _ Runtime = (*Fake)(nil)

// NewFake returns an empty Fake whose Exec exits 0.
func NewFake() *Fake {
	return &Fake{
		Fail:       map[string]error{},
		Delay:      map[string]time.Duration{},
		VersionStr: "release-fake",
		containers: map[string]string{},
		pids:       map[string]int{},
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
	for id, st := range f.containers {
		out[id] = st
	}
	return out
}

// enter records the call, applies Delay and returns a one-shot Fail error.
func (f *Fake) enter(ctx context.Context, method string, ref *Ref) error {
	f.mu.Lock()
	call := method
	if ref != nil {
		call = fmt.Sprintf("%s id=%s net=%s", method, ref.ID, ref.Network)
	}
	f.calls = append(f.calls, call)
	delay := f.Delay[method]
	f.mu.Unlock()

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
	return nil
}

// transition moves ref from status from to status to under the lock.
func (f *Fake) transition(method string, ref Ref, from, to string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.containers[ref.ID]
	if !ok {
		return fmt.Errorf("fake %s %s: %w", method, ref.ID, ErrNotFound)
	}
	if st != from {
		return fmt.Errorf("fake %s %s: container is %s, want %s", method, ref.ID, st, from)
	}
	f.containers[ref.ID] = to
	delete(f.pids, ref.ID)
	if to == "running" {
		f.nextPID++
		f.pids[ref.ID] = 1000 + f.nextPID
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
	f.containers[ref.ID] = "created"
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

func (f *Fake) Exec(ctx context.Context, ref Ref, spec ExecSpec) (int, error) {
	if err := f.enter(ctx, "Exec", &ref); err != nil {
		return -1, err
	}
	if err := f.requireRunning("Exec", ref); err != nil {
		return -1, err
	}
	if f.ExecFunc == nil {
		return 0, nil
	}
	return f.ExecFunc(ref, spec)
}

func (f *Fake) KillProcess(ctx context.Context, ref Ref, internalPID int) error {
	if err := f.enter(ctx, "KillProcess", &ref); err != nil {
		return err
	}
	return f.requireRunning("KillProcess", ref)
}

func (f *Fake) requireRunning(method string, ref Ref) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.containers[ref.ID]
	if !ok {
		return fmt.Errorf("fake %s %s: %w", method, ref.ID, ErrNotFound)
	}
	if st != "running" {
		return fmt.Errorf("fake %s %s: container is %s", method, ref.ID, st)
	}
	return nil
}

func (f *Fake) Delete(ctx context.Context, ref Ref) error {
	if err := f.enter(ctx, "Delete", &ref); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.containers, ref.ID)
	delete(f.pids, ref.ID)
	return nil
}

func (f *Fake) State(ctx context.Context, ref Ref) (State, error) {
	if err := f.enter(ctx, "State", &ref); err != nil {
		return State{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.containers[ref.ID]
	if !ok {
		return State{}, fmt.Errorf("fake State %s: %w", ref.ID, ErrNotFound)
	}
	return State{ID: ref.ID, Status: st, PID: f.pids[ref.ID]}, nil
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
	f.containers = map[string]string{}
	f.pids = map[string]int{}
	return nil
}
