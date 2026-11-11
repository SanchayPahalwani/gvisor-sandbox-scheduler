package runsc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestFakeLifecycle(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	ref := Ref{ID: "a", Network: NetworkNone}

	status := func(want string) {
		t.Helper()
		st, err := f.State(ctx, ref)
		if err != nil {
			t.Fatalf("State: %v", err)
		}
		if st.Status != want || st.ID != "a" {
			t.Fatalf("State = %+v, want status %q", st, want)
		}
		if got := f.Containers()["a"]; got != want {
			t.Fatalf("Containers()[a] = %q, want %q", got, want)
		}
	}

	if err := f.Create(ctx, ref, "/b", "/log"); err != nil {
		t.Fatal(err)
	}
	status("created")
	if err := f.Create(ctx, ref, "/b", "/log"); err == nil {
		t.Error("second Create of the same id: want error")
	}
	if err := f.Start(ctx, ref); err != nil {
		t.Fatal(err)
	}
	status("running")
	if st, _ := f.State(ctx, ref); st.PID <= 0 {
		t.Errorf("running PID = %d", st.PID)
	}
	code, err := f.Exec(ctx, ref, ExecSpec{Args: []string{"true"}})
	if err != nil || code != 0 {
		t.Errorf("Exec = %d, %v", code, err)
	}
	if err := f.KillProcess(ctx, ref, 5); err == nil {
		t.Error("KillProcess of a pid no Exec handed out: want error")
	}
	if err := f.Checkpoint(ctx, ref, "/img"); err != nil {
		t.Fatal(err)
	}
	status("stopped")
	if _, err := f.Exec(ctx, ref, ExecSpec{Args: []string{"true"}}); err == nil {
		t.Error("Exec in a stopped container: want error")
	}
	ids, err := f.List(ctx)
	if err != nil || !reflect.DeepEqual(ids, []string{"a"}) {
		t.Errorf("List = %q, %v", ids, err)
	}
	if err := f.Delete(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.Containers()["a"]; ok {
		t.Error("container still listed after Delete")
	}

	unknown := Ref{ID: "nope", Network: NetworkNone}
	if _, err := f.State(ctx, unknown); !errors.Is(err, ErrNotFound) {
		t.Errorf("State(unknown) err = %v, want ErrNotFound", err)
	}
	if err := f.Delete(ctx, unknown); err != nil {
		t.Errorf("Delete(unknown) = %v, want nil", err)
	}
	if _, err := f.Exec(ctx, unknown, ExecSpec{}); !errors.Is(err, ErrNotFound) {
		t.Errorf("Exec(unknown) err = %v, want ErrNotFound", err)
	}

	// Restore path: Create then Restore gives a running container.
	b := Ref{ID: "b", Network: NetworkSandbox}
	if err := f.Create(ctx, b, "/b2", "/log2"); err != nil {
		t.Fatal(err)
	}
	if err := f.Restore(ctx, b, "/b2", "/img"); err != nil {
		t.Fatal(err)
	}
	if got := f.Containers()["b"]; got != "running" {
		t.Errorf("after Restore status = %q", got)
	}
	if err := f.Restore(ctx, b, "/b2", "/img"); err == nil {
		t.Error("Restore of a running container: want error")
	}
}

func TestFakeFailOnce(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	ref := Ref{ID: "a", Network: NetworkNone}
	boom := errors.New("boom")
	f.Fail["Start"] = boom
	if err := f.Create(ctx, ref, "/b", "/log"); err != nil {
		t.Fatal(err)
	}
	if err := f.Start(ctx, ref); !errors.Is(err, boom) {
		t.Fatalf("first Start = %v, want boom", err)
	}
	if got := f.Containers()["a"]; got != "created" {
		t.Errorf("status after failed Start = %q, want created", got)
	}
	if err := f.Start(ctx, ref); err != nil {
		t.Fatalf("second Start = %v, want nil", err)
	}
}

func TestFakeRecordsCalls(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	f.VersionStr = "release-20261102.0"
	a := Ref{ID: "a", Network: NetworkNone}
	var gotSpec ExecSpec
	f.ExecFunc = func(_ context.Context, ref Ref, spec ExecSpec) (int, error) {
		gotSpec = spec
		return 3, nil
	}

	_ = f.Create(ctx, a, "/b", "/log")
	_ = f.Start(ctx, a)
	code, _ := f.Exec(ctx, a, ExecSpec{Args: []string{"python3", "-c", "x"}})
	_ = f.KillProcess(ctx, a, 9)
	_, _ = f.State(ctx, a)
	_ = f.Checkpoint(ctx, a, "/img")
	_ = f.Delete(ctx, a)
	_, _ = f.List(ctx)
	v, _ := f.Version(ctx)
	_ = f.RemoveRoot(ctx)

	if code != 3 || !reflect.DeepEqual(gotSpec.Args, []string{"python3", "-c", "x"}) {
		t.Errorf("ExecFunc not used: code %d spec %+v", code, gotSpec)
	}
	if v != "release-20261102.0" {
		t.Errorf("Version = %q", v)
	}
	want := []string{
		"Create id=a net=none",
		"Start id=a net=none",
		"Exec id=a net=none",
		"KillProcess id=a net=none",
		"State id=a net=none",
		"Checkpoint id=a net=none",
		"Delete id=a net=none",
		"List",
		"Version",
		"RemoveRoot",
	}
	if got := f.Calls(); !reflect.DeepEqual(got, want) {
		t.Errorf("Calls:\n got %q\nwant %q", got, want)
	}
}

func TestFakeDelayHonoursContext(t *testing.T) {
	f := NewFake()
	f.Delay["Create"] = time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := f.Create(ctx, Ref{ID: "a", Network: NetworkNone}, "/b", "/log")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Create = %v, want deadline exceeded", err)
	}
	if _, ok := f.Containers()["a"]; ok {
		t.Error("container created despite cancelled delay")
	}
}

func TestFakeCancelledContext(t *testing.T) {
	f := NewFake()
	bg := context.Background()
	a := Ref{ID: "a", Network: NetworkNone}
	if err := f.Create(bg, a, "/b", "/log"); err != nil {
		t.Fatal(err)
	}
	if err := f.Start(bg, a); err != nil {
		t.Fatal(err)
	}
	before := f.Containers()
	ctx, cancel := context.WithCancel(bg)
	cancel()
	nb := Ref{ID: "b", Network: NetworkNone}
	errs := map[string]error{
		"Create":      f.Create(ctx, nb, "/b", "/log"),
		"Start":       f.Start(ctx, a),
		"Restore":     f.Restore(ctx, a, "/b", "/i"),
		"Checkpoint":  f.Checkpoint(ctx, a, "/i"),
		"KillProcess": f.KillProcess(ctx, a, 1),
		"Delete":      f.Delete(ctx, a),
		"RemoveRoot":  f.RemoveRoot(ctx),
	}
	_, errs["Exec"] = f.Exec(ctx, a, ExecSpec{Args: []string{"true"}})
	_, errs["State"] = f.State(ctx, a)
	_, errs["List"] = f.List(ctx)
	_, errs["Version"] = f.Version(ctx)
	for name, err := range errs {
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%s with cancelled ctx = %v, want context.Canceled", name, err)
		}
	}
	if got := f.Containers(); !reflect.DeepEqual(got, before) {
		t.Errorf("containers changed: %v -> %v", before, got)
	}
}

func TestFakeZeroValue(t *testing.T) {
	var f Fake
	ctx := context.Background()
	a := Ref{ID: "a", Network: NetworkNone}
	if err := f.Create(ctx, a, "/b", "/log"); err != nil {
		t.Fatal(err)
	}
	if err := f.Start(ctx, a); err != nil {
		t.Fatal(err)
	}
	if code, err := f.Exec(ctx, a, ExecSpec{Args: []string{"true"}}); code != 0 || err != nil {
		t.Errorf("Exec = %d, %v", code, err)
	}
	if err := f.Delete(ctx, a); err != nil {
		t.Error(err)
	}
	_ = f.RemoveRoot(ctx)
}

func TestFakeStatePIDs(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	a := Ref{ID: "a", Network: NetworkNone}
	_ = f.Create(ctx, a, "/b", "/log")
	_ = f.Start(ctx, a)
	st1, _ := f.State(ctx, a)
	st2, _ := f.State(ctx, a)
	if st1.PID <= 0 || st1.PID != st2.PID {
		t.Errorf("running PIDs %d, %d; want one stable positive pid", st1.PID, st2.PID)
	}
	_ = f.Checkpoint(ctx, a, "/i")
	if st, _ := f.State(ctx, a); st.PID != -1 {
		t.Errorf("stopped PID = %d, want -1", st.PID)
	}
	if _, err := f.Exec(ctx, a, ExecSpec{Args: []string{"true"}}); !errors.Is(err, ErrNotRunning) {
		t.Errorf("Exec in stopped container = %v, want ErrNotRunning", err)
	}
}

func TestFakeExecPIDs(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	a := Ref{ID: "a", Network: NetworkNone}
	_ = f.Create(ctx, a, "/b", "/log")
	_ = f.Start(ctx, a)

	if err := f.KillProcess(ctx, a, 12345); err == nil {
		t.Error("KillProcess of a pid never handed out: want error")
	}

	pidFile := filepath.Join(t.TempDir(), "pid")
	started := make(chan int)
	release := make(chan struct{})
	f.ExecFunc = func(_ context.Context, ref Ref, spec ExecSpec) (int, error) {
		raw, err := os.ReadFile(spec.InternalPIDFile)
		if err != nil {
			t.Errorf("pid file not written before ExecFunc: %v", err)
		}
		pid, _ := strconv.Atoi(string(raw))
		started <- pid
		<-release
		return 0, nil
	}
	done := make(chan error)
	go func() {
		_, err := f.Exec(ctx, a, ExecSpec{Args: []string{"sleep"}, InternalPIDFile: pidFile})
		done <- err
	}()
	pid := <-started
	if pid <= 0 {
		t.Fatalf("pid file held %d", pid)
	}
	if err := f.KillProcess(ctx, a, pid); err != nil {
		t.Errorf("KillProcess(live pid) = %v", err)
	}
	if err := f.KillProcess(ctx, a, pid); err == nil {
		t.Error("second KillProcess of the same pid: want error")
	}
	close(release)
	if err := <-done; err != nil {
		t.Errorf("Exec = %v", err)
	}

	// A pid is released when ExecFunc returns.
	f.ExecFunc = func(_ context.Context, ref Ref, spec ExecSpec) (int, error) {
		raw, _ := os.ReadFile(spec.InternalPIDFile)
		pid, _ = strconv.Atoi(string(raw))
		return 0, nil
	}
	if _, err := f.Exec(ctx, a, ExecSpec{Args: []string{"true"}, InternalPIDFile: pidFile}); err != nil {
		t.Fatal(err)
	}
	if err := f.KillProcess(ctx, a, pid); err == nil {
		t.Error("KillProcess of an exited exec's pid: want error")
	}
}

func TestFakeExecCancelKeepsPID(t *testing.T) {
	f := NewFake()
	bg := context.Background()
	a := Ref{ID: "a", Network: NetworkNone}
	_ = f.Create(bg, a, "/b", "/log")
	_ = f.Start(bg, a)
	f.ExecFunc = func(ctx context.Context, ref Ref, spec ExecSpec) (int, error) {
		<-ctx.Done()
		return -1, ctx.Err()
	}
	pidFile := filepath.Join(t.TempDir(), "pid")
	ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
	defer cancel()
	code, err := f.Exec(ctx, a, ExecSpec{Args: []string{"sleep"}, InternalPIDFile: pidFile})
	if code != -1 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Exec = %d, %v; want -1, deadline exceeded", code, err)
	}
	raw, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(raw))
	if err != nil || pid <= 0 {
		t.Fatalf("pid file holds %q", raw)
	}
	if err := f.KillProcess(bg, a, pid); err != nil {
		t.Errorf("KillProcess(%d) after a cancelled Exec = %v", pid, err)
	}
	if err := f.KillProcess(bg, a, pid); err == nil {
		t.Errorf("second KillProcess(%d): want error", pid)
	}

	// Delete releases a pid left by a cancelled Exec.
	ctx2, cancel2 := context.WithTimeout(bg, 10*time.Millisecond)
	defer cancel2()
	_, _ = f.Exec(ctx2, a, ExecSpec{Args: []string{"sleep"}, InternalPIDFile: pidFile})
	raw, _ = os.ReadFile(pidFile)
	pid2, _ := strconv.Atoi(string(raw))
	_ = f.Delete(bg, a)
	_ = f.Create(bg, a, "/b", "/log")
	_ = f.Start(bg, a)
	if err := f.KillProcess(bg, a, pid2); err == nil {
		t.Errorf("KillProcess(%d) after Delete and re-create: want error", pid2)
	}
}
