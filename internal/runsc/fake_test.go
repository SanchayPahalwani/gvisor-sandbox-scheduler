package runsc

import (
	"context"
	"errors"
	"reflect"
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
	if err := f.KillProcess(ctx, ref, 5); err != nil {
		t.Errorf("KillProcess: %v", err)
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
	f.ExecFunc = func(ref Ref, spec ExecSpec) (int, error) {
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
