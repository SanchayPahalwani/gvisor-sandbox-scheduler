package runsc

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestArgs(t *testing.T) {
	c := NewCLI("", "/run/gss/n1")
	none := Ref{ID: "sb1", Network: NetworkNone}
	egress := Ref{ID: "sb2", Network: NetworkSandbox}
	top := []string{"--root", "/run/gss/n1", "--network=none", "--overlay2=none"}
	topSandbox := []string{"--root", "/run/gss/n1", "--network=sandbox", "--overlay2=none"}
	with := func(base []string, rest ...string) []string {
		return append(append([]string{}, base...), rest...)
	}

	tests := []struct {
		name string
		got  []string
		want []string
	}{
		{"create", c.createArgs(none, "/b"), with(top, "create", "--bundle", "/b", "sb1")},
		{"create egress", c.createArgs(egress, "/b"), with(topSandbox, "create", "--bundle", "/b", "sb2")},
		{"start", c.startArgs(none), with(top, "start", "sb1")},
		{"restore", c.restoreArgs(none, "/b", "/img"),
			with(top, "restore", "--detach", "--image-path", "/img", "--bundle", "/b", "sb1")},
		{"checkpoint", c.checkpointArgs(none, "/img"), with(top, "checkpoint", "--image-path", "/img", "sb1")},
		{"exec env pidfile", c.execArgs(none, ExecSpec{
			Args: []string{"python3", "-c", "x"}, Env: []string{"A=1"}, InternalPIDFile: "/p", Cwd: "/work",
		}), with(top, "exec", "--internal-pid-file", "/p", "--cwd", "/work", "--env", "A=1", "sb1", "python3", "-c", "x")},
		{"exec plain", c.execArgs(none, ExecSpec{Args: []string{"ls"}, Cwd: "/tmp"}),
			with(top, "exec", "--cwd", "/tmp", "sb1", "ls")},
		{"exec two env", c.execArgs(none, ExecSpec{Args: []string{"env"}, Env: []string{"A=1", "B=2"}, Cwd: "/work"}),
			with(top, "exec", "--cwd", "/work", "--env", "A=1", "--env", "B=2", "sb1", "env")},
		{"kill pid", c.killPIDArgs(none, 7), with(top, "kill", "--pid", "7", "sb1", "KILL")},
		{"kill", c.killArgs(none), with(top, "kill", "sb1", "KILL")},
		{"delete", c.deleteArgs(none), with(top, "delete", "--force", "sb1")},
		{"state", c.stateArgs(none), with(top, "state", "sb1")},
		{"list", c.listArgs(), []string{"--root", "/run/gss/n1", "list", "-quiet"}},
		{"version", c.versionArgs(), []string{"--version"}},
	}
	for _, tt := range tests {
		if !reflect.DeepEqual(tt.got, tt.want) {
			t.Errorf("%s:\n got %q\nwant %q", tt.name, tt.got, tt.want)
		}
	}
}

func TestNewCLIDefaultBinary(t *testing.T) {
	if got := NewCLI("", "/r").Binary; got != "runsc" {
		t.Errorf("Binary = %q, want runsc", got)
	}
	if got := NewCLI("/usr/local/bin/runsc", "/r").Binary; got != "/usr/local/bin/runsc" {
		t.Errorf("Binary = %q", got)
	}
}

func TestExecDefaultCwd(t *testing.T) {
	c := NewCLI("", "/r")
	got := c.execArgs(Ref{ID: "a", Network: NetworkNone}, ExecSpec{Args: []string{"true"}})
	want := []string{"--root", "/r", "--network=none", "--overlay2=none", "exec", "--cwd", "/work", "a", "true"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %q\nwant %q", got, want)
	}
}

func TestParseState(t *testing.T) {
	got, err := parseState([]byte(`{"id":"a","status":"running","pid":42,"bundle":"/b"}`))
	if err != nil {
		t.Fatal(err)
	}
	if want := (State{ID: "a", Status: "running", PID: 42}); got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if _, err := parseState([]byte("not json")); err == nil {
		t.Error("parseState(garbage): want error")
	}
}

func TestParseVersion(t *testing.T) {
	got, err := parseVersion([]byte("runsc version release-20261102.0\nspec: 1.2.1\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got != "release-20261102.0" {
		t.Errorf("got %q", got)
	}
	if _, err := parseVersion([]byte("")); err == nil {
		t.Error("parseVersion(empty): want error")
	}
	if _, err := parseVersion([]byte("something else\n")); err == nil {
		t.Error("parseVersion(bad): want error")
	}
}

func TestParseList(t *testing.T) {
	got := parseList([]byte("a\n\nb\n"))
	if want := []string{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
	if got := parseList(nil); len(got) != 0 {
		t.Errorf("parseList(nil) = %q", got)
	}
}

func TestIsNotFound(t *testing.T) {
	if !isNotFound("FetchSpec failed: loading container: file does not exist\n") {
		t.Error("runsc's missing-container message not recognised")
	}
	if isNotFound("some other failure") {
		t.Error("unrelated message treated as not found")
	}
}

func TestTail(t *testing.T) {
	long := strings.Repeat("x", 3000) + "END"
	got := tail([]byte(long))
	if len(got) != 2048 || !strings.HasSuffix(got, "END") {
		t.Errorf("tail len %d suffix %q", len(got), got[len(got)-3:])
	}
	if got := tail([]byte("  short\n")); got != "short" {
		t.Errorf("tail(short) = %q", got)
	}
}

func TestTailWriter(t *testing.T) {
	var w tailWriter
	w.Write([]byte(strings.Repeat("a", 2000)))
	w.Write([]byte(strings.Repeat("b", 2000)))
	got := w.String()
	if len(got) != 2048 || got[len(got)-1] != 'b' || got[0] != 'a' {
		t.Errorf("len %d first %q last %q", len(got), got[0], got[len(got)-1])
	}
}

func TestClassifyExecDiag(t *testing.T) {
	tests := []struct {
		stderr string
		want   error // nil: not a runsc diagnostic
		diag   bool
	}{
		{`executing processes for container: cannot execute in container "a" in state stopped`, ErrNotRunning, true},
		{`executing processes for container: executing command "true" in sandbox: connecting to control server at PID 18316: connection refused`, ErrNotRunning, true},
		{`executing processes for container: executing command "/no/such/binary" in sandbox: failed to load /no/such/binary: no such file or directory`, nil, true},
		{`executing processes for container: executing command "true" in sandbox: failed to find initial working directory "/nope": no such file or directory`, nil, true},
		{"exec [command options] <container-id> <command> [command options] || --process process.json <container-id>\n...\nparsing process spec: both a container-id and command are required", nil, true},
		{"FetchSpec failed: loading container: file does not exist", ErrNotFound, true},
		{"", nil, false},
		{"Traceback (most recent call last):\n  ...", nil, false},
	}
	for _, tt := range tests {
		if got := isExecDiag(tt.stderr); got != tt.diag {
			t.Errorf("isExecDiag(%q) = %v, want %v", tt.stderr, got, tt.diag)
		}
		if !tt.diag {
			continue
		}
		err := cmdError("exec", errors.New("exit status 128"), tt.stderr)
		if tt.want != nil && !errors.Is(err, tt.want) {
			t.Errorf("cmdError(%q) = %v, want %v", tt.stderr, err, tt.want)
		}
		if tt.want == nil && (errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotRunning)) {
			t.Errorf("cmdError(%q) = %v, want no sentinel", tt.stderr, err)
		}
	}
}

func TestRejectsUnknownNetwork(t *testing.T) {
	c := NewCLI("/nonexistent/runsc", "/r")
	ctx := context.Background()
	bad := Ref{ID: "a", Network: "host"}
	checks := map[string]error{
		"Create":      c.Create(ctx, bad, "/b", filepath.Join(t.TempDir(), "log")),
		"Start":       c.Start(ctx, bad),
		"Restore":     c.Restore(ctx, bad, "/b", "/i"),
		"Checkpoint":  c.Checkpoint(ctx, bad, "/i"),
		"KillProcess": c.KillProcess(ctx, bad, 1),
		"Delete":      c.Delete(ctx, bad),
	}
	_, checks["Exec"] = c.Exec(ctx, bad, ExecSpec{Args: []string{"true"}})
	_, checks["State"] = c.State(ctx, bad)
	for name, err := range checks {
		if err == nil || !strings.Contains(err.Error(), `network "host"`) {
			t.Errorf("%s with network host: err = %v", name, err)
		}
	}
	if _, err := c.Exec(ctx, Ref{ID: "a", Network: NetworkNone}, ExecSpec{}); err == nil {
		t.Error("Exec with empty Args: want error")
	}
}

// fakeRunsc writes a shell script that stands in for runsc: for exec it acts
// on the last argument (the command), for state it prints JSON.
func fakeRunsc(t *testing.T) *CLI {
	t.Helper()
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("no /bin/sh")
	}
	script := `#!/bin/sh
for a in "$@"; do last=$a; done
sub=""
for a in "$@"; do case $a in exec|state|start|kill|delete|checkpoint) sub=$a; break;; esac; done
case "$sub" in
state)
	case "$last" in
	ab) echo '{"id":"abc","status":"running","pid":7}';;
	ckpt)
		# "running" for the first two calls after the checkpoint, then "stopped".
		n=$(cat "$0.n" 2>/dev/null || echo 0); n=$((n+1)); echo $n > "$0.n"
		if [ $n -le 2 ]; then echo '{"id":"ckpt","status":"running","pid":7}'
		else echo '{"id":"ckpt","status":"stopped","pid":-1}'; fi;;
	*) echo '{"id":"'"$last"'","status":"running","pid":7}';;
	esac
	exit 0;;
start)
	echo 'start went wrong' >&2; exit 1;;
checkpoint)
	exit 0;;
delete)
	if [ "$last" = ab ]; then echo 'deleted another container' >&2; exit 1; fi
	exit 0;;
kill)
	exit 0;;
esac
case "$last" in
exit3) echo out; echo err >&2; exit 3;;
exit128) echo user-out; echo user-err >&2; exit 128;;
quiet128) exit 128;;
stopped) echo 'executing processes for container: cannot execute in container "sb1" in state stopped' >&2; exit 128;;
nobin) echo 'executing processes for container: executing command "/no/such" in sandbox: failed to load /no/such: no such file or directory' >&2; exit 128;;
gone) echo 'FetchSpec failed: loading container: file does not exist' >&2; exit 128;;
both) i=0; while [ $i -lt 200 ]; do echo out$i; echo err$i >&2; i=$((i+1)); done; exit 0;;
big) head -c 1000000 /dev/zero; exit 0;;
sleep) exec sleep 30;;
esac
exit 99
`
	bin := filepath.Join(t.TempDir(), "runsc")
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return NewCLI(bin, t.TempDir())
}

func TestExecExitMapping(t *testing.T) {
	c := fakeRunsc(t)
	ctx := context.Background()
	ref := Ref{ID: "sb1", Network: NetworkNone}
	run := func(cmd string) (int, string, string, error) {
		var out, errOut bytes.Buffer
		code, err := c.Exec(ctx, ref, ExecSpec{Args: []string{cmd}, Stdout: &out, Stderr: &errOut})
		return code, out.String(), errOut.String(), err
	}

	if code, out, errOut, err := run("exit3"); code != 3 || err != nil || out != "out\n" || errOut != "err\n" {
		t.Errorf("exit3: %d %v %q %q", code, err, out, errOut)
	}
	if code, out, errOut, err := run("exit128"); code != 128 || err != nil || out != "user-out\n" || errOut != "user-err\n" {
		t.Errorf("exit128 with user output: %d %v %q %q", code, err, out, errOut)
	}
	if code, _, _, err := run("quiet128"); code != 128 || err != nil {
		t.Errorf("quiet128: %d %v", code, err)
	}
	if code, _, errOut, err := run("stopped"); !errors.Is(err, ErrNotRunning) || code != -1 || errOut != "" {
		t.Errorf("stopped: %d %v, stderr forwarded %q", code, err, errOut)
	}
	if _, _, errOut, err := run("nobin"); err == nil || !strings.Contains(err.Error(), "/no/such") || errOut != "" {
		t.Errorf("nobin: %v, stderr forwarded %q", err, errOut)
	}
	if _, _, _, err := run("gone"); !errors.Is(err, ErrNotFound) {
		t.Errorf("gone: %v", err)
	}
}

func TestExecSameWriter(t *testing.T) {
	c := fakeRunsc(t)
	var buf bytes.Buffer // not safe for concurrent writes; -race catches it
	code, err := c.Exec(context.Background(), Ref{ID: "sb1", Network: NetworkNone},
		ExecSpec{Args: []string{"both"}, Stdout: &buf, Stderr: &buf})
	if err != nil || code != 0 {
		t.Fatalf("Exec = %d, %v", code, err)
	}
	if got := strings.Count(buf.String(), "\n"); got != 400 {
		t.Errorf("got %d lines, want 400", got)
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestExecWriterError(t *testing.T) {
	c := fakeRunsc(t)
	done := make(chan struct{})
	var code int
	var err error
	go func() {
		defer close(done)
		code, err = c.Exec(context.Background(), Ref{ID: "sb1", Network: NetworkNone},
			ExecSpec{Args: []string{"big"}, Stdout: failWriter{}})
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Exec blocked on a failing writer")
	}
	if code != 0 || err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Errorf("Exec = %d, %v; want 0 and the write error", code, err)
	}
}

func TestExecCancel(t *testing.T) {
	c := fakeRunsc(t)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	code, err := c.Exec(ctx, Ref{ID: "sb1", Network: NetworkNone}, ExecSpec{Args: []string{"sleep"}})
	if !errors.Is(err, context.DeadlineExceeded) || code != -1 {
		t.Errorf("Exec = %d, %v; want -1 and deadline exceeded", code, err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("Exec took %v after cancel", d)
	}
}

func TestRunErrorAndStateMismatch(t *testing.T) {
	c := fakeRunsc(t)
	ctx := context.Background()
	err := c.Start(ctx, Ref{ID: "sb1", Network: NetworkNone})
	if err == nil || !strings.Contains(err.Error(), "runsc start") || !strings.Contains(err.Error(), "start went wrong") {
		t.Errorf("Start err = %v", err)
	}
	// runsc matches IDs by prefix: asking for "ab" can report "abc".
	if _, err := c.State(ctx, Ref{ID: "ab", Network: NetworkNone}); !errors.Is(err, ErrNotFound) {
		t.Errorf("State(prefix of another id) err = %v, want ErrNotFound", err)
	}
	if st, err := c.State(ctx, Ref{ID: "sb1", Network: NetworkNone}); err != nil || st.ID != "sb1" {
		t.Errorf("State(sb1) = %+v, %v", st, err)
	}
	if err := c.Delete(ctx, Ref{ID: "ab", Network: NetworkNone}); err != nil {
		t.Errorf("Delete(prefix of another id) = %v, want nil", err)
	}
}

func TestCheckpointWaitsForStopped(t *testing.T) {
	c := fakeRunsc(t)
	ctx := context.Background()
	ref := Ref{ID: "ckpt", Network: NetworkNone}
	if err := c.Checkpoint(ctx, ref, "/img"); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	st, err := c.State(ctx, ref)
	if err != nil || st.Status != "stopped" || st.PID != -1 {
		t.Errorf("State after Checkpoint = %+v, %v; want stopped, -1", st, err)
	}
}
