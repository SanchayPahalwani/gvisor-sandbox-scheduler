package runsc

import (
	"reflect"
	"strings"
	"testing"
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
