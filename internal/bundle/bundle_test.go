package bundle

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"
)

var update = flag.Bool("update", false, "rewrite testdata/config.golden.json")

func goldenOptions() Options {
	return Options{ID: "sb1", Dir: "/b/sb1", RootFS: "/r", NodeID: "n1", CPUMillis: 500, MemoryBytes: 256 << 20, Prime: "json"}
}

func TestSpecGolden(t *testing.T) {
	got, err := json.MarshalIndent(Spec(goldenOptions()), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	path := filepath.Join("testdata", "config.golden.json")
	if *update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Errorf("spec differs from %s (run with -update to rewrite)\ngot:\n%s\nwant:\n%s", path, got, want)
	}
}

func TestSpecLimits(t *testing.T) {
	for _, c := range []struct{ millis, quota int64 }{{500, 50000}, {1500, 150000}} {
		o := goldenOptions()
		o.CPUMillis = c.millis
		s := Spec(o)
		cpu := s.Linux.Resources.CPU
		if cpu.Quota == nil || *cpu.Quota != c.quota || cpu.Period == nil || *cpu.Period != 100000 {
			t.Errorf("%dm: cpu = %+v, want quota %d period 100000", c.millis, cpu, c.quota)
		}
		if m := s.Linux.Resources.Memory; m.Limit == nil || *m.Limit != o.MemoryBytes {
			t.Errorf("memory = %+v, want limit %d", m, o.MemoryBytes)
		}
	}
}

func netPath(s *specs.Spec, t *testing.T) string {
	t.Helper()
	for _, ns := range s.Linux.Namespaces {
		if ns.Type == specs.NetworkNamespace {
			return ns.Path
		}
	}
	t.Fatal("no network namespace")
	return ""
}

func TestSpecNetNS(t *testing.T) {
	o := goldenOptions()
	if p := netPath(Spec(o), t); p != "" {
		t.Errorf("empty NetNSPath gave path %q", p)
	}
	o.NetNSPath = "/var/run/netns/x"
	if p := netPath(Spec(o), t); p != "/var/run/netns/x" {
		t.Errorf("path = %q", p)
	}
}

func TestProcessIdenticalAcrossSandboxes(t *testing.T) {
	a := Spec(goldenOptions())
	b := Spec(Options{ID: "other", Dir: "/x/y", RootFS: "/r", NodeID: "n1", CPUMillis: 2000, MemoryBytes: 1 << 30,
		NetNSPath: "/var/run/netns/z", Hosts: []HostEntry{{IP: "10.1.2.3", Name: "api.example"}}, Prime: "json"})
	if !reflect.DeepEqual(a.Process, b.Process) {
		t.Errorf("process differs:\n%+v\n%+v", a.Process, b.Process)
	}
}

func TestWrite(t *testing.T) {
	o := goldenOptions()
	o.Dir = filepath.Join(t.TempDir(), "sb1")
	o.Hosts = []HostEntry{{IP: "10.1.2.3", Name: "api.example"}}
	if err := Write(o); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(o.Dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got specs.Spec
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&got, Spec(o)) {
		t.Errorf("config.json does not round-trip to Spec(o)")
	}
	hosts, err := os.ReadFile(filepath.Join(o.Dir, "hosts"))
	if err != nil {
		t.Fatal(err)
	}
	if want := "127.0.0.1 localhost\n10.1.2.3 api.example\n"; string(hosts) != want {
		t.Errorf("hosts = %q, want %q", hosts, want)
	}
	if runtime.GOOS != "windows" {
		st, err := os.Stat(o.Dir)
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode().Perm() != 0o700 {
			t.Errorf("dir mode = %v, want 0700", st.Mode().Perm())
		}
	}
}

func TestCgroupPath(t *testing.T) {
	if got := CgroupPath("n1", "sb1"); got != "/gss/n1/sb1" {
		t.Errorf("CgroupPath = %q", got)
	}
}
