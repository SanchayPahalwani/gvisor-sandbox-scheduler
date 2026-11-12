package main

import (
	"context"
	"io"
	"log/slog"
	"net/netip"
	"os"
	"runtime"
	"strings"
	"testing"
)

func TestParseFlagsDefaults(t *testing.T) {
	o, err := parseFlags([]string{"--node-id", "n-1"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	want := options{
		nodeID: "n-1", group: "default", listen: ":7101",
		cpuMillis: 4000, memoryBytes: 2048 << 20, maxSandboxes: 16, warmPoolSize: 2,
		rootfs: "/var/lib/gss/rootfs/python", stateDir: "/var/lib/gss/nodes/n-1",
		runscRoot: "/run/gss/n-1", checkpointDir: "/var/lib/gss/checkpoints",
		egressCIDR: netip.MustParsePrefix("10.200.0.0/20"), runsc: "runsc",
	}
	if *o != want {
		t.Errorf("options = %+v\nwant      %+v", *o, want)
	}

	o, err = parseFlags([]string{"--node-id=n1", "--checkpoint-dir=", "--warm-pool-size=0", "--listen=127.0.0.1:0"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if o.checkpointDir != "" || o.warmPoolSize != 0 || o.listen != "127.0.0.1:0" {
		t.Errorf("checkpoint dir %q, pool %d, listen %q", o.checkpointDir, o.warmPoolSize, o.listen)
	}

	// The lock file sits next to the root, so the root is kept clean.
	o, err = parseFlags([]string{"--node-id=n1", "--runsc-root=/run/gss/other/"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if o.runscRoot != "/run/gss/other" {
		t.Errorf("runsc root %q, want /run/gss/other", o.runscRoot)
	}
}

func TestParseFlagsRejects(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "--node-id is required"},
		{[]string{"--node-id", "n_1"}, "--node-id"},                   // netpolicy allows no '_'
		{[]string{"--node-id", "n.1"}, "--node-id"},                   // nor '.'
		{[]string{"--node-id", "-n1"}, "--node-id"},                   // must start with a letter or digit
		{[]string{"--node-id", strings.Repeat("a", 33)}, "--node-id"}, // at most 32
		{[]string{"--node-id", "n1", "--listen", "7101"}, "--listen"}, // no port separator
		{[]string{"--node-id", "n1", "--advertise", "host"}, "--advertise"},
		{[]string{"--node-id", "n1", "--scheduler", "127.0.0.1:7000"}, "not implemented"},
		{[]string{"--node-id", "n1", "--cpu-millis", "0"}, "positive"},
		{[]string{"--node-id", "n1", "--memory-mib", "-1"}, "positive"},
		{[]string{"--node-id", "n1", "--max-sandboxes", "0"}, "positive"},
		{[]string{"--node-id", "n1", "--warm-pool-size", "-1"}, "--warm-pool-size"},
		{[]string{"--node-id", "n1", "--state-dir", "rel/dir"}, "--state-dir"},
		{[]string{"--node-id", "n1", "--runsc-root", "/"}, "--runsc-root"},
		{[]string{"--node-id", "n1", "--runsc-root", "/var/lib/gss"}, "under /run/"}, // a clean shutdown removes it
		{[]string{"--node-id", "n1", "--runsc-root", "/run"}, "under /run/"},
		{[]string{"--node-id", "n1", "--runsc-root", "/run/../var/lib"}, "under /run/"},
		{[]string{"--node-id", "n1", "--egress-cidr", "10.200.0.0"}, "--egress-cidr"},
		{[]string{"--node-id", "n1", "extra"}, "unexpected arguments"},
	} {
		var out strings.Builder
		_, err := parseFlags(tc.args, &out)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("parseFlags(%q) = %v, want an error about %q", tc.args, err, tc.want)
			continue
		}
		if !strings.Contains(out.String(), err.Error()) {
			t.Errorf("parseFlags(%q) printed %q, not the error", tc.args, out.String())
		}
	}
}

func TestRunRefusesWithoutLinuxOrRoot(t *testing.T) {
	o, err := parseFlags([]string{"--node-id", "n1"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	var want string
	switch {
	case runtime.GOOS != "linux":
		want = "runs only on Linux"
	case os.Geteuid() != 0:
		want = "must run as root"
	default:
		t.Skip("running as root on Linux")
	}
	stopped := false
	err = run(context.Background(), func() { stopped = true }, o)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("run = %v, want an error saying it %s", err, want)
	}
	if !stopped {
		t.Error("a failed start did not restore the default signal handling")
	}
}

// TestRunStoppedDuringStartUp: a signal during start-up is a clean stop.
func TestRunStoppedDuringStartUp(t *testing.T) {
	if runtime.GOOS == "linux" && os.Geteuid() == 0 {
		t.Skip("as root on Linux the start-up would touch the host")
	}
	o, err := parseFlags([]string{"--node-id", "n1"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	var logs strings.Builder
	defer slog.SetDefault(slog.Default())
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run(ctx, func() {}, o); err != nil {
		t.Errorf("run with the signal context done = %v, want nil (exit 0)", err)
	}
	if !strings.Contains(logs.String(), `msg="node agent stopped during start-up"`) {
		t.Errorf("log %q has no stopped-during-start-up line", logs.String())
	}
}
