package main

import (
	"context"
	"io"
	"log/slog"
	"net/netip"
	"strings"
	"testing"
)

func TestParseFlagsDefaults(t *testing.T) {
	o, err := parseFlags([]string{"--node-id", "n-1"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	want := options{
		nodeID: "n-1", group: "default", listen: "127.0.0.1:7101",
		cpuMillis: 4000, memoryBytes: 2048 << 20, maxSandboxes: 16, warmPoolSize: 2,
		rootfs: "/var/lib/gss/rootfs/python", stateDir: "/var/lib/gss/nodes/n-1",
		runscRoot: "/run/gss/n-1", checkpointDir: "/var/lib/gss/checkpoints",
		egressCIDR: netip.MustParsePrefix("10.200.0.0/20"), runsc: "runsc",
	}
	if *o != want {
		t.Errorf("options = %+v\nwant      %+v", *o, want)
	}

	o, err = parseFlags([]string{"--node-id=n1", "--checkpoint-dir=", "--warm-pool-size=0", "--listen=127.0.0.1:0", "--scheduler=127.0.0.1:7000", "--max-sandboxes=10000"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if o.checkpointDir != "" || o.warmPoolSize != 0 || o.listen != "127.0.0.1:0" || o.scheduler != "127.0.0.1:7000" || o.maxSandboxes != 10000 {
		t.Errorf("checkpoint dir %q, pool %d, listen %q, scheduler %q, max sandboxes %d", o.checkpointDir, o.warmPoolSize, o.listen, o.scheduler, o.maxSandboxes)
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
		{[]string{"--node-id", "n1", "--scheduler", "7000"}, "--scheduler"}, // no port separator
		{[]string{"--node-id", "n1", "--cpu-millis", "0"}, "positive"},
		{[]string{"--node-id", "n1", "--memory-mib", "-1"}, "positive"},
		{[]string{"--node-id", "n1", "--max-sandboxes", "0"}, "positive"},
		{[]string{"--node-id", "n1", "--max-sandboxes", "10001"}, "--max-sandboxes"},
		{[]string{"--node-id", "n1", "--max-sandboxes", "4294967297"}, "--max-sandboxes"}, // wraps to 1 as an int32
		{[]string{"--node-id", "n1", "--warm-pool-size", "-1"}, "--warm-pool-size"},
		{[]string{"--node-id", "n1", "--state-dir", "rel/dir"}, "--state-dir"},
		{[]string{"--node-id", "n1", "--runsc-root", "/run/gss/x"}, "not defined"}, // always /run/gss/<node-id>
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

// TestAgentExitStatus drives the agent as main does, with a stop that
// cancels the context as signal.NotifyContext's stop does. --runsc points
// nowhere, so the start-up fails before it touches the host on any system
// (off Linux, without root, or at the runsc lookup).
func TestAgentExitStatus(t *testing.T) {
	defer slog.SetDefault(slog.Default())
	args := []string{"--node-id", "n1", "--runsc", "/nonexistent/runsc"}
	for _, tc := range []struct {
		name     string
		args     []string
		signal   bool // a signal arrived during start-up
		want     int
		wantLine string
	}{
		{"failed start-up", args, false, 1, `level=ERROR msg="node agent failed" node=n1 err=`},
		{"signal during start-up", args, true, 0, `level=INFO msg="node agent stopped during start-up" node=n1`},
		{"bad flag", []string{"--node-id", "n_1"}, false, 2, `nodeagent: --node-id "n_1"`},
		{"help", []string{"-h"}, false, 0, "Usage of nodeagent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			if tc.signal {
				stop()
			}
			var out strings.Builder
			if got := agent(ctx, stop, tc.args, &out); got != tc.want {
				t.Errorf("exit status %d, want %d; output:\n%s", got, tc.want, out.String())
			}
			if !strings.Contains(out.String(), tc.wantLine) {
				t.Errorf("output has no %q:\n%s", tc.wantLine, out.String())
			}
			if tc.want == 1 && ctx.Err() == nil {
				t.Error("a failed start did not restore the default signal handling")
			}
		})
	}
}
