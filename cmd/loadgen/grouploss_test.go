package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
)

// groupLossFake places creates alternately on a1 (group a) and b1 (group b). After
// kill, Execs on group b fail with Unavailable; 300 ms later both b nodes are DEAD,
// and 500 ms later every b sandbox is on a2 at the next incarnation and runs again.
func groupLossFake(t *testing.T) (f *fake, c gssv1.SandboxServiceClient, kill func(context.Context) error, kills *int) {
	f, c = newFake(t)
	f.nodes = []*gssv1.Node{
		node("a1", "a", gssv1.NodeState_NODE_STATE_READY, 2),
		node("a2", "a", gssv1.NodeState_NODE_STATE_READY, 2),
		node("b1", "b", gssv1.NodeState_NODE_STATE_READY, 2),
		node("b2", "b", gssv1.NodeState_NODE_STATE_READY, 2),
	}
	f.place = func(n int) (string, string) {
		if n%2 == 0 {
			return "a1", "a"
		}
		return "b1", "b"
	}
	var killedAt time.Time // guarded by f.mu
	moved := false
	f.tick = func(f *fake) {
		if killedAt.IsZero() {
			return
		}
		since := time.Since(killedAt)
		if since >= 300*time.Millisecond {
			for _, n := range f.nodes {
				if n.GetInfo().GetGroup() == "b" {
					n.State = gssv1.NodeState_NODE_STATE_DEAD
				}
			}
		}
		if since >= 500*time.Millisecond && !moved {
			moved = true
			for _, sb := range f.live {
				if sb.NodeGroup == "b" {
					sb.NodeId, sb.NodeGroup, sb.Incarnation = "a2", "a", sb.Incarnation+1
				}
			}
		}
	}
	f.execErr = func(sb *gssv1.Sandbox) error {
		if !killedAt.IsZero() && sb.NodeGroup == "b" {
			return status.Errorf(codes.Unavailable, "node %s is unreachable", sb.NodeId)
		}
		return nil
	}
	kills = new(int)
	kill = func(context.Context) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		killedAt = time.Now()
		*kills++
		return nil
	}
	return f, c, kill, kills
}

func testGroupLossConfig(kill func(context.Context) error, retry bool) groupLossConfig {
	cfg := newGroupLossConfig()
	cfg.Sessions = 8
	cfg.Interval = 50 * time.Millisecond
	cfg.Warmup = 300 * time.Millisecond
	cfg.Observe = 2 * time.Second
	cfg.KillCmd = "test kill"
	cfg.Kill = kill
	cfg.LoseGroup = "b"
	cfg.Retry = retry
	return cfg
}

func TestGroupLossMetrics(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry=%v", retry), func(t *testing.T) {
			f, c, kill, kills := groupLossFake(t)
			r, err := runGroupLoss(context.Background(), c, testGroupLossConfig(kill, retry), io.Discard)
			if err != nil {
				t.Fatalf("runGroupLoss: %v", err)
			}
			s := r.Scalars
			if *kills != 1 {
				t.Errorf("kill ran %d times, want 1", *kills)
			}
			if s["sessions"] != 8 || s["affected_sessions"] != 4 {
				t.Errorf("sessions %v affected %v, want 8 and the 4 on group b", s["sessions"], s["affected_sessions"])
			}
			if got := s["recovery_ms"]; got < 400 || got > 1500 {
				t.Errorf("recovery_ms = %v, want in [400, 1500]", got)
			}
			if got := s["detection_ms"]; got < 250 || got > 1000 {
				t.Errorf("detection_ms = %v, want about 300 (b nodes go DEAD 300 ms after the kill)", got)
			}
			if retry && s["requests_failed"] != 0 {
				t.Errorf("requests_failed = %v with --retry, want 0", s["requests_failed"])
			}
			if !retry && s["requests_failed"] <= 0 {
				t.Errorf("requests_failed = %v without retry, want > 0", s["requests_failed"])
			}
			if s["requests_failed_unaffected"] != 0 {
				t.Errorf("requests_failed_unaffected = %v, want 0", s["requests_failed_unaffected"])
			}
			if s["rescheduled"] != 4 {
				t.Errorf("rescheduled = %v, want 4", s["rescheduled"])
			}
			if s["requests_total"] < 8 || s["max_blocked_exec_ms"] <= 0 {
				t.Errorf("requests_total %v max_blocked_exec_ms %v, want at least one request per session and a positive max",
					s["requests_total"], s["max_blocked_exec_ms"])
			}
			if before, after := seriesByName(t, r, "exec_before_ms"), seriesByName(t, r, "exec_after_ms"); before.Count == 0 || after.Count == 0 {
				t.Errorf("exec_before_ms count %d, exec_after_ms count %d: want both > 0", before.Count, after.Count)
			}
			notes := strings.Join(r.Notes, "\n")
			if !strings.Contains(notes, "local failure-domain test: both groups are processes in one VM on one laptop") {
				t.Errorf("notes do not carry the local failure-domain sentence: %q", r.Notes)
			}
			var summary bytes.Buffer
			if err := printGroupLoss(&summary, r, "out.json"); err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(summary.String(), "local failure-domain test: both groups are processes in one VM on one laptop\n") {
				t.Errorf("summary does not start with the local failure-domain line:\n%s", summary.String())
			}
			// The test is a local failure domain only; its output must never describe it as more.
			if all := strings.ToLower(notes + summary.String()); strings.Contains(all, "multi-") || strings.Contains(all, "region") || strings.Contains(all, "zone") {
				t.Errorf("output describes more than one local failure domain:\n%s", all)
			}
			if f.liveCount() != 0 {
				t.Errorf("%d sandboxes left live after the run", f.liveCount())
			}
		})
	}
}

func TestGroupLossNeedsBothGroups(t *testing.T) {
	f, c, kill, kills := groupLossFake(t)
	f.place = func(int) (string, string) { return "a1", "a" }
	_, err := runGroupLoss(context.Background(), c, testGroupLossConfig(kill, false), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "each group") {
		t.Fatalf("err = %v, want an error that sessions must land on each group", err)
	}
	if *kills != 0 {
		t.Errorf("kill ran %d times, want 0", *kills)
	}
	if f.liveCount() != 0 {
		t.Errorf("%d sandboxes left live after the error", f.liveCount())
	}
}
