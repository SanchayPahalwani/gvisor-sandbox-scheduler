package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
)

// glFake places creates alternately on a1 (group a) and b1 (group b). After kill,
// Execs on group b fail with Unavailable; 300 ms later both b nodes are DEAD, and 500 ms
// later every b sandbox is on a2 at the next incarnation and runs again.
type glFake struct {
	*fake
	c gssv1.SandboxServiceClient
	// Guarded by fake.mu; hooks may read them directly.
	killedAt time.Time
	kills    int
}

func groupLossFake(t *testing.T) *glFake {
	f, c := newFake(t)
	g := &glFake{fake: f, c: c}
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
	moved := false
	f.tick = func(f *fake) {
		if g.killedAt.IsZero() {
			return
		}
		since := time.Since(g.killedAt)
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
		if !g.killedAt.IsZero() && sb.NodeGroup == "b" {
			return status.Errorf(codes.Unavailable, "node %s is unreachable", sb.NodeId)
		}
		return nil
	}
	return g
}

func (g *glFake) kill(context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.killedAt = time.Now()
	g.kills++
	return nil
}

func (g *glFake) killCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.kills
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
	cfg.SchedPolicy = "spread"
	return cfg
}

func TestGroupLossMetrics(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry=%v", retry), func(t *testing.T) {
			g := groupLossFake(t)
			r, err := runGroupLoss(context.Background(), g.c, testGroupLossConfig(g.kill, retry), io.Discard)
			if err != nil {
				t.Fatalf("runGroupLoss: %v", err)
			}
			s := r.Scalars
			if g.killCount() != 1 {
				t.Errorf("kill ran %d times, want 1", g.killCount())
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
			for _, k := range []string{"requests_failed_unaffected", "requests_failed_before_kill", "lookup_errors"} {
				if got, ok := s[k]; !ok || got != 0 {
					t.Errorf("%s = %v (present %v), want 0", k, got, ok)
				}
			}
			if s["rescheduled"] != 4 {
				t.Errorf("rescheduled = %v, want 4", s["rescheduled"])
			}
			if _, ok := s["requests_spanning_kill"]; !ok {
				t.Error("no requests_spanning_kill scalar")
			}
			if s["requests_total"] < 8 || s["max_blocked_exec_ms"] <= 0 || s["max_request_with_retries_ms"] < s["max_blocked_exec_ms"] {
				t.Errorf("requests_total %v max_blocked_exec_ms %v max_request_with_retries_ms %v",
					s["requests_total"], s["max_blocked_exec_ms"], s["max_request_with_retries_ms"])
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
			// Spec D35: both the notes and the summary say the spread policy was used, and why.
			const why = "under bin-packing all sessions fit on one node"
			for name, text := range map[string]string{"notes": notes, "summary": summary.String()} {
				if !strings.Contains(text, "spread") || !strings.Contains(text, why) {
					t.Errorf("%s do not say the spread policy was used and why:\n%s", name, text)
				}
			}
			// The test is a local failure domain only; its output must never describe it as more.
			if all := strings.ToLower(notes + summary.String()); strings.Contains(all, "multi-") || strings.Contains(all, "region") || strings.Contains(all, "zone") {
				t.Errorf("output describes more than one local failure domain:\n%s", all)
			}
			if g.liveCount() != 0 {
				t.Errorf("%d sandboxes left live after the run", g.liveCount())
			}
		})
	}
}

// One Exec try blocks for 800 ms before failing, and the retry waits 1 s more: the
// blocked figure is the single try, the retry backoff appears only in the whole-request
// figure, and exec_after_ms holds single-try latencies.
func TestGroupLossBlockedTry(t *testing.T) {
	g := groupLossFake(t)
	blocked, lookupFailed := false, false
	g.execDelayFor = func(sb *gssv1.Sandbox) time.Duration {
		if !g.killedAt.IsZero() && sb.NodeGroup == "b" && !blocked {
			blocked = true
			return 800 * time.Millisecond
		}
		return 0
	}
	g.getErr = func(string) error {
		if !lookupFailed {
			lookupFailed = true
			return status.Error(codes.Unavailable, "scheduler busy")
		}
		return nil
	}
	cfg := testGroupLossConfig(g.kill, true)
	cfg.Sessions = 2
	cfg.Observe = 3 * time.Second
	r, err := runGroupLoss(context.Background(), g.c, cfg, io.Discard)
	if err != nil {
		t.Fatalf("runGroupLoss: %v", err)
	}
	s := r.Scalars
	if got := s["max_blocked_exec_ms"]; got < 800 || got >= 1000 {
		t.Errorf("max_blocked_exec_ms = %v, want the 800 ms try alone, not the retry backoff", got)
	}
	if got := s["max_request_with_retries_ms"]; got < 1800 {
		t.Errorf("max_request_with_retries_ms = %v, want >= 1800 (800 ms try + 1 s backoff + retry)", got)
	}
	if after := seriesByName(t, r, "exec_after_ms"); after.Count == 0 || after.Max >= 800 {
		t.Errorf("exec_after_ms count %d max %v: want single-try latencies, all well under 800 ms", after.Count, after.Max)
	}
	if s["requests_failed"] != 0 || s["affected_sessions"] != 1 || s["rescheduled"] != 1 || s["recovery_ms"] < 1800 {
		t.Errorf("failed %v affected %v rescheduled %v recovery %v: want 0, 1, 1 and >= 1800",
			s["requests_failed"], s["affected_sessions"], s["rescheduled"], s["recovery_ms"])
	}
	if s["lookup_errors"] != 1 || !strings.Contains(strings.Join(r.Notes, "\n"), "1 lookup calls failed") {
		t.Errorf("lookup_errors = %v, notes %q: want the one failed GetSandbox counted and noted", s["lookup_errors"], r.Notes)
	}
}

func TestGroupLossNeedsBothGroups(t *testing.T) {
	g := groupLossFake(t)
	g.place = func(int) (string, string) { return "a1", "a" }
	_, err := runGroupLoss(context.Background(), g.c, testGroupLossConfig(g.kill, false), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "each group") {
		t.Fatalf("err = %v, want an error that sessions must land on each group", err)
	}
	if g.killCount() != 0 {
		t.Errorf("kill ran %d times, want 0", g.killCount())
	}
	if g.liveCount() != 0 {
		t.Errorf("%d sandboxes left live after the error", g.liveCount())
	}
}

// The metric rules on hand-built records.
func TestGroupLossFigures(t *testing.T) {
	kill := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) time.Time { return kill.Add(d) }
	ms := time.Millisecond
	rec := func(start, end time.Duration, ok bool, inc int32, tries ...time.Duration) execRecord {
		return execRecord{start: at(start), end: at(end), ok: ok, incarnation: inc, tries: tries}
	}
	sessions := []sessionResult{
		{ // unaffected
			incAtKill: 1, finalInc: 1,
			records: []execRecord{
				rec(-2000*ms, -1900*ms, true, 0, 100*ms), // before
				rec(-50*ms, 50*ms, true, 0, 100*ms),      // spans the kill
				rec(1000*ms, 1200*ms, true, 0, 200*ms),   // after
				rec(4000*ms, 4100*ms, false, 1, 100*ms),  // failed on an unaffected session
			},
		},
		{ // affected, rescheduled, recovers at 3.2 s
			affected: true, incAtKill: 1, finalInc: 2,
			records: []execRecord{
				rec(-1000*ms, -900*ms, false, 1, 100*ms),     // failed before the kill
				rec(100*ms, 600*ms, false, 1, 500*ms),        // failed after
				rec(700*ms, 3000*ms, true, 1, 300*ms, 50*ms), // retried, still the old incarnation
				rec(3100*ms, 3200*ms, true, 2, 100*ms),       // first success on the new one
				rec(3300*ms, 3400*ms, true, 0, 100*ms),       // not looked up
			},
		},
		{ // affected, rescheduled, recovers at 2.0 s
			affected: true, incAtKill: 1, finalInc: 3,
			records: []execRecord{
				rec(200*ms, 300*ms, false, 1, 100*ms),
				rec(400*ms, 2000*ms, true, 3, 1600*ms),
			},
		},
	}
	f := groupLossMetrics(sessions, kill, at(3000*ms), 10*time.Second)
	want := groupLossFigures{
		affected: 2, rescheduled: 2, recovered: 2,
		total: 11, failed: 3, failedBeforeKill: 1, failedUnaffected: 1, spanning: 1,
		maxTry: 1600 * ms, maxRequest: 2300 * ms,
		recovery: 3200 * ms, recoveryOK: true,
		detection: 3000 * ms, detectionOK: true,
	}
	got := f
	got.before, got.after = nil, nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("figures\n got %+v\nwant %+v", got, want)
	}
	if fmt.Sprint(f.before) != "[100]" || fmt.Sprint(f.after) != "[200 50 100 100 1600]" {
		t.Errorf("before %v after %v: want [100] and the last-try latencies [200 50 100 100 1600]", f.before, f.after)
	}

	// With a 3 s observe window the 3.2 s recovery and a 3.5 s detection are too late.
	f = groupLossMetrics(sessions, kill, at(3500*ms), 3*time.Second)
	if f.recoveryOK || f.recovered != 1 || f.detectionOK {
		t.Errorf("3 s window: recoveryOK %v recovered %d detectionOK %v, want false, 1, false", f.recoveryOK, f.recovered, f.detectionOK)
	}
	// Not detected at all, and nobody affected.
	f = groupLossMetrics(sessions[:1], kill, time.Time{}, 10*time.Second)
	if f.detectionOK || f.recoveryOK || f.affected != 0 {
		t.Errorf("no detection, no affected: %+v", f)
	}
}
