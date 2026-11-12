package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bench"
)

// localFailureDomain heads every summary and the notes of every run: what was lost is a
// group of processes in one VM, nothing more.
const localFailureDomain = "local failure-domain test: both groups are processes in one VM on one laptop"

const (
	groupLossCode    = "print(1)"
	groupLossRetries = 3                     // retries of a failed Exec with --retry
	detectionPoll    = 50 * time.Millisecond // ListNodes interval after the kill
)

type groupLossConfig struct {
	Sessions     int
	Interval     time.Duration // between the starts of one session's Execs
	Warmup       time.Duration // sessions run this long before the kill
	Observe      time.Duration // sessions run this long after the kill
	KillCmd      string        // recorded in the params; Kill runs it
	Kill         func(context.Context) error
	LoseGroup    string
	Retry        bool          // retry a failed Exec on Unavailable or DeadlineExceeded
	RetryBackoff time.Duration // pause before the first retry, doubled before each next one
	ExecDeadline time.Duration // deadline of each Exec try
}

func newGroupLossConfig() groupLossConfig {
	return groupLossConfig{
		Sessions:     12,
		Interval:     200 * time.Millisecond,
		Warmup:       15 * time.Second,
		Observe:      45 * time.Second,
		KillCmd:      "scripts/cluster.sh kill-group b",
		LoseGroup:    "b",
		RetryBackoff: time.Second,
		ExecDeadline: 20 * time.Second,
	}
}

func groupLossCmd(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, cf := newFlagSet("grouploss", stderr)
	cfg := newGroupLossConfig()
	fs.IntVar(&cfg.Sessions, "sessions", cfg.Sessions, "long-lived sandboxes, each running one Exec per interval")
	fs.DurationVar(&cfg.Interval, "interval", cfg.Interval, "time between the starts of one session's Execs")
	fs.DurationVar(&cfg.Warmup, "warmup", cfg.Warmup, "how long sessions run before the kill")
	fs.DurationVar(&cfg.Observe, "observe", cfg.Observe, "how long sessions run after the kill")
	fs.StringVar(&cfg.KillCmd, "kill-cmd", cfg.KillCmd, "shell command that kills every node of --lose-group")
	fs.StringVar(&cfg.LoseGroup, "lose-group", cfg.LoseGroup, "node group the kill command takes down")
	fs.BoolVar(&cfg.Retry, "retry", cfg.Retry, fmt.Sprintf("retry a failed Exec up to %d times on Unavailable or DeadlineExceeded", groupLossRetries))
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if cfg.Sessions < 2 || cfg.Interval <= 0 || cfg.Warmup <= 0 || cfg.Observe <= 0 || cfg.KillCmd == "" || cfg.LoseGroup == "" {
		return errors.New("--sessions must be at least 2, durations positive, --kill-cmd and --lose-group set")
	}
	cfg.Kill = func(ctx context.Context) error {
		cmd := exec.CommandContext(ctx, "sh", "-c", cfg.KillCmd)
		cmd.Stdout, cmd.Stderr = stderr, stderr
		return cmd.Run()
	}
	r, err := cf.measure(args, func(c gssv1.SandboxServiceClient) (*bench.Run, error) {
		return runGroupLoss(ctx, c, cfg, stderr)
	})
	if err != nil {
		return err
	}
	return printGroupLoss(stdout, r, cf.out)
}

func groupLossSpec() *gssv1.SandboxSpec {
	return &gssv1.SandboxSpec{
		Resources: &gssv1.Resources{CpuMillis: densityCPUMillis, MemoryBytes: densityMemBytes},
		Ttl:       durationpb.New(densityTTL),
	}
}

// glSession is one long-lived sandbox and the Execs run in it.
type glSession struct {
	id string
	// From the create response.
	node, group string
	inc         int32
	// Placement at the kill; written under groupLoss.mu before the kill, read under it.
	nodeAtKill, groupAtKill string
	incAtKill               int32
	affected                bool
	// Owned by the session's goroutine until it ends.
	records     []execRecord
	recoveredAt time.Time // end of the first successful Exec on a new incarnation after the kill
	// Set after the sessions stopped.
	finalInc int32
}

// execRecord is one Exec request, with its retries.
type execRecord struct {
	at          time.Time
	incarnation int32 // from GetSandbox after a failed try; 0 when not looked up
	ok          bool
	latency     time.Duration // first try sent -> outcome of the last try
	tries       int
}

type groupLoss struct {
	c    gssv1.SandboxServiceClient
	cfg  groupLossConfig
	stop chan struct{}

	mu     sync.Mutex
	killAt time.Time
}

// runGroupLoss is the local failure-domain test of spec 8.6: sessions run Execs while
// every node of one group is killed, and the run records how long the scheduler took
// to notice and how long until every affected session ran code again.
func runGroupLoss(ctx context.Context, c gssv1.SandboxServiceClient, cfg groupLossConfig, log io.Writer) (*bench.Run, error) {
	nodes, err := listNodes(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	var groups, lose []string
	for _, n := range nodes {
		if ready(n) && !slices.Contains(groups, n.GetInfo().GetGroup()) {
			groups = append(groups, n.GetInfo().GetGroup())
		}
		if n.GetInfo().GetGroup() == cfg.LoseGroup {
			if !ready(n) {
				return nil, fmt.Errorf("node %s of group %s is %v before the kill; every node of the group must be READY", n.GetInfo().GetId(), cfg.LoseGroup, n.GetState())
			}
			lose = append(lose, n.GetInfo().GetId())
		}
	}
	sort.Strings(groups)
	if len(lose) == 0 || len(groups) < 2 {
		return nil, fmt.Errorf("need READY nodes in group %q and in at least one other group; READY groups: %v", cfg.LoseGroup, groups)
	}
	r := newRun("grouploss", map[string]any{
		"sessions":      cfg.Sessions,
		"interval":      cfg.Interval.String(),
		"warmup":        cfg.Warmup.String(),
		"observe":       cfg.Observe.String(),
		"kill_cmd":      cfg.KillCmd,
		"lose_group":    cfg.LoseGroup,
		"lose_nodes":    lose,
		"retry":         cfg.Retry,
		"retry_backoff": fmt.Sprintf("%v doubling, at most %d retries", cfg.RetryBackoff, groupLossRetries),
		"exec_deadline": cfg.ExecDeadline.String(),
		"cpu_millis":    densityCPUMillis,
		"memory_mib":    densityMemBytes >> 20,
		"ttl":           densityTTL.String(),
		"code":          groupLossCode,
	})
	began := time.Now()
	g := &groupLoss{c: c, cfg: cfg, stop: make(chan struct{})}
	var sessions []*glSession
	var cleanup errorTally
	destroyAll := func() {
		for _, s := range sessions {
			if err := destroySandbox(cleanupCtx(ctx), c, s.id); err != nil {
				cleanup.add("destroy "+s.id, err)
			}
		}
	}

	// Sessions, without a group constraint.
	perGroup := map[string]int{}
	for i := range cfg.Sessions {
		sb, err := createSandbox(ctx, c, groupLossSpec())
		if err != nil {
			destroyAll()
			return nil, fmt.Errorf("create session %d of %d: %w", i+1, cfg.Sessions, err)
		}
		sessions = append(sessions, &glSession{id: sb.GetId(), node: sb.GetNodeId(), group: sb.GetNodeGroup(), inc: sb.GetIncarnation()})
		perGroup[sb.GetNodeGroup()]++
	}
	for _, grp := range groups {
		if perGroup[grp] == 0 {
			destroyAll()
			return nil, fmt.Errorf("the %d sessions landed on groups %v; the test needs at least one on each group %v", cfg.Sessions, perGroup, groups)
		}
	}
	fmt.Fprintf(log, "grouploss: %d sessions, per group %v; warm-up %v\n", cfg.Sessions, perGroup, cfg.Warmup)

	// Each session runs one Exec per interval until stop.
	var wg sync.WaitGroup
	for _, s := range sessions {
		wg.Go(func() { g.runSession(ctx, s) })
	}
	abort := func(err error) (*bench.Run, error) {
		close(g.stop)
		wg.Wait()
		destroyAll()
		return nil, err
	}
	if err := sleepCtx(ctx, cfg.Warmup); err != nil {
		return abort(err)
	}

	// Placement at the kill, then the kill. ListSandboxes is the scheduler's view of
	// where each sandbox runs; if it fails, the create responses stand in for it.
	var notes []string
	placed := map[string]*gssv1.Sandbox{}
	lctx, cancel := context.WithTimeout(ctx, queryTimeout)
	list, err := c.ListSandboxes(lctx, &gssv1.ListSandboxesRequest{})
	cancel()
	if err == nil {
		for _, sb := range list.GetSandboxes() {
			placed[sb.GetId()] = sb
		}
	} else {
		notes = append(notes, fmt.Sprintf("ListSandboxes before the kill failed (%v); placement at the kill is taken from the create responses", err))
	}
	g.mu.Lock()
	for _, s := range sessions {
		s.nodeAtKill, s.groupAtKill, s.incAtKill = s.node, s.group, s.inc
		if sb, ok := placed[s.id]; ok {
			s.nodeAtKill, s.groupAtKill, s.incAtKill = sb.GetNodeId(), sb.GetNodeGroup(), sb.GetIncarnation()
		}
		s.affected = s.groupAtKill == cfg.LoseGroup
	}
	g.killAt = time.Now()
	killAt := g.killAt
	g.mu.Unlock()

	// Detection: poll ListNodes from the kill on until every node of the group is DEAD.
	pollCtx, stopPoll := context.WithCancel(ctx)
	detected := make(chan time.Time, 1)
	go g.pollDetection(pollCtx, lose, detected)
	fmt.Fprintf(log, "grouploss: running %q\n", cfg.KillCmd)
	if err := cfg.Kill(ctx); err != nil {
		stopPoll()
		<-detected
		return abort(fmt.Errorf("kill command %q: %w", cfg.KillCmd, err))
	}

	// Observe, then stop. Requests in flight run to their outcome.
	if err := sleepCtx(ctx, time.Until(killAt.Add(cfg.Observe))); err != nil {
		stopPoll()
		<-detected
		return abort(err)
	}
	close(g.stop)
	wg.Wait()
	stopPoll()
	detectedAt := <-detected
	if ctx.Err() != nil {
		destroyAll()
		return nil, ctx.Err()
	}
	finalFailed := 0
	for _, s := range sessions {
		if sb, err := getSandbox(ctx, c, s.id); err == nil {
			s.finalInc = sb.GetIncarnation()
		} else {
			finalFailed++
			cleanup.add("final GetSandbox "+s.id, err)
		}
	}
	destroyAll()
	r.Seconds = time.Since(began).Seconds()

	// Figures.
	var (
		affected, rescheduled, recovered int
		total, failed, failedUnaffected  int
		maxBlocked, lastRecovery         time.Duration
		before, after                    []float64
	)
	for _, s := range sessions {
		if s.affected {
			affected++
			if s.finalInc > s.incAtKill {
				rescheduled++
			}
			if !s.recoveredAt.IsZero() {
				recovered++
				lastRecovery = max(lastRecovery, s.recoveredAt.Sub(killAt))
			}
		}
		for _, rec := range s.records {
			total++
			maxBlocked = max(maxBlocked, rec.latency)
			switch {
			case !rec.ok:
				failed++
				if !s.affected {
					failedUnaffected++
				}
			case rec.at.Before(killAt):
				before = append(before, ms(rec.latency))
			default:
				after = append(after, ms(rec.latency))
			}
		}
		r.Rows = append(r.Rows, s.row(killAt))
	}
	detectionMS, recoveryMS := -1.0, -1.0
	if !detectedAt.IsZero() {
		detectionMS = ms(detectedAt.Sub(killAt))
	} else {
		notes = append(notes, fmt.Sprintf("the scheduler did not list every node of group %s as DEAD within %v of the kill: detection_ms is -1", cfg.LoseGroup, cfg.Observe))
	}
	switch {
	case affected == 0:
		notes = append(notes, fmt.Sprintf("no session was on group %s at the kill: recovery_ms is -1", cfg.LoseGroup))
	case recovered < affected:
		notes = append(notes, fmt.Sprintf("%d of %d affected sessions had no successful Exec on a new incarnation within %v of the kill: recovery_ms is -1",
			affected-recovered, affected, cfg.Observe))
	default:
		recoveryMS = ms(lastRecovery)
	}
	if failedUnaffected > 0 {
		notes = append(notes, fmt.Sprintf("requests_failed_unaffected is %d, not 0: sessions outside group %s lost requests, so this run is not valid", failedUnaffected, cfg.LoseGroup))
	}
	if finalFailed > 0 {
		notes = append(notes, fmt.Sprintf("GetSandbox after the run failed for %d sessions; they do not count as rescheduled", finalFailed))
	}
	r.Scalars = map[string]float64{
		"sessions":                   float64(cfg.Sessions),
		"affected_sessions":          float64(affected),
		"detection_ms":               detectionMS,
		"recovery_ms":                recoveryMS,
		"requests_total":             float64(total),
		"requests_failed":            float64(failed),
		"requests_failed_unaffected": float64(failedUnaffected),
		"max_blocked_exec_ms":        ms(maxBlocked),
		"rescheduled":                float64(rescheduled),
	}
	r.Series = append(r.Series, bench.Summarize("exec_before_ms", "ms", before), bench.Summarize("exec_after_ms", "ms", after))
	r.Errors = failedUnaffected + cleanup.n
	retryNote := "requests are not retried: a request fails when its one Exec fails"
	if cfg.Retry {
		retryNote = fmt.Sprintf("a request is retried up to %d times on Unavailable or DeadlineExceeded, after %v and then twice the previous pause, and fails only if every try fails",
			groupLossRetries, cfg.RetryBackoff)
	}
	r.Notes = append([]string{
		localFailureDomain,
		fmt.Sprintf("kill command: %s; affected sessions are those whose sandbox was on group %s at the kill (ListSandboxes just before it)", cfg.KillCmd, cfg.LoseGroup),
		fmt.Sprintf("detection_ms is the kill -> the first ListNodes (polled every %v) that lists every node of group %s as DEAD", detectionPoll, cfg.LoseGroup),
		"recovery_ms is the kill -> the end of the last affected session's first successful Exec on a new incarnation (GetSandbox right after the Exec shows an incarnation above the one at the kill)",
		fmt.Sprintf("each session starts one Exec of %s every %v (later if the previous one took longer), each try with a %v deadline; a request counts as failed when its stream ends with a gRPC status, without an exit event, or with a non-zero exit", groupLossCode, cfg.Interval, cfg.ExecDeadline),
		retryNote,
		"requests_failed_unaffected counts failed requests of sessions not on the lost group; it must be 0 for a valid run",
		"max_blocked_exec_ms is the longest request (first try sent -> final outcome); exec_before_ms and exec_after_ms are successful requests started before and after the kill",
		"rescheduled counts affected sessions whose incarnation after the run is above the one at the kill",
		"errors counts failed requests of unaffected sessions and failed GetSandbox or destroy calls after the run",
	}, notes...)
	r.Notes = append(r.Notes, cleanup.notes()...)
	return r, nil
}

// runSession runs one Exec per interval in s until stop. A request that is in flight
// when stop closes runs to its outcome and is recorded.
func (g *groupLoss) runSession(ctx context.Context, s *glSession) {
	for {
		select {
		case <-g.stop:
			return
		default:
		}
		start := time.Now()
		tries, err := g.request(ctx, s.id)
		end := time.Now()
		if ctx.Err() != nil {
			return // cut off by Ctrl-C: not an outcome
		}
		rec := execRecord{at: start, ok: err == nil, latency: end.Sub(start), tries: tries}
		if err != nil || tries > 1 {
			if sb, gerr := getSandbox(ctx, g.c, s.id); gerr == nil {
				rec.incarnation = sb.GetIncarnation()
			}
		}
		g.mu.Lock()
		killAt, affected, incAtKill := g.killAt, s.affected, s.incAtKill
		g.mu.Unlock()
		if rec.ok && affected && s.recoveredAt.IsZero() && !killAt.IsZero() && end.After(killAt) {
			// The Exec ran on a new incarnation if the sandbox is past the one at the kill
			// right after it: an old incarnation cannot answer once its node is dead, and
			// the move to a new one comes seconds after that.
			inc := rec.incarnation
			if inc == 0 {
				if sb, gerr := getSandbox(ctx, g.c, s.id); gerr == nil {
					inc = sb.GetIncarnation()
				}
			}
			if inc > incAtKill {
				s.recoveredAt = end
			}
		}
		s.records = append(s.records, rec)
		if wait := time.Until(start.Add(g.cfg.Interval)); wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-g.stop:
				t.Stop()
				return
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
	}
}

// request runs one Exec of groupLossCode in id, retried when the config says so. It
// returns the number of tries and the error of the last one.
func (g *groupLoss) request(ctx context.Context, id string) (int, error) {
	pause := g.cfg.RetryBackoff
	for tries := 1; ; tries++ {
		_, err := execCode(ctx, g.c, id, groupLossCode, g.cfg.ExecDeadline)
		if err == nil || !g.cfg.Retry || tries > groupLossRetries {
			return tries, err
		}
		if code := status.Code(err); code != codes.Unavailable && code != codes.DeadlineExceeded {
			return tries, err
		}
		if sleepCtx(ctx, pause) != nil {
			return tries, err
		}
		pause *= 2
	}
}

// pollDetection sends the time of the first ListNodes that lists every node in lose as
// DEAD, or the zero time if ctx ends first.
func (g *groupLoss) pollDetection(ctx context.Context, lose []string, out chan<- time.Time) {
	var at time.Time
	defer func() { out <- at }()
	tick := time.NewTicker(detectionPoll)
	defer tick.Stop()
	for {
		if nodes, err := listNodes(ctx, g.c); err == nil && allDead(nodes, lose) {
			at = time.Now()
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func allDead(nodes []*gssv1.Node, ids []string) bool {
	for _, id := range ids {
		i := slices.IndexFunc(nodes, func(n *gssv1.Node) bool { return n.GetInfo().GetId() == id })
		if i < 0 || nodes[i].GetState() != gssv1.NodeState_NODE_STATE_DEAD {
			return false
		}
	}
	return true
}

// row is the session's line in the results file.
func (s *glSession) row(killAt time.Time) map[string]any {
	failed := 0
	var maxLatency time.Duration
	for _, rec := range s.records {
		if !rec.ok {
			failed++
		}
		maxLatency = max(maxLatency, rec.latency)
	}
	recovery := -1.0
	if !s.recoveredAt.IsZero() {
		recovery = ms(s.recoveredAt.Sub(killAt))
	}
	return map[string]any{
		"sandbox":             s.id,
		"node_at_kill":        s.nodeAtKill,
		"group_at_kill":       s.groupAtKill,
		"affected":            s.affected,
		"incarnation_at_kill": s.incAtKill,
		"incarnation_final":   s.finalInc,
		"requests":            len(s.records),
		"failed":              failed,
		"recovery_ms":         recovery,
		"max_request_ms":      ms(maxLatency),
	}
}

func printGroupLoss(w io.Writer, r *bench.Run, out string) error {
	s := r.Scalars
	before, after := bench.Series{}, bench.Series{}
	for _, x := range r.Series {
		switch x.Name {
		case "exec_before_ms":
			before = x
		case "exec_after_ms":
			after = x
		}
	}
	_, err := fmt.Fprintf(w, `%s
retry: %v
sessions: %.0f, affected: %.0f, rescheduled: %.0f
detection_ms: %.0f
recovery_ms: %.0f
requests: %.0f, failed: %.0f, failed on unaffected sessions: %.0f
max_blocked_exec_ms: %.0f
exec before the kill: n %d, p50 %.1f ms, p95 %.1f ms; after: n %d, p50 %.1f ms, p95 %.1f ms
wrote %s
`, localFailureDomain, r.Params["retry"],
		s["sessions"], s["affected_sessions"], s["rescheduled"],
		s["detection_ms"], s["recovery_ms"],
		s["requests_total"], s["requests_failed"], s["requests_failed_unaffected"],
		s["max_blocked_exec_ms"],
		before.Count, before.P50, before.P95, after.Count, after.P50, after.P95, out)
	if err != nil {
		return err
	}
	if s["requests_failed_unaffected"] != 0 {
		_, err = fmt.Fprintln(w, "warning: requests failed on sessions outside the lost group; this run is not valid")
	}
	return err
}
