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
	Interval     time.Duration // between the starts of one session's requests
	Warmup       time.Duration // sessions run this long before the kill
	Observe      time.Duration // sessions run this long after the kill
	KillCmd      string        // recorded in the params; Kill runs it
	Kill         func(context.Context) error
	LoseGroup    string
	Retry        bool          // retry a failed Exec on Unavailable or DeadlineExceeded
	RetryBackoff time.Duration // pause before the first retry, doubled before each next one
	ExecDeadline time.Duration // deadline of each Exec try
	SchedPolicy  string        // placement policy the scheduler was started with, as told by the caller
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
	fs.DurationVar(&cfg.Interval, "interval", cfg.Interval, "time between the starts of one session's requests")
	fs.DurationVar(&cfg.Warmup, "warmup", cfg.Warmup, "how long sessions run before the kill")
	fs.DurationVar(&cfg.Observe, "observe", cfg.Observe, "how long sessions run after the kill")
	fs.StringVar(&cfg.KillCmd, "kill-cmd", cfg.KillCmd, "shell command that kills every node of --lose-group")
	fs.StringVar(&cfg.LoseGroup, "lose-group", cfg.LoseGroup, "node group the kill command takes down")
	fs.BoolVar(&cfg.Retry, "retry", cfg.Retry, fmt.Sprintf("retry a failed Exec up to %d times on Unavailable or DeadlineExceeded", groupLossRetries))
	fs.StringVar(&cfg.SchedPolicy, "sched-policy", "", "placement policy the scheduler was started with, recorded in the results (group-loss.sh: spread, D35)")
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

// policyNote says which placement policy the run used and, for spread, why (D35).
func policyNote(policy, loseGroup string) string {
	switch policy {
	case "":
		return "the scheduler's placement policy was not recorded (--sched-policy not given)"
	case "spread":
		return fmt.Sprintf("the scheduler ran with --policy spread (D35), which splits the sessions evenly between the groups on every run: "+
			"under the default policy the split depends on warm-pool refill and heartbeat timing, so the number of sessions lost with group %s would vary from run to run", loseGroup)
	}
	return "the scheduler ran with --policy " + policy
}

// execRecord is one request: an Exec, with its retries.
type execRecord struct {
	start, end  time.Time       // first try sent; outcome of the last try
	tries       []time.Duration // each try, sent -> outcome, in order; the last one decided the request
	ok          bool
	incarnation int32 // from GetSandbox right after the request; 0 when not looked up
}

// sessionResult is what the figures are computed from for one session.
type sessionResult struct {
	affected  bool  // on the lost group at the kill
	incAtKill int32 // incarnation at the kill
	finalInc  int32 // incarnation after the run; 0 when it could not be read
	records   []execRecord
}

// glSession is one long-lived sandbox and the requests run in it.
type glSession struct {
	id string
	// From the create response.
	node, group string
	inc         int32
	// Placement at the kill. affected and incAtKill are written under groupLoss.mu before
	// the kill and read under it by the session; records belong to the session's
	// goroutine until it ends; finalInc is set after the sessions stopped.
	nodeAtKill, groupAtKill string
	sessionResult
}

type groupLoss struct {
	c    gssv1.SandboxServiceClient
	cfg  groupLossConfig
	stop chan struct{}

	mu      sync.Mutex
	killAt  time.Time
	lookups errorTally // failed GetSandbox and ListNodes calls
}

func (g *groupLoss) lookupFailed(what string, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.lookups.add(what, err)
}

// runGroupLoss is the local failure-domain test: sessions run Execs while
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
		"sched_policy":  cfg.SchedPolicy,
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

	// Each session runs one request per interval until stop.
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

	// Detection: poll ListNodes from the kill until every node of the group is DEAD, for
	// at most the observe window.
	pollCtx, stopPoll := context.WithDeadline(ctx, killAt.Add(cfg.Observe))
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
	for _, s := range sessions {
		if sb, err := getSandbox(ctx, c, s.id); err == nil {
			s.finalInc = sb.GetIncarnation()
		} else {
			g.lookupFailed("GetSandbox "+s.id+" after the run", err)
		}
	}
	destroyAll()
	r.Seconds = time.Since(began).Seconds()

	results := make([]sessionResult, len(sessions))
	for i, s := range sessions {
		results[i] = s.sessionResult
		r.Rows = append(r.Rows, s.row(killAt, cfg.Observe))
	}
	f := groupLossMetrics(results, killAt, detectedAt, cfg.Observe)
	detectionMS, recoveryMS := -1.0, -1.0
	if f.detectionOK {
		detectionMS = ms(f.detection)
	} else {
		notes = append(notes, fmt.Sprintf("the scheduler did not list every node of group %s as DEAD within %v of the kill: detection_ms is -1", cfg.LoseGroup, cfg.Observe))
	}
	switch {
	case f.affected == 0:
		notes = append(notes, fmt.Sprintf("no session was on group %s at the kill: recovery_ms is -1", cfg.LoseGroup))
	case !f.recoveryOK:
		notes = append(notes, fmt.Sprintf("%d of %d affected sessions had no successful request on a new incarnation within %v of the kill: recovery_ms is -1",
			f.affected-f.recovered, f.affected, cfg.Observe))
	default:
		recoveryMS = ms(f.recovery)
	}
	if f.failedUnaffected > 0 {
		notes = append(notes, fmt.Sprintf("requests_failed_unaffected is %d, not 0: sessions outside group %s lost requests, so this run is not valid", f.failedUnaffected, cfg.LoseGroup))
	}
	if f.failedBeforeKill > 0 {
		notes = append(notes, fmt.Sprintf("%d requests failed before the kill, so the cluster was not steady when it came", f.failedBeforeKill))
	}
	if g.lookups.n > 0 {
		notes = append(notes, fmt.Sprintf("%d lookup calls failed (GetSandbox, ListNodes); each leaves an incarnation unknown or a detection poll empty, and the first are listed below", g.lookups.n))
	}
	r.Scalars = map[string]float64{
		"sessions":                    float64(cfg.Sessions),
		"affected_sessions":           float64(f.affected),
		"detection_ms":                detectionMS,
		"recovery_ms":                 recoveryMS,
		"requests_total":              float64(f.total),
		"requests_failed":             float64(f.failed),
		"requests_failed_before_kill": float64(f.failedBeforeKill),
		"requests_failed_unaffected":  float64(f.failedUnaffected),
		"requests_spanning_kill":      float64(f.spanning),
		"max_blocked_exec_ms":         ms(f.maxTry),
		"max_request_with_retries_ms": ms(f.maxRequest),
		"rescheduled":                 float64(f.rescheduled),
		"lookup_errors":               float64(g.lookups.n),
	}
	r.Series = append(r.Series, bench.Summarize("exec_before_ms", "ms", f.before), bench.Summarize("exec_after_ms", "ms", f.after))
	r.Errors = f.failedUnaffected + g.lookups.n + cleanup.n
	retryNote := "requests are not retried: a request is one Exec try"
	if cfg.Retry {
		retryNote = fmt.Sprintf("a request is retried up to %d times on Unavailable or DeadlineExceeded, after %v and then twice the previous pause, and fails only if every try fails",
			groupLossRetries, cfg.RetryBackoff)
	}
	r.Notes = append([]string{
		localFailureDomain,
		policyNote(cfg.SchedPolicy, cfg.LoseGroup),
		fmt.Sprintf("kill command: %s; affected sessions are those whose sandbox was on group %s at the kill (ListSandboxes just before it)", cfg.KillCmd, cfg.LoseGroup),
		fmt.Sprintf("detection_ms is the kill -> the first ListNodes (polled every %v until the observe window ends) that lists every node of group %s as DEAD", detectionPoll, cfg.LoseGroup),
		"recovery_ms is the kill -> the end of the last affected session's first successful request on a new incarnation (GetSandbox right after it shows an incarnation above the one at the kill), within the observe window",
		fmt.Sprintf("each session starts one request, an Exec of %s, every %v (later if the previous one took longer), each try with a %v deadline; a try fails when its stream ends with a gRPC status, without an exit event, or with a non-zero exit", groupLossCode, cfg.Interval, cfg.ExecDeadline),
		retryNote,
		"requests_failed counts failed requests that did not end before the kill; requests_failed_before_kill counts those that did",
		"requests_failed_unaffected counts failed requests, before or after the kill, of sessions not on the lost group; it must be 0 for a valid run",
		"max_blocked_exec_ms is the longest single Exec try; max_request_with_retries_ms is the longest request from its first try to its final outcome, retry pauses included",
		"exec_before_ms holds successful requests that ended before the kill and exec_after_ms those that started after it, each as the latency of the try that succeeded; requests_spanning_kill counts requests that started before the kill and ended after it, which are in neither series",
		"rescheduled counts affected sessions whose incarnation after the run is above the one at the kill",
		"lookup_errors counts failed GetSandbox and ListNodes calls; errors counts failed requests of unaffected sessions, lookup errors and failed destroys",
	}, notes...)
	r.Notes = append(r.Notes, g.lookups.notes()...)
	r.Notes = append(r.Notes, cleanup.notes()...)
	return r, nil
}

// runSession runs one request per interval in s until stop. A request that is in
// flight when stop closes runs to its outcome and is recorded.
func (g *groupLoss) runSession(ctx context.Context, s *glSession) {
	recoverySeen := false
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
		rec := execRecord{start: start, end: end, tries: tries, ok: err == nil}
		g.mu.Lock()
		killAt, affected, incAtKill := g.killAt, s.affected, s.incAtKill
		g.mu.Unlock()
		// After a failed or retried request, and after each success of an affected
		// session until one runs on a new incarnation, look the incarnation up. A success
		// followed by an incarnation above the one at the kill ran on the new incarnation:
		// the old one cannot answer once its node is dead, and the move comes seconds later.
		checkRecovery := rec.ok && affected && !recoverySeen && !killAt.IsZero() && end.After(killAt)
		if !rec.ok || len(tries) > 1 || checkRecovery {
			if sb, gerr := getSandbox(ctx, g.c, s.id); gerr == nil {
				rec.incarnation = sb.GetIncarnation()
			} else if ctx.Err() == nil {
				g.lookupFailed("GetSandbox "+s.id, gerr)
			}
		}
		if checkRecovery && rec.incarnation > incAtKill {
			recoverySeen = true
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
// returns how long each try took and the error of the last one.
func (g *groupLoss) request(ctx context.Context, id string) ([]time.Duration, error) {
	pause := g.cfg.RetryBackoff
	var tries []time.Duration
	for {
		sent := time.Now()
		exitAt, err := execCode(ctx, g.c, id, groupLossCode, g.cfg.ExecDeadline)
		if err != nil {
			exitAt = time.Now()
		}
		tries = append(tries, exitAt.Sub(sent))
		if err == nil || !g.cfg.Retry || len(tries) > groupLossRetries {
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
		nodes, err := listNodes(ctx, g.c)
		if err == nil && allDead(nodes, lose) {
			at = time.Now()
			return
		}
		if err != nil && ctx.Err() == nil {
			g.lookupFailed("ListNodes", err)
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

// groupLossFigures are the measured figures of one run.
type groupLossFigures struct {
	affected, rescheduled, recovered int
	total, failed, failedBeforeKill  int
	failedUnaffected, spanning       int
	maxTry, maxRequest               time.Duration
	recovery                         time.Duration // valid when recoveryOK
	recoveryOK                       bool          // every affected session recovered within observe, and there was one
	detection                        time.Duration // valid when detectionOK
	detectionOK                      bool
	before, after                    []float64 // ms, latency of the try that succeeded
}

// groupLossMetrics computes the figures from the session records alone. A request that
// ended before killAt is "before", one that started after it is "after", and one that
// started before and ended after spans the kill. Detection and recovery count only
// within observe of killAt.
func groupLossMetrics(sessions []sessionResult, killAt, detectedAt time.Time, observe time.Duration) groupLossFigures {
	var f groupLossFigures
	window := killAt.Add(observe)
	if !detectedAt.IsZero() && !detectedAt.After(window) {
		f.detection, f.detectionOK = detectedAt.Sub(killAt), true
	}
	for _, s := range sessions {
		if s.affected {
			f.affected++
			if s.finalInc > s.incAtKill {
				f.rescheduled++
			}
			if d, ok := sessionRecovery(s, killAt, observe); ok {
				f.recovered++
				f.recovery = max(f.recovery, d)
			}
		}
		for _, rec := range s.records {
			f.total++
			for _, t := range rec.tries {
				f.maxTry = max(f.maxTry, t)
			}
			f.maxRequest = max(f.maxRequest, rec.end.Sub(rec.start))
			if !rec.ok && !s.affected {
				f.failedUnaffected++
			}
			var last float64
			if len(rec.tries) > 0 {
				last = ms(rec.tries[len(rec.tries)-1])
			}
			switch {
			case rec.end.Before(killAt):
				if rec.ok {
					f.before = append(f.before, last)
				} else {
					f.failedBeforeKill++
				}
			case rec.start.After(killAt):
				if rec.ok {
					f.after = append(f.after, last)
				} else {
					f.failed++
				}
			default:
				f.spanning++
				if !rec.ok {
					f.failed++
				}
			}
		}
	}
	f.recoveryOK = f.affected > 0 && f.recovered == f.affected
	if !f.recoveryOK {
		f.recovery = 0
	}
	return f
}

// sessionRecovery returns killAt -> the end of s's first successful request after the
// kill that ran on a new incarnation, if that end is within observe.
func sessionRecovery(s sessionResult, killAt time.Time, observe time.Duration) (time.Duration, bool) {
	for _, rec := range s.records {
		if rec.ok && rec.end.After(killAt) && rec.incarnation > s.incAtKill {
			d := rec.end.Sub(killAt)
			return d, d <= observe
		}
	}
	return 0, false
}

// row is the session's line in the results file.
func (s *glSession) row(killAt time.Time, observe time.Duration) map[string]any {
	failed := 0
	var maxTry, maxRequest time.Duration
	for _, rec := range s.records {
		if !rec.ok {
			failed++
		}
		for _, t := range rec.tries {
			maxTry = max(maxTry, t)
		}
		maxRequest = max(maxRequest, rec.end.Sub(rec.start))
	}
	recovery := -1.0
	if d, ok := sessionRecovery(s.sessionResult, killAt, observe); ok {
		recovery = ms(d)
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
		"max_try_ms":          ms(maxTry),
		"max_request_ms":      ms(maxRequest),
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
	policy, _ := r.Params["sched_policy"].(string)
	lose, _ := r.Params["lose_group"].(string)
	_, err := fmt.Fprintf(w, `%s
%s
retry: %v
sessions: %.0f, affected: %.0f, rescheduled: %.0f
detection_ms: %.0f
recovery_ms: %.0f
requests: %.0f, failed after the kill: %.0f, failed before it: %.0f, failed on unaffected sessions: %.0f, spanning the kill: %.0f
max_blocked_exec_ms (longest single try): %.0f; max_request_with_retries_ms: %.0f
exec before the kill: n %d, p50 %.1f ms, p95 %.1f ms; after: n %d, p50 %.1f ms, p95 %.1f ms
lookup errors: %.0f
wrote %s
`, localFailureDomain, policyNote(policy, lose), r.Params["retry"],
		s["sessions"], s["affected_sessions"], s["rescheduled"],
		s["detection_ms"], s["recovery_ms"],
		s["requests_total"], s["requests_failed"], s["requests_failed_before_kill"], s["requests_failed_unaffected"], s["requests_spanning_kill"],
		s["max_blocked_exec_ms"], s["max_request_with_retries_ms"],
		before.Count, before.P50, before.P95, after.Count, after.P50, after.P95,
		s["lookup_errors"], out)
	if err != nil {
		return err
	}
	if s["requests_failed_unaffected"] != 0 {
		_, err = fmt.Fprintln(w, "warning: requests failed on sessions outside the lost group; this run is not valid")
	}
	return err
}
