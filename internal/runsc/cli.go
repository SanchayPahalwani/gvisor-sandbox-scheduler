package runsc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// stderrTail is how much of runsc's stderr an error message carries.
const stderrTail = 2048

// waitDelay bounds how long a command waits for its stdout and stderr pipes
// after it exits (a process it started may hold them open).
const waitDelay = 2 * time.Second

// checkpointStopWait bounds how long Checkpoint waits for the container to
// leave state "running" after `runsc checkpoint` returns.
const checkpointStopWait = 5 * time.Second

// maxHeldDiag bounds how much stderr Exec holds back while it could still be
// a runsc diagnostic (runsc's usage text is about 2.5 KiB), plus four times
// the size of the request, which runsc quotes.
const maxHeldDiag = 16 << 10

// execStateTimeout bounds the State call Exec makes after an exit 128.
const execStateTimeout = 5 * time.Second

// CLI implements Runtime by running the runsc binary.
type CLI struct {
	Binary string // default "runsc"
	Root   string // runsc --root

	mu   sync.Mutex
	seen map[string]bool // container IDs passed to Create or returned by List
}

var _ Runtime = (*CLI)(nil)

// NewCLI returns a CLI for the given binary (default "runsc") and state root.
func NewCLI(binary, root string) *CLI {
	if binary == "" {
		binary = "runsc"
	}
	return &CLI{Binary: binary, Root: root}
}

// checkRef rejects a Ref that would make runsc act on the wrong thing: an
// empty ID (runsc matches IDs by prefix) or an unknown network mode.
func checkRef(ref Ref) error {
	if ref.ID == "" {
		return errors.New("runsc: empty container id")
	}
	switch ref.Network {
	case "", NetworkNone, NetworkSandbox:
		return nil
	}
	return fmt.Errorf("runsc: network %q is not %q or %q", ref.Network, NetworkNone, NetworkSandbox)
}

// args builds the full argument list: the top-level flags that must be the
// same on every command for a sandbox, then the subcommand and its arguments.
// An empty network means none.
func (c *CLI) args(ref Ref, sub string, rest ...string) []string {
	network := ref.Network
	if network == "" {
		network = NetworkNone
	}
	out := []string{"--root", c.Root, "--network=" + string(network), "--overlay2=none", sub}
	return append(out, rest...)
}

func (c *CLI) createArgs(ref Ref, bundleDir string) []string {
	return c.args(ref, "create", "--bundle", bundleDir, ref.ID)
}

func (c *CLI) startArgs(ref Ref) []string { return c.args(ref, "start", ref.ID) }

func (c *CLI) restoreArgs(ref Ref, bundleDir, imagePath string) []string {
	return c.args(ref, "restore", "--detach", "--image-path", imagePath, "--bundle", bundleDir, ref.ID)
}

func (c *CLI) checkpointArgs(ref Ref, imagePath string) []string {
	return c.args(ref, "checkpoint", "--image-path", imagePath, ref.ID)
}

func (c *CLI) execArgs(ref Ref, spec ExecSpec) []string {
	var rest []string
	if spec.InternalPIDFile != "" {
		rest = append(rest, "--internal-pid-file", spec.InternalPIDFile)
	}
	cwd := spec.Cwd
	if cwd == "" {
		cwd = DefaultCwd
	}
	rest = append(rest, "--cwd", cwd)
	for _, kv := range spec.Env {
		rest = append(rest, "--env", kv)
	}
	rest = append(rest, ref.ID)
	rest = append(rest, spec.Args...)
	return c.args(ref, "exec", rest...)
}

func (c *CLI) killPIDArgs(ref Ref, pid int) []string {
	return c.args(ref, "kill", "--pid", strconv.Itoa(pid), ref.ID, "KILL")
}

func (c *CLI) killArgs(ref Ref) []string   { return c.args(ref, "kill", ref.ID, "KILL") }
func (c *CLI) deleteArgs(ref Ref) []string { return c.args(ref, "delete", "--force", ref.ID) }
func (c *CLI) stateArgs(ref Ref) []string  { return c.args(ref, "state", ref.ID) }

// listArgs and versionArgs name no sandbox, so they carry no network flag.
func (c *CLI) listArgs() []string    { return []string{"--root", c.Root, "list", "-quiet"} }
func (c *CLI) versionArgs() []string { return []string{"--version"} }

// Create runs `runsc create`. The child's stdout and stderr (and so the
// container's) are logPath opened for append; stdin is /dev/null.
func (c *CLI) Create(ctx context.Context, ref Ref, bundleDir string, logPath string) error {
	if err := checkRef(ref); err != nil {
		return err
	}
	c.remember(ref.ID) // for RemoveRoot, even if create fails half way
	log, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("runsc create: open log: %w", err)
	}
	defer log.Close()
	var offset int64
	if fi, err := log.Stat(); err == nil {
		offset = fi.Size()
	}
	cmd := exec.CommandContext(ctx, c.Binary, c.createArgs(ref, bundleDir)...)
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Run(); err != nil {
		return runError(ctx, "create", err, readTail(logPath, offset))
	}
	return nil
}

func (c *CLI) Start(ctx context.Context, ref Ref) error {
	if err := checkRef(ref); err != nil {
		return err
	}
	return c.run(ctx, "start", c.startArgs(ref), nil)
}

func (c *CLI) Restore(ctx context.Context, ref Ref, bundleDir, imagePath string) error {
	if err := checkRef(ref); err != nil {
		return err
	}
	return c.run(ctx, "restore", c.restoreArgs(ref, bundleDir, imagePath), nil)
}

func (c *CLI) Checkpoint(ctx context.Context, ref Ref, imagePath string) error {
	if err := checkRef(ref); err != nil {
		return err
	}
	if err := c.run(ctx, "checkpoint", c.checkpointArgs(ref, imagePath), nil); err != nil {
		return err
	}
	// runsc checkpoint returns while the sandbox process is still exiting,
	// and `runsc state` says "running" until it is gone.
	deadline := time.Now().Add(checkpointStopWait)
	for {
		st, err := c.State(ctx, ref)
		if err != nil {
			return fmt.Errorf("runsc checkpoint: %w", err)
		}
		if st.Status != "running" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("runsc checkpoint %s: still running %v after the checkpoint", ref.ID, checkpointStopWait)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("runsc checkpoint: %w", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Exec runs `runsc exec` and waits for it. The exit code of the process is
// returned with a nil error whenever the process ran and its exit was seen.
//
// runsc exits 128 when it fails, and writes its diagnostics to the same
// stderr as the process. So on exit 128 Exec asks State: a container that is
// gone or not running means the exit was not seen (ErrNotRunning). For a
// running container, stderr that starts like a runsc diagnostic (the process
// never ran) or ends with a runsc failure line is an error. A diagnostic at
// the start is held back while it may still be one, and is then not
// forwarded to spec.Stderr; a process that really exits 128 leaves runsc's
// stderr empty.
func (c *CLI) Exec(ctx context.Context, ref Ref, spec ExecSpec) (int, error) {
	if err := checkRef(ref); err != nil {
		return -1, err
	}
	if len(spec.Args) == 0 {
		return -1, errors.New("runsc exec: empty Args")
	}
	stdout, stderr := newSinks(spec.Stdout, spec.Stderr)
	diag := &diagWriter{maxHeld: maxHeldFor(spec)}
	if stderr != nil {
		diag.w = stderr
	}
	cmd := exec.CommandContext(ctx, c.Binary, c.execArgs(ref, spec)...)
	cmd.WaitDelay = waitDelay
	cmd.Stdin = spec.Stdin
	if stdout != nil {
		cmd.Stdout = stdout
	}
	cmd.Stderr = diag
	runErr := cmd.Run()

	ps := cmd.ProcessState
	if ps == nil || !ps.Exited() {
		diag.flush()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return -1, fmt.Errorf("runsc exec: %w", ctxErr)
		}
		return -1, cmdError("exec", runErr, diag.last.String())
	}
	code := ps.ExitCode()
	if code == 128 {
		if err := c.execFailure(ref, runErr, diag); err != nil {
			return -1, err
		}
	}
	diag.flush()
	var extra error
	var exitErr *exec.ExitError
	if runErr != nil && !errors.As(runErr, &exitErr) && !errors.Is(runErr, exec.ErrWaitDelay) &&
		!errors.Is(runErr, context.Canceled) && !errors.Is(runErr, context.DeadlineExceeded) {
		extra = runErr // e.g. copying Stdin failed
	}
	if err := errors.Join(stdout.error(), stderr.error(), extra); err != nil {
		return code, fmt.Errorf("runsc exec: output: %w", err)
	}
	return code, nil
}

// execFailure decides whether a `runsc exec` exit 128 was runsc failing
// rather than the process exiting 128. It returns nil for a real exit.
func (c *CLI) execFailure(ref Ref, runErr error, diag *diagWriter) error {
	last := diag.last.String()
	// The caller's ctx may be done; the question still needs an answer.
	sctx, cancel := context.WithTimeout(context.Background(), execStateTimeout)
	defer cancel()
	st, err := c.State(sctx, ref)
	switch {
	case errors.Is(err, ErrNotFound):
		return fmt.Errorf("runsc exec %s: exit not seen: %w: %w: %s", ref.ID, ErrNotRunning, ErrNotFound, last)
	case err != nil:
		// Nothing confirmed the exit (a timeout, a Delete holding the lock).
		return fmt.Errorf("runsc exec %s: exit not seen, state unknown: %w: %w: %s", ref.ID, ErrNotRunning, err, last)
	case st.Status != "running":
		return fmt.Errorf("runsc exec %s: exit not seen, container is %s: %w: %s", ref.ID, st.Status, ErrNotRunning, last)
	}
	// The container is running, so no sentinel: whatever the text says, it
	// is not "not found" or "not running".
	if diag.isDiag() {
		return diagError(runErr, string(diag.held))
	}
	if line := lastLine(last); strings.HasPrefix(line, "writing internal pid file:") || strings.HasPrefix(line, "waiting on pid ") {
		return fmt.Errorf("runsc exec: %w: %s", runErr, line)
	}
	return nil
}

// diagError turns a runsc exec diagnostic into a plain error. It carries no
// sentinel: it is used only once State said the container is running, and
// the text may quote the user's argv.
func diagError(runErr error, diag string) error {
	return fmt.Errorf("runsc exec: %w: %s", runErr, tail([]byte(diag)))
}

// maxHeldFor sizes the diagnostic hold: runsc quotes the whole argv (and
// may quote cwd and env) in its message.
func maxHeldFor(spec ExecSpec) int {
	n := len(spec.Cwd)
	for _, a := range spec.Args {
		n += len(a)
	}
	for _, e := range spec.Env {
		n += len(e)
	}
	return maxHeldDiag + 4*n
}

// lastLine returns the last non-empty line of s.
func lastLine(s string) string {
	s = strings.TrimRight(s, "\n")
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		return s[i+1:]
	}
	return s
}

func (c *CLI) KillProcess(ctx context.Context, ref Ref, internalPID int) error {
	if err := checkRef(ref); err != nil {
		return err
	}
	return c.run(ctx, "kill", c.killPIDArgs(ref, internalPID), nil)
}

// Delete sends SIGKILL to the container and force-deletes it. A container
// that does not exist is not an error. It asks State first because runsc
// matches IDs by prefix: deleting a gone "a" would otherwise delete "ab".
func (c *CLI) Delete(ctx context.Context, ref Ref) error {
	if err := checkRef(ref); err != nil {
		return err
	}
	if _, err := c.State(ctx, ref); errors.Is(err, ErrNotFound) {
		return nil
	}
	// The kill fails for a container that is stopped; delete --force is what
	// must succeed.
	_ = c.run(ctx, "kill", c.killArgs(ref), nil)
	if err := c.run(ctx, "delete", c.deleteArgs(ref), nil); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	return nil
}

// State returns ErrNotFound if no container has exactly ref.ID (runsc would
// otherwise report a container whose ID starts with it).
func (c *CLI) State(ctx context.Context, ref Ref) (State, error) {
	if err := checkRef(ref); err != nil {
		return State{}, err
	}
	var out bytes.Buffer
	if err := c.run(ctx, "state", c.stateArgs(ref), &out); err != nil {
		return State{}, err
	}
	st, err := parseState(out.Bytes())
	if err != nil {
		return State{}, fmt.Errorf("runsc state: %w", err)
	}
	if st.ID != ref.ID {
		return State{}, fmt.Errorf("runsc state %s: matched container %q: %w", ref.ID, st.ID, ErrNotFound)
	}
	return st, nil
}

func (c *CLI) List(ctx context.Context) ([]string, error) {
	var out bytes.Buffer
	if err := c.run(ctx, "list", c.listArgs(), &out); err != nil {
		return nil, err
	}
	ids := parseList(out.Bytes())
	c.remember(ids...)
	return ids, nil
}

func (c *CLI) Version(ctx context.Context) (string, error) {
	var out bytes.Buffer
	if err := c.run(ctx, "version", c.versionArgs(), &out); err != nil {
		return "", err
	}
	v, err := parseVersion(out.Bytes())
	if err != nil {
		return "", fmt.Errorf("runsc --version: %w", err)
	}
	return v, nil
}

// nullNetns is the file in the root that runsc bind-mounts a network
// namespace on (seen with --network=none).
const nullNetns = "null-netns"

// umount unmounts path; tests replace it.
var umount = func(ctx context.Context, path string) error {
	if out, err := exec.CommandContext(ctx, "umount", path).CombinedOutput(); err != nil {
		return fmt.Errorf("umount %s: %w: %s", path, err, tail(out))
	}
	return nil
}

func (c *CLI) remember(ids ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seen == nil {
		c.seen = map[string]bool{}
	}
	for _, id := range ids {
		c.seen[id] = true
	}
}

// containers returns the container IDs a root directory with entries ents
// is known to hold things for: those this CLI created or listed, and those
// named by a state file ("<container>_sandbox:<sandbox>.state").
func (c *CLI) containers(ents []os.DirEntry) map[string]bool {
	ids := map[string]bool{}
	c.mu.Lock()
	for id := range c.seen {
		ids[id] = true
	}
	c.mu.Unlock()
	for _, e := range ents {
		if name, ok := strings.CutSuffix(e.Name(), ".state"); ok && !e.IsDir() {
			if id, _, ok := strings.Cut(name, "_sandbox:"); ok {
				ids[id] = true
			}
		}
	}
	return ids
}

// runscEntry reports whether e, an entry of the root, is one runsc makes
// there: null-netns, a "*.state" or "*.lock" file, a sandbox's control
// socket, or a directory named after a container in containers.
//
// The control socket is "runsc-<sandbox-id>.sock" (controlSocketName in
// gVisor's runsc/sandbox/sandbox.go, created in the root when the path
// fits). It stays when a sandbox dies without runsc deleting it.
func runscEntry(e os.DirEntry, containers map[string]bool) bool {
	name := e.Name()
	if e.IsDir() {
		return containers[name]
	}
	return name == nullNetns || strings.HasSuffix(name, ".state") || strings.HasSuffix(name, ".lock") ||
		strings.HasPrefix(name, "runsc-") && strings.HasSuffix(name, ".sock")
}

// RemoveRoot removes the root directory and what runsc keeps in it, and
// nothing else: it unmounts and removes null-netns, removes the "*.state"
// and "*.lock" files, the control sockets and the directories named after a
// container (one this CLI created or listed, or one a state file names),
// and then removes the directory itself, which fails, naming what is left,
// if anything else is in it. A missing root is not an error. Call it only
// after every container under the root was deleted.
func (c *CLI) RemoveRoot(ctx context.Context) error {
	if c.Root == "" || filepath.Clean(c.Root) == "/" {
		return fmt.Errorf("runsc: refusing to remove root %q", c.Root)
	}
	ents, err := os.ReadDir(c.Root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("runsc: remove root: %w", err)
	}
	known := c.containers(ents)
	var errs, left []string
	for _, e := range ents {
		if !runscEntry(e, known) {
			left = append(left, e.Name())
			continue
		}
		p := filepath.Join(c.Root, e.Name())
		var umountErr error
		if e.Name() == nullNetns {
			// Not mounted is fine; the Remove below fails on a mount that stayed.
			umountErr = umount(ctx, p)
		}
		if err := os.RemoveAll(p); err != nil {
			errs = append(errs, errors.Join(umountErr, err).Error())
		}
	}
	if len(left) > 0 {
		errs = append(errs, fmt.Sprintf("it holds entries runsc does not make, left in place: %q", left))
	} else if err := os.Remove(c.Root); err != nil {
		errs = append(errs, err.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("runsc: root %s not removed: %s", c.Root, strings.Join(errs, "; "))
	}
	return nil
}

// run executes runsc with the tail of its stderr kept in memory.
func (c *CLI) run(ctx context.Context, sub string, args []string, stdout io.Writer) error {
	var errTail tailWriter
	cmd := exec.CommandContext(ctx, c.Binary, args...)
	cmd.WaitDelay = waitDelay
	cmd.Stdout = stdout
	cmd.Stderr = &errTail
	err := cmd.Run()
	if errors.Is(err, exec.ErrWaitDelay) && cmd.ProcessState != nil && cmd.ProcessState.Success() {
		// runsc succeeded; a process it started still holds the pipes.
		return nil
	}
	if err != nil {
		return runError(ctx, sub, err, errTail.String())
	}
	return nil
}

// runError is cmdError, wrapping the ctx error when ctx ended the command.
func runError(ctx context.Context, sub string, err error, stderr string) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("runsc %s: %w: %w", sub, ctxErr, cmdError(sub, err, stderr))
	}
	return cmdError(sub, err, stderr)
}

// cmdError wraps a failed runsc command with its subcommand and stderr tail,
// and with ErrNotFound or ErrNotRunning when stderr says so.
func cmdError(sub string, err error, stderr string) error {
	stderr = tail([]byte(stderr))
	switch {
	case isNotFound(stderr):
		return fmt.Errorf("runsc %s: %w: %w: %s", sub, ErrNotFound, err, stderr)
	case isNotRunning(stderr):
		return fmt.Errorf("runsc %s: %w: %w: %s", sub, ErrNotRunning, err, stderr)
	case stderr == "":
		return fmt.Errorf("runsc %s: %w", sub, err)
	}
	return fmt.Errorf("runsc %s: %w: %s", sub, err, stderr)
}

// isNotFound reports whether runsc's stderr says the container does not exist
// ("FetchSpec failed: loading container: file does not exist").
func isNotFound(stderr string) bool {
	return strings.Contains(stderr, "loading container") && strings.Contains(stderr, "file does not exist")
}

// isNotRunning reports whether runsc's stderr says the container cannot run
// a process: "cannot execute in container ... in state stopped" (its init
// exited, or it was checkpointed) or "connecting to control server ...:
// connection refused" (the sandbox process is gone, seen right after a
// checkpoint).
func isNotRunning(stderr string) bool {
	return strings.Contains(stderr, "in state stopped") ||
		(strings.Contains(stderr, "connecting to control server") && strings.Contains(stderr, "connection refused"))
}

// execDiagPrefixes are how runsc's own messages from `runsc exec` begin.
var execDiagPrefixes = []string{
	"executing processes for container:", // could not start the process (failed to load ..., cwd, state)
	"exec [command options]",             // usage text: bad arguments
	"FetchSpec failed:",                  // could not load the container
}

// isExecDiag reports whether stderr is a runsc diagnostic rather than output
// of a process.
func isExecDiag(stderr string) bool {
	for _, p := range execDiagPrefixes {
		if strings.HasPrefix(stderr, p) {
			return true
		}
	}
	return false
}

// couldBeExecDiag reports whether b is, or may grow into, a runsc diagnostic.
func couldBeExecDiag(b []byte) bool {
	for _, p := range execDiagPrefixes {
		if len(b) <= len(p) && strings.HasPrefix(p, string(b)) || bytes.HasPrefix(b, []byte(p)) {
			return true
		}
	}
	return false
}

// diagWriter is Exec's stderr: it forwards to w, but holds back output for
// as long as it could be a runsc diagnostic, and keeps the last stderrTail
// bytes of everything in last. Only the exec stderr goroutine writes to it;
// the other methods are called after the command finished.
type diagWriter struct {
	w       io.Writer // nil discards
	maxHeld int
	held    []byte
	passing bool
	last    tailWriter
}

func (d *diagWriter) Write(p []byte) (int, error) {
	d.last.Write(p)
	if d.passing {
		d.forward(p)
		return len(p), nil
	}
	d.held = append(d.held, p...)
	if len(d.held) <= d.maxHeld && couldBeExecDiag(d.held) {
		return len(p), nil
	}
	d.flush()
	return len(p), nil
}

func (d *diagWriter) forward(p []byte) {
	if d.w != nil && len(p) > 0 {
		d.w.Write(p) // errors are recorded by the sink
	}
}

// flush forwards what is held and passes everything through from then on.
func (d *diagWriter) flush() {
	d.passing = true
	d.forward(d.held)
	d.held = nil
}

func (d *diagWriter) isDiag() bool { return !d.passing && isExecDiag(string(d.held)) }

// sink wraps a caller's writer: it serialises writes with a mutex (shared
// when Stdout and Stderr are the same writer), records the first write error
// and then discards, so the process never blocks on a failed writer.
type sink struct {
	mu  *sync.Mutex
	w   io.Writer
	err error
}

func (s *sink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		if _, err := s.w.Write(p); err != nil {
			s.err = err
		}
	}
	return len(p), nil
}

func (s *sink) error() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// newSinks wraps the caller's writers; a nil writer gives a nil sink.
func newSinks(stdout, stderr io.Writer) (*sink, *sink) {
	var out, errS *sink
	if stdout != nil {
		out = &sink{mu: &sync.Mutex{}, w: stdout}
	}
	if stderr != nil {
		errS = &sink{mu: &sync.Mutex{}, w: stderr}
		if out != nil && sameWriter(stdout, stderr) {
			errS.mu = out.mu
		}
	}
	return out, errS
}

// sameWriter reports whether a and b are the same value. Comparing can panic
// for a comparable type that holds an uncomparable one (a struct with an
// interface field holding a slice type); that counts as different.
func sameWriter(a, b io.Writer) (same bool) {
	defer func() {
		if recover() != nil {
			same = false
		}
	}()
	return a == b
}

func parseState(b []byte) (State, error) {
	var s struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		PID    int    `json:"pid"`
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return State{}, fmt.Errorf("parse state: %w", err)
	}
	return State{ID: s.ID, Status: s.Status, PID: s.PID}, nil
}

// parseVersion reads "runsc version <release>" from the first line.
func parseVersion(b []byte) (string, error) {
	first, _, _ := strings.Cut(string(b), "\n")
	f := strings.Fields(first)
	if len(f) != 3 || f[0] != "runsc" || f[1] != "version" {
		return "", fmt.Errorf("unexpected version output %q", tail([]byte(first)))
	}
	return f[2], nil
}

func parseList(b []byte) []string {
	ids := []string{}
	for _, line := range strings.Split(string(b), "\n") {
		if id := strings.TrimSpace(line); id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

// tail returns the last stderrTail bytes of b, with surrounding space trimmed.
func tail(b []byte) string {
	if len(b) > stderrTail {
		b = b[len(b)-stderrTail:]
	}
	return strings.TrimSpace(string(b))
}

// readTail returns the tail of the file at path from offset on.
func readTail(path string, offset int64) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	if fi, err := f.Stat(); err == nil && fi.Size()-offset > stderrTail {
		offset = fi.Size() - stderrTail
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return ""
	}
	b, _ := io.ReadAll(io.LimitReader(f, stderrTail))
	return tail(b)
}

// tailWriter keeps the last stderrTail bytes written to it.
type tailWriter struct{ buf []byte }

func (w *tailWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	if len(w.buf) > stderrTail {
		w.buf = append(w.buf[:0], w.buf[len(w.buf)-stderrTail:]...)
	}
	return len(p), nil
}

func (w *tailWriter) String() string { return tail(w.buf) }
