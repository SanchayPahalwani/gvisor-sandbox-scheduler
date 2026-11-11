package runsc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// stderrTail is how much of runsc's stderr an error message carries.
const stderrTail = 2048

// execWaitDelay bounds how long Exec waits for the stdout and stderr pipes
// after the process exits (a background child may hold them open).
const execWaitDelay = 2 * time.Second

// CLI implements Runtime by running the runsc binary.
type CLI struct {
	Binary string // default "runsc"
	Root   string // runsc --root
}

var _ Runtime = (*CLI)(nil)

// NewCLI returns a CLI for the given binary (default "runsc") and state root.
func NewCLI(binary, root string) *CLI {
	if binary == "" {
		binary = "runsc"
	}
	return &CLI{Binary: binary, Root: root}
}

// args builds the full argument list: the top-level flags that must be the
// same on every command for a sandbox, then the subcommand and its arguments.
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
		return cmdError("create", err, readTail(logPath, offset))
	}
	return nil
}

func (c *CLI) Start(ctx context.Context, ref Ref) error {
	return c.run(ctx, "start", c.startArgs(ref), nil)
}

func (c *CLI) Restore(ctx context.Context, ref Ref, bundleDir, imagePath string) error {
	return c.run(ctx, "restore", c.restoreArgs(ref, bundleDir, imagePath), nil)
}

func (c *CLI) Checkpoint(ctx context.Context, ref Ref, imagePath string) error {
	return c.run(ctx, "checkpoint", c.checkpointArgs(ref, imagePath), nil)
}

// Exec runs `runsc exec` and waits for it. The exit code of the process is
// returned with a nil error whenever the process ran.
func (c *CLI) Exec(ctx context.Context, ref Ref, spec ExecSpec) (int, error) {
	var errTail tailWriter
	cmd := exec.CommandContext(ctx, c.Binary, c.execArgs(ref, spec)...)
	cmd.WaitDelay = execWaitDelay
	cmd.Stdin = spec.Stdin
	cmd.Stdout = spec.Stdout
	if spec.Stderr != nil {
		cmd.Stderr = io.MultiWriter(spec.Stderr, &errTail)
	} else {
		cmd.Stderr = &errTail
	}
	err := cmd.Run()
	if ctxErr := ctx.Err(); err != nil && ctxErr != nil {
		return -1, fmt.Errorf("runsc exec: %w", ctxErr)
	}
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return 0, nil
	case errors.As(err, &exitErr):
		code := exitErr.ExitCode()
		if code < 0 {
			return -1, cmdError("exec", err, errTail.String())
		}
		// runsc itself exits 128 when it cannot reach the container.
		if code == 128 && isNotFound(errTail.String()) {
			return -1, cmdError("exec", err, errTail.String())
		}
		return code, nil
	case errors.Is(err, exec.ErrWaitDelay) && cmd.ProcessState != nil:
		// The process exited; something it started still holds the pipes.
		return cmd.ProcessState.ExitCode(), nil
	default:
		return -1, cmdError("exec", err, errTail.String())
	}
}

func (c *CLI) KillProcess(ctx context.Context, ref Ref, internalPID int) error {
	return c.run(ctx, "kill", c.killPIDArgs(ref, internalPID), nil)
}

// Delete sends SIGKILL to the container and force-deletes it. A container
// that does not exist is not an error.
func (c *CLI) Delete(ctx context.Context, ref Ref) error {
	// The kill fails for a container that is stopped or gone; delete --force
	// is what must succeed.
	_ = c.run(ctx, "kill", c.killArgs(ref), nil)
	if err := c.run(ctx, "delete", c.deleteArgs(ref), nil); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	return nil
}

func (c *CLI) State(ctx context.Context, ref Ref) (State, error) {
	var out bytes.Buffer
	if err := c.run(ctx, "state", c.stateArgs(ref), &out); err != nil {
		return State{}, err
	}
	st, err := parseState(out.Bytes())
	if err != nil {
		return State{}, fmt.Errorf("runsc state: %w", err)
	}
	return st, nil
}

func (c *CLI) List(ctx context.Context) ([]string, error) {
	var out bytes.Buffer
	if err := c.run(ctx, "list", c.listArgs(), &out); err != nil {
		return nil, err
	}
	return parseList(out.Bytes()), nil
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

// RemoveRoot unmounts the null-netns bind mount runsc leaves in the root
// directory, then removes the directory. A missing root is not an error.
func (c *CLI) RemoveRoot(ctx context.Context) error {
	if c.Root == "" || filepath.Clean(c.Root) == "/" {
		return fmt.Errorf("runsc: refusing to remove root %q", c.Root)
	}
	var umountErr error
	netns := filepath.Join(c.Root, "null-netns")
	if _, err := os.Lstat(netns); err == nil {
		if out, err := exec.CommandContext(ctx, "umount", netns).CombinedOutput(); err != nil {
			// Not mounted is fine; RemoveAll below reports a mount that stayed.
			umountErr = fmt.Errorf("umount %s: %w: %s", netns, err, tail(out))
		}
	}
	if err := os.RemoveAll(c.Root); err != nil {
		return errors.Join(umountErr, fmt.Errorf("runsc: remove root: %w", err))
	}
	return nil
}

// run executes runsc with stderr captured in a temporary file. A file rather
// than a pipe, so a process runsc leaves behind cannot keep Wait blocked.
func (c *CLI) run(ctx context.Context, sub string, args []string, stdout io.Writer) error {
	errFile, err := os.CreateTemp("", "gss-runsc-stderr-")
	if err != nil {
		return fmt.Errorf("runsc %s: %w", sub, err)
	}
	defer os.Remove(errFile.Name())
	defer errFile.Close()
	cmd := exec.CommandContext(ctx, c.Binary, args...)
	cmd.Stdout = stdout
	cmd.Stderr = errFile
	if err := cmd.Run(); err != nil {
		return cmdError(sub, err, readTail(errFile.Name(), 0))
	}
	return nil
}

// cmdError wraps a failed runsc command with its subcommand and stderr tail.
func cmdError(sub string, err error, stderr string) error {
	if isNotFound(stderr) {
		return fmt.Errorf("runsc %s: %w: %w: %s", sub, ErrNotFound, err, stderr)
	}
	if stderr == "" {
		return fmt.Errorf("runsc %s: %w", sub, err)
	}
	return fmt.Errorf("runsc %s: %w: %s", sub, err, stderr)
}

// isNotFound reports whether runsc's stderr says the container does not exist
// ("FetchSpec failed: loading container: file does not exist").
func isNotFound(stderr string) bool {
	return strings.Contains(stderr, "loading container") && strings.Contains(stderr, "file does not exist")
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
