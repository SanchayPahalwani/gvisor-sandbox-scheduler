package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bundle"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
	"github.com/opencontainers/runtime-spec/specs-go"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sys/unix"
)

// The checkpoint image (spec 7.2, call D36) of a node lives in
// CheckpointDir/<key>, which every node agent on the host shares: an image
// is built by one agent and restored by all of them. An image is published
// when it holds completeFile. A builder takes an exclusive flock on
// CheckpointDir/<key>.lock, checkpoints into CheckpointDir/<key>.tmp-<pid>,
// restores one sandbox from it, writes completeFile, fsyncs the directory
// and renames it to <key>, so no agent ever sees a partial image, or one
// that was never restored, under <key>.
const (
	completeFile = ".complete"
	// lockPoll is how often a wait for another builder's lock tries again.
	lockPoll = 20 * time.Millisecond
	// defaultRestoreReadyTimeout bounds the readiness probe of a restored
	// sandbox (about 85 ms in the VM); a cold start keeps Config.ReadyTimeout.
	defaultRestoreReadyTimeout = 5 * time.Second
	// imageRecheck is how often a node without an image looks for one that
	// another agent on the host has built since.
	imageRecheck = time.Second
	// readyFailuresToDrop is how many restored sandboxes in a row must fail
	// their readiness probe before the node drops the image. One slow probe
	// on a loaded node (the density benchmark loads nodes until starts slow
	// down) is not a bad image.
	readyFailuresToDrop = 3
)

// ImageKey identifies what a checkpoint image is valid for: the runsc
// release, the rootfs build, the primed module list, the sandbox layout the
// bundle gives that list (on restore the process's credentials, rlimits and
// environment come from the image, not from the new bundle), and the global
// flags runsc runs with. It is the first 16 hex characters of the sha256 of
// those five, joined by "\n".
func ImageKey(runscVersion, rootfsID, prime string) string {
	parts := []string{runscVersion, rootfsID, prime, sandboxLayout(prime), strings.Join(runsc.SandboxFlags(), " ")}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(sum[:])[:16]
}

// sandboxLayout is layoutDigest of the bundle every sandbox with this primed
// set gets. The fields a restored bundle may change (ID, directory, limits,
// network namespace, hosts) take placeholder values: bundle.Spec does not
// put them in the parts the digest covers.
func sandboxLayout(prime string) string {
	return layoutDigest(bundle.Spec(bundle.Options{
		ID: "layout", Dir: "/layout", RootFS: "/layout", NodeID: "layout",
		CPUMillis: 1000, MemoryBytes: 1 << 30, Prime: prime,
	}))
}

// layoutDigest is the hex sha256 of the JSON of s's process section, then a
// line per mount, in order, with its destination and type.
func layoutDigest(s *specs.Spec) string {
	h := sha256.New()
	// The process section is plain strings, numbers and slices: it always
	// marshals.
	process, _ := json.Marshal(s.Process)
	h.Write(process)
	for _, mnt := range s.Mounts {
		fmt.Fprintf(h, "\n%s %s", mnt.Destination, mnt.Type)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// EnsureImage returns the directory of the node's checkpoint image, building
// it first if no agent has. Concurrent calls, in this process or in other
// agents sharing CheckpointDir, build it once: the others wait for the lock
// (bounded by ctx) and then find it published. A rootfs without a build ID
// gets no image. A failed build leaves no image and no build directory.
// The sandboxes that build the image are not in the manager's table, so
// they are neither listed nor counted, and EnsureImage has removed them when
// it returns, also when ctx ended. Start calls it; once it has succeeded,
// Creates can take the restore path. After this manager dropped the image
// (restoreFailed) it refuses.
func (m *Manager) EnsureImage(ctx context.Context) (dir string, err error) {
	if m.cfg.CheckpointDir == "" {
		return "", errors.New("nodeagent: checkpoint images are off (no CheckpointDir)")
	}
	m.mu.Lock()
	d, bad, closed := m.imageDir, m.imageBad, m.closed
	m.mu.Unlock()
	switch {
	case bad:
		return "", errors.New("nodeagent: a restore from the checkpoint image failed; restore is off until the agent restarts")
	case closed:
		return "", errors.New("nodeagent: EnsureImage after Close")
	}
	if d != "" {
		ok, err := imageComplete(d)
		if err != nil {
			return "", fmt.Errorf("nodeagent: checkpoint image: %w", err)
		}
		if ok {
			return d, nil
		}
		m.setImage("") // it was removed: Creates start cold until it is back
	}

	ctx, span := startSpan(ctx, "node.image")
	defer func() { endSpan(span, err) }()
	rootfs, err := rootfsID(m.cfg.RootFS)
	if err != nil {
		return "", fmt.Errorf("nodeagent: checkpoint images are off: %w", err)
	}
	version, err := m.rt.Version(ctx)
	if err != nil {
		return "", fmt.Errorf("nodeagent: checkpoint image: runsc version: %w", err)
	}
	key := ImageKey(version, rootfs, m.cfg.Prime)
	span.SetAttributes(attribute.String("image.key", key))
	m.mu.Lock()
	m.imageKey = key
	m.mu.Unlock()
	dir = filepath.Join(m.cfg.CheckpointDir, key)
	ok, err := imageComplete(dir)
	if err != nil {
		return "", fmt.Errorf("nodeagent: checkpoint image: %w", err)
	}
	if !ok {
		if err := os.MkdirAll(m.cfg.CheckpointDir, 0o700); err != nil {
			return "", fmt.Errorf("nodeagent: checkpoint image: %w", err)
		}
		unlock, err := lockFile(ctx, filepath.Join(m.cfg.CheckpointDir, key+".lock"))
		if err != nil {
			return "", fmt.Errorf("nodeagent: checkpoint image %s: lock: %w", key, err)
		}
		defer unlock()
		// Another agent may have built it while this one waited.
		if ok, err = imageComplete(dir); err != nil {
			return "", fmt.Errorf("nodeagent: checkpoint image: %w", err)
		}
		if !ok {
			span.SetAttributes(attribute.Bool("image.built", true))
			info := fmt.Sprintf("runsc %s\nrootfs %s\nprime %s\n", version, rootfs, m.cfg.Prime)
			if err := m.buildImage(ctx, key, dir, info); err != nil {
				return "", fmt.Errorf("nodeagent: build checkpoint image %s: %w", key, err)
			}
		}
	}
	m.setImage(dir)
	return dir, nil
}

// ImageReady reports whether Creates can restore from a checkpoint image.
func (m *Manager) ImageReady() bool { return m.image() != "" }

// image is the directory of the ready checkpoint image, or "".
func (m *Manager) image() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.imageDir
}

func (m *Manager) setImage(dir string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.imageDir = dir
}

// restoreImage is the image a Create restores from, or "". A node that has
// none, because its own build failed or its wait for another agent's build
// ended, looks at most once per imageRecheck for the image of its key that
// another agent has published since. After a failed restore it never looks.
func (m *Manager) restoreImage() string {
	m.mu.Lock()
	dir, key := m.imageDir, m.imageKey
	now := m.now()
	look := dir == "" && key != "" && !m.imageBad && now.Sub(m.imageLooked) >= imageRecheck
	if look {
		m.imageLooked = now
	}
	m.mu.Unlock()
	if !look {
		return dir
	}
	found := filepath.Join(m.cfg.CheckpointDir, key)
	if ok, err := imageComplete(found); err != nil || !ok {
		return ""
	}
	m.mu.Lock()
	adopted := m.imageDir == "" && !m.imageBad
	if adopted {
		m.imageDir = found
	}
	dir = m.imageDir
	m.mu.Unlock()
	if adopted {
		slog.Info("checkpoint image found", "node", m.cfg.NodeID, "dir", found)
	}
	return dir
}

// restoreFailed counts a restore from the image that failed in step (a
// Create whose ctx ended does not count). A failed runsc restore drops the
// image at once. A restored sandbox that did not get ready drops it only as
// the readyFailuresToDrop-th such failure in a row; restoreSucceeded resets
// the count.
func (m *Manager) restoreFailed(step string, err error) {
	if step == "sandbox.ready" {
		m.mu.Lock()
		m.readyFailures++
		inARow := m.readyFailures
		m.mu.Unlock()
		if inARow < readyFailuresToDrop {
			return
		}
	}
	m.dropImage(step, err)
}

// restoreSucceeded records a restored sandbox that got ready.
func (m *Manager) restoreSucceeded() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readyFailures = 0
}

// dropImage turns restore off for good on this manager, so that it stops
// paying for a bad image (step and err say what failed last); a new agent
// builds or checks it again.
func (m *Manager) dropImage(step string, err error) {
	m.mu.Lock()
	first := !m.imageBad
	dir := m.imageDir
	m.imageBad, m.imageDir = true, ""
	m.mu.Unlock()
	if first {
		slog.Warn("restore from the checkpoint image failed; this node starts sandboxes cold until it restarts",
			"node", m.cfg.NodeID, "dir", dir, "step", step, "err", err)
	}
}

// buildImage makes the image under the key's lock, which the caller holds:
// it cold-starts the image sandbox and checkpoints it into a build
// directory, restores one sandbox from that directory to see that it works,
// then marks it complete, fsyncs it and renames it to dir. Either dir is a
// published image afterwards or the build directory is gone.
func (m *Manager) buildImage(ctx context.Context, key, dir, info string) (err error) {
	if err := removeStaleBuilds(m.cfg.CheckpointDir, key); err != nil {
		return err
	}
	tmp := filepath.Join(m.cfg.CheckpointDir, fmt.Sprintf("%s.tmp-%d", key, os.Getpid()))
	if err := os.Mkdir(tmp, 0o700); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, os.RemoveAll(tmp))
		}
	}()

	err = m.withImageSandbox(ctx, key, "", func(e *sandbox) (string, error) {
		return "runsc.checkpoint", m.rt.Checkpoint(ctx, e.ref, tmp)
	})
	if err != nil {
		return err
	}
	if err := m.withImageSandbox(ctx, key, tmp, nil); err != nil {
		return fmt.Errorf("test restore: %w", err)
	}
	if err := os.WriteFile(filepath.Join(tmp, completeFile), []byte(info), 0o600); err != nil {
		return err
	}
	if err := m.syncTree(tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, dir); err != nil {
		return err
	}
	// The rename is durable only once the directory holding it is synced. A
	// failure here leaves a published image, so it is not undone.
	if err := m.fsync(m.cfg.CheckpointDir); err != nil {
		slog.Warn("checkpoint image published, but fsync of the checkpoint directory failed",
			"path", m.cfg.CheckpointDir, "image", dir, "err", err)
	}
	return nil
}

// withImageSandbox starts an image sandbox (cold, or restored from imageDir),
// runs then on it once it is ready (if then is not nil), and removes it. The
// removal runs whatever happened before, with its own bounded ctx, and is
// done when withImageSandbox returns.
func (m *Manager) withImageSandbox(ctx context.Context, key, imageDir string, then func(*sandbox) (string, error)) error {
	e := m.imageSandbox(key)
	step, err := m.startSandbox(ctx, e, imageDir)
	if err == nil && then != nil {
		step, err = then(e)
	}
	if err != nil {
		err = fmt.Errorf("%s: %w", step, err)
	}
	if terr := m.teardown(ctx, e); terr != nil {
		err = errors.Join(err, fmt.Errorf("remove image sandbox %s: %w", e.ref.ID, terr))
	}
	return err
}

// imageSandbox is a sandbox of the image build: no network, the default
// size, and a runsc ID ("img_<key>_<pid>_") that no user sandbox can have
// (sandbox IDs contain no '_', and "img" is reserved). The build's two
// sandboxes use it one after the other. It is not in the manager's table.
func (m *Manager) imageSandbox(key string) *sandbox {
	rid := fmt.Sprintf("img_%s_%d_", key, os.Getpid())
	spec := &gssv1.SandboxSpec{}
	gssv1.ApplyDefaults(spec)
	return &sandbox{
		id: rid, spec: spec,
		ref:       runsc.Ref{ID: rid, Network: runsc.NetworkNone},
		bundleDir: filepath.Join(m.bundlesDir(), rid),
		cgroup:    bundle.CgroupPath(m.cfg.NodeID, rid),
	}
}

// syncTree fsyncs every regular file and directory below dir, then dir.
func (m *Manager) syncTree(dir string) error {
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == dir || !(d.Type().IsRegular() || d.IsDir()) {
			return nil
		}
		return m.fsync(path)
	})
	if err != nil {
		return err
	}
	return m.fsync(dir)
}

// fsyncPath is Manager.fsync: it opens path (a file or a directory) and
// fsyncs it.
func fsyncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("fsync %s: %w", path, err)
	}
	return nil
}

// removeStaleBuilds removes what a builder of key that died left in ckdir:
// build directories, and an image directory without completeFile. The
// caller holds the key's lock, so no one else is building it.
func removeStaleBuilds(ckdir, key string) error {
	ents, err := os.ReadDir(ckdir)
	if err != nil {
		return err
	}
	var errs []error
	for _, ent := range ents {
		if name := ent.Name(); name == key || strings.HasPrefix(name, key+".tmp-") {
			errs = append(errs, os.RemoveAll(filepath.Join(ckdir, name)))
		}
	}
	return errors.Join(errs...)
}

// imageComplete reports whether dir holds a published image. Only a missing
// marker means no; any other error is returned.
func imageComplete(dir string) (bool, error) {
	_, err := os.Stat(filepath.Join(dir, completeFile))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	}
	return false, err
}

// rootfsID is the build ID of the rootfs at path: the content of the sibling
// file <path>.id that scripts/build-rootfs.sh writes. A rootfs without one
// (no file, or an empty one) gets no image: an image is valid only for the
// filesystem that made it.
func rootfsID(path string) (string, error) {
	idFile := filepath.Clean(path) + ".id"
	b, err := os.ReadFile(idFile)
	if errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("the rootfs has no build ID (%s is missing)", idFile)
	}
	if err != nil {
		return "", fmt.Errorf("rootfs build ID: %w", err)
	}
	id := strings.TrimSpace(string(b))
	if id == "" {
		return "", fmt.Errorf("the rootfs has no build ID (%s is empty)", idFile)
	}
	return id, nil
}

// lockFile takes an exclusive flock on path, creating the file if needed.
// It polls, so that ctx can end the wait. Closing the file (unlock) releases
// the lock, and so does the end of the process.
func lockFile(ctx context.Context, path string) (unlock func(), err error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	for {
		err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return func() { f.Close() }, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EINTR) {
			f.Close()
			return nil, fmt.Errorf("flock %s: %w", path, err)
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-time.After(lockPoll):
		}
	}
}
