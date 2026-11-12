package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bundle"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sys/unix"
)

// The checkpoint image (spec 7.2) of a node lives in CheckpointDir/<key>,
// which every node agent on the host shares: an image is built by one agent
// and restored by all of them. An image is complete when it holds
// completeFile. A builder takes an exclusive flock on CheckpointDir/<key>.lock,
// checkpoints into CheckpointDir/<key>.tmp-<pid>, writes completeFile there
// and renames the directory to <key>, so no agent ever sees a partial image
// under <key>.
const (
	completeFile = ".complete"
	// lockPoll is how often a wait for another builder's lock tries again.
	lockPoll = 20 * time.Millisecond
	// unknownRootfsID stands for a rootfs without a build ID file.
	unknownRootfsID = "unknown"
)

// ImageKey identifies what a checkpoint image is valid for: the runsc
// release, the rootfs build and the primed module list. It is the first 16
// hex characters of sha256(runscVersion + "\n" + rootfsID + "\n" + prime).
func ImageKey(runscVersion, rootfsID, prime string) string {
	sum := sha256.Sum256([]byte(runscVersion + "\n" + rootfsID + "\n" + prime))
	return hex.EncodeToString(sum[:])[:16]
}

// EnsureImage returns the directory of the node's checkpoint image, building
// it first if no agent has. Concurrent calls, in this process or in other
// agents sharing CheckpointDir, build it once: the others wait for the lock
// (bounded by ctx) and then find it complete. A failed build leaves no image
// and no build directory. The sandbox that builds the image is not in the
// manager's table, so it is neither listed nor counted, and EnsureImage
// removes it before it returns; Close does not wait for EnsureImage. Start
// calls it; once it has succeeded, Creates can take the restore path.
func (m *Manager) EnsureImage(ctx context.Context) (dir string, err error) {
	if m.cfg.CheckpointDir == "" {
		return "", errors.New("nodeagent: checkpoint images are off (no CheckpointDir)")
	}
	if d := m.image(); d != "" {
		if imageComplete(d) {
			return d, nil
		}
		m.setImage("") // it was removed: Creates start cold until it is back
	}
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return "", errors.New("nodeagent: EnsureImage after Close")
	}

	ctx, span := startSpan(ctx, "node.image")
	defer func() { endSpan(span, err) }()
	version, err := m.rt.Version(ctx)
	if err != nil {
		return "", fmt.Errorf("nodeagent: checkpoint image: runsc version: %w", err)
	}
	rootfs, err := rootfsID(m.cfg.RootFS)
	if err != nil {
		return "", fmt.Errorf("nodeagent: checkpoint image: %w", err)
	}
	key := ImageKey(version, rootfs, m.cfg.Prime)
	span.SetAttributes(attribute.String("image.key", key))
	dir = filepath.Join(m.cfg.CheckpointDir, key)
	if !imageComplete(dir) {
		if err := os.MkdirAll(m.cfg.CheckpointDir, 0o700); err != nil {
			return "", fmt.Errorf("nodeagent: checkpoint image: %w", err)
		}
		unlock, err := lockFile(ctx, filepath.Join(m.cfg.CheckpointDir, key+".lock"))
		if err != nil {
			return "", fmt.Errorf("nodeagent: checkpoint image %s: lock: %w", key, err)
		}
		defer unlock()
		// Another agent may have built it while this one waited.
		if !imageComplete(dir) {
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

// buildImage cold-starts the image sandbox, checkpoints it into a build
// directory, removes the sandbox, and publishes the directory as dir. The
// caller holds the key's lock. Either dir is a complete image afterwards or
// the build directory is gone.
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

	e := m.imageSandbox(key)
	step, err := m.startSandbox(ctx, e, "")
	if err == nil {
		step, err = "runsc.checkpoint", m.rt.Checkpoint(ctx, e.ref, tmp)
	}
	if err != nil {
		err = fmt.Errorf("%s: %w", step, err)
	}
	// runsc checkpoint leaves the container stopped; it goes either way.
	if terr := m.teardown(ctx, e); terr != nil {
		err = errors.Join(err, fmt.Errorf("remove image sandbox %s: %w", e.ref.ID, terr))
	}
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(tmp, completeFile), []byte(info), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, dir)
}

// imageSandbox is the sandbox that an image is checkpointed from: no
// network, the default size, and a runsc ID ("img_<key>_<pid>_") that no
// user sandbox can have (sandbox IDs contain no '_', and "img" is reserved).
// It is not in the manager's table.
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

func imageComplete(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, completeFile))
	return err == nil
}

// rootfsID is the build ID of the rootfs at path: the content of the sibling
// file <path>.id that scripts/build-rootfs.sh writes, or "unknown" if there
// is none.
func rootfsID(path string) (string, error) {
	b, err := os.ReadFile(filepath.Clean(path) + ".id")
	if errors.Is(err, fs.ErrNotExist) {
		return unknownRootfsID, nil
	}
	if err != nil {
		return "", fmt.Errorf("rootfs build ID: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
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
