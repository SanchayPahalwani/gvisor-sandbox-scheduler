package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/bundle"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
)

// imageFake is the fake runtime plus what runsc does to an image directory:
// a Checkpoint writes checkpoint.img into it. It records the image path of
// every Checkpoint and Restore, as "Checkpoint <path>" and "Restore <path>".
type imageFake struct {
	*runsc.Fake
	mu    sync.Mutex
	paths []string
}

func (f *imageFake) record(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paths = append(f.paths, s)
}

func (f *imageFake) Paths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.paths)
}

func (f *imageFake) Checkpoint(ctx context.Context, ref runsc.Ref, imagePath string) error {
	f.record("Checkpoint " + imagePath)
	if err := f.Fake.Checkpoint(ctx, ref, imagePath); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(imagePath, "checkpoint.img"), []byte("image"), 0o644)
}

func (f *imageFake) Restore(ctx context.Context, ref runsc.Ref, bundleDir, imagePath string) error {
	f.record("Restore " + imagePath)
	return f.Fake.Restore(ctx, ref, bundleDir, imagePath)
}

// testRootfsID is the build ID of the rootfs of an imageEnv.
const testRootfsID = "rootfs-1"

// imageEnv returns an env, not yet started, whose checkpoint images live in
// ckdir and whose rootfs has the build ID testRootfsID. Its runtime is the
// env's fake wrapped in an imageFake.
func imageEnv(t *testing.T, ckdir string, mod func(*Config, *Deps)) (*env, *imageFake) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "python.id"), []byte(testRootfsID+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var f *imageFake
	e := newEnv(t, func(c *Config, d *Deps) {
		c.CheckpointDir = ckdir
		c.RootFS = filepath.Join(root, "python")
		f = &imageFake{Fake: d.Runtime.(*runsc.Fake)}
		d.Runtime = f
		if mod != nil {
			mod(c, d)
		}
	})
	return e, f
}

// testImageKey is the key of an imageEnv's image.
func testImageKey() string { return ImageKey("release-fake", testRootfsID, bundle.DefaultPrime) }

// imageRunscID is the runsc ID of the sandbox that builds the image key.
func imageRunscID(key string) string { return fmt.Sprintf("img_%s_%d_", key, os.Getpid()) }

// tmpImageDirs returns the names of the build directories of key left in ckdir.
func tmpImageDirs(t *testing.T, ckdir, key string) []string {
	t.Helper()
	ents, err := os.ReadDir(ckdir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, ent := range ents {
		if strings.HasPrefix(ent.Name(), key+".tmp") {
			out = append(out, ent.Name())
		}
	}
	return out
}

func TestImageKey(t *testing.T) {
	const v, r, p = "release-20261102.0", "3f2a9c", bundle.DefaultPrime
	k := ImageKey(v, r, p)
	sum := sha256.Sum256([]byte(v + "\n" + r + "\n" + p))
	if want := hex.EncodeToString(sum[:])[:16]; k != want {
		t.Errorf("ImageKey = %q, want %q", k, want)
	}
	if len(k) != 16 {
		t.Errorf("ImageKey %q has %d characters, want 16", k, len(k))
	}
	if _, err := hex.DecodeString(k); err != nil {
		t.Errorf("ImageKey %q is not hex: %v", k, err)
	}
	if again := ImageKey(v, r, p); again != k {
		t.Errorf("ImageKey is not stable: %q then %q", k, again)
	}
	for name, other := range map[string]string{
		"runsc version": ImageKey("release-20261103.0", r, p),
		"rootfs ID":     ImageKey(v, "3f2a9d", p),
		"primed set":    ImageKey(v, r, p+",sqlite3"),
	} {
		if other == k {
			t.Errorf("ImageKey does not change with the %s", name)
		}
	}
}

func TestEnsureImageBuildsOnce(t *testing.T) {
	sr := recordSpans(t)
	ck := t.TempDir()
	e, f := imageEnv(t, ck, nil)
	ctx := context.Background()
	key := testImageKey()
	rid := imageRunscID(key)

	// The image sandbox is not in the manager's table: not listed, not counted.
	listedDuringBuild := -1
	e.rt.ExecFunc = func(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
		if ref.ID == rid {
			listedDuringBuild = len(e.m.List())
		}
		return 0, nil
	}
	dir, err := e.m.EnsureImage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(ck, key); dir != want {
		t.Errorf("EnsureImage = %q, want %q", dir, want)
	}
	want := []string{
		"Version",
		"Create id=" + rid + " net=none",
		"Start id=" + rid + " net=none",
		"Exec id=" + rid + " net=none", // the readiness probe
		"Checkpoint id=" + rid + " net=none",
		"Delete id=" + rid + " net=none",
	}
	if got := e.rt.Calls(); !slices.Equal(got, want) {
		t.Errorf("runtime calls = %q, want %q", got, want)
	}
	if listedDuringBuild != 0 {
		t.Errorf("List during the build had %d entries, want 0", listedDuringBuild)
	}
	tmp := filepath.Join(ck, fmt.Sprintf("%s.tmp-%d", key, os.Getpid()))
	if got := f.Paths(); !slices.Equal(got, []string{"Checkpoint " + tmp}) {
		t.Errorf("image paths = %q, want the checkpoint into %s", got, tmp)
	}
	for _, name := range []string{".complete", "checkpoint.img"} {
		if !exists(filepath.Join(dir, name)) {
			t.Errorf("no %s in %s", name, dir)
		}
	}
	if got := tmpImageDirs(t, ck, key); len(got) != 0 {
		t.Errorf("build directories left: %q", got)
	}
	if !e.m.ImageReady() {
		t.Error("ImageReady = false after EnsureImage")
	}
	// The image sandbox is gone: container, bundle, cgroup.
	if got := e.rt.Containers(); len(got) != 0 {
		t.Errorf("containers = %v", got)
	}
	if exists(e.bundleDir(rid)) {
		t.Errorf("bundle %s left", e.bundleDir(rid))
	}
	if e.cg.count("Remove "+bundle.CgroupPath("n1", rid)) != 1 {
		t.Errorf("cgroup calls = %q", e.cg.Calls())
	}
	spans := sr.Ended()
	for _, name := range []string{"runsc.create", "runsc.start", "sandbox.ready", "runsc.checkpoint", "runsc.delete"} {
		checkParent(t, spans, name, "node.image")
	}

	// A second call finds the image ready and calls nothing.
	before := len(e.rt.Calls())
	again, err := e.m.EnsureImage(ctx)
	if err != nil || again != dir {
		t.Errorf("second EnsureImage = %q, %v; want %q", again, err, dir)
	}
	if got := e.rt.Calls()[before:]; len(got) != 0 {
		t.Errorf("second EnsureImage made runtime calls %q", got)
	}

	// Another agent with the same CheckpointDir finds it too: it only asks
	// runsc its version, for the key.
	other, _ := imageEnv(t, ck, nil)
	if got, err := other.m.EnsureImage(ctx); err != nil || got != dir {
		t.Errorf("other agent's EnsureImage = %q, %v; want %q", got, err, dir)
	}
	if got := other.rt.Calls(); !slices.Equal(got, []string{"Version"}) {
		t.Errorf("other agent's runtime calls = %q, want [Version]", got)
	}
	if !other.m.ImageReady() {
		t.Error("other agent: ImageReady = false")
	}
}

func TestEnsureImageConcurrent(t *testing.T) {
	ck := t.TempDir()
	a, _ := imageEnv(t, ck, nil)
	b, _ := imageEnv(t, ck, nil)
	for _, e := range []*env{a, b} {
		e.rt.Delay["Checkpoint"] = 100 * time.Millisecond // the two calls overlap
	}
	var wg sync.WaitGroup
	dirs, errs := make([]string, 2), make([]error, 2)
	for i, e := range []*env{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dirs[i], errs[i] = e.m.EnsureImage(context.Background())
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("agent %d: %v", i, err)
		}
	}
	if want := filepath.Join(ck, testImageKey()); dirs[0] != want || dirs[1] != want {
		t.Errorf("dirs = %q, want %q twice", dirs, want)
	}
	checkpoints := 0
	for _, e := range []*env{a, b} {
		for _, call := range e.rt.Calls() {
			if strings.HasPrefix(call, "Checkpoint ") {
				checkpoints++
			}
		}
		if !e.m.ImageReady() {
			t.Error("ImageReady = false")
		}
	}
	if checkpoints != 1 {
		t.Errorf("%d Checkpoint calls in total, want 1", checkpoints)
	}
}

func TestEnsureImageFailureLeavesNoImage(t *testing.T) {
	ck := t.TempDir()
	key := testImageKey()
	rid := imageRunscID(key)
	e, _ := imageEnv(t, ck, nil)
	e.rt.Fail["Checkpoint"] = errors.New("checkpoint failed")
	_, err := e.m.EnsureImage(context.Background())
	if err == nil || !strings.Contains(err.Error(), "checkpoint failed") {
		t.Fatalf("EnsureImage = %v, want the checkpoint error", err)
	}
	if exists(filepath.Join(ck, key)) {
		t.Errorf("image directory %s exists", filepath.Join(ck, key))
	}
	if got := tmpImageDirs(t, ck, key); len(got) != 0 {
		t.Errorf("build directories left: %q", got)
	}
	if e.m.ImageReady() {
		t.Error("ImageReady = true after a failed build")
	}
	if got := e.rt.Containers(); len(got) != 0 || exists(e.bundleDir(rid)) || e.cg.count("Remove "+bundle.CgroupPath("n1", rid)) != 1 {
		t.Errorf("image sandbox not cleaned up: containers %v, cgroup calls %q", got, e.cg.Calls())
	}

	// Start logs a failed build and serves cold starts.
	s, _ := imageEnv(t, ck, nil)
	s.rt.Fail["Checkpoint"] = errors.New("checkpoint failed")
	s.start(t)
	if s.m.ImageReady() {
		t.Error("ImageReady = true after Start's build failed")
	}
	if resp := s.mustCreate(t, "sb-1", 1, nil); resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
		t.Errorf("started_as = %v, want COLD", resp.GetStartedAs())
	}
	if exists(filepath.Join(ck, key)) {
		t.Errorf("image directory %s exists", filepath.Join(ck, key))
	}
}

func TestEnsureImageRemovesStaleBuilds(t *testing.T) {
	ck := t.TempDir()
	key := testImageKey()
	// What a builder that died leaves: its build directory, and an image
	// directory without .complete.
	for _, d := range []string{key + ".tmp-1", key} {
		if err := os.MkdirAll(filepath.Join(ck, d), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(ck, d, "checkpoint.img"), []byte("partial"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	e, _ := imageEnv(t, ck, nil)
	dir, err := e.m.EnsureImage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(dir, "checkpoint.img")); err != nil || string(b) != "image" {
		t.Errorf("checkpoint.img = %q, %v; want the new image", b, err)
	}
	if got := tmpImageDirs(t, ck, key); len(got) != 0 {
		t.Errorf("build directories left: %q", got)
	}
}

func TestRootfsIDMissing(t *testing.T) {
	ck := t.TempDir()
	e := newEnv(t, func(c *Config, _ *Deps) {
		c.CheckpointDir = ck
		c.RootFS = filepath.Join(t.TempDir(), "python") // no python.id next to it
	})
	dir, err := e.m.EnsureImage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(ck, ImageKey("release-fake", "unknown", bundle.DefaultPrime)); dir != want {
		t.Errorf("EnsureImage = %q, want %q (rootfs ID \"unknown\")", dir, want)
	}
}

func TestEnsureImageDisabled(t *testing.T) {
	e := newEnv(t, nil)
	if _, err := e.m.EnsureImage(context.Background()); err == nil {
		t.Error("EnsureImage without a CheckpointDir succeeded")
	}
	if e.m.ImageReady() {
		t.Error("ImageReady = true without a CheckpointDir")
	}
	if got := e.rt.Calls(); len(got) != 0 {
		t.Errorf("runtime calls = %q", got)
	}
}

func TestEnsureImageRemovedImage(t *testing.T) {
	ck := t.TempDir()
	e, _ := imageEnv(t, ck, nil)
	ctx := context.Background()
	dir, err := e.m.EnsureImage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The image goes away and the rebuild fails: the node no longer
	// restores.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	e.rt.Fail["Checkpoint"] = errors.New("checkpoint failed")
	if _, err := e.m.EnsureImage(ctx); err == nil {
		t.Fatal("EnsureImage succeeded with a failing checkpoint")
	}
	if e.m.ImageReady() {
		t.Error("ImageReady = true with the image gone")
	}
	// The next call builds it again.
	if got, err := e.m.EnsureImage(ctx); err != nil || got != dir || !e.m.ImageReady() {
		t.Errorf("EnsureImage = %q, %v (ready %v); want %q", got, err, e.m.ImageReady(), dir)
	}
	if !exists(filepath.Join(dir, ".complete")) {
		t.Errorf("no .complete in %s", dir)
	}
}
