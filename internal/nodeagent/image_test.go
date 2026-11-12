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
	"github.com/opencontainers/runtime-spec/specs-go"
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
	parts := []string{v, r, p, sandboxLayout(p), strings.Join(runsc.SandboxFlags(), " ")}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
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
	if !slices.Contains(runsc.SandboxFlags(), "--overlay2=none") {
		t.Errorf("runsc.SandboxFlags() = %q, want the --overlay2=none the CLI passes", runsc.SandboxFlags())
	}
}

// TestSandboxLayout checks the part of the key that the bundle fixes: the
// process section and the mounts, which a restore takes from the image.
func TestSandboxLayout(t *testing.T) {
	base := bundle.Options{ID: "a_1_", Dir: "/s/a", RootFS: "/r", NodeID: "n1",
		CPUMillis: 500, MemoryBytes: 256 << 20, Prime: bundle.DefaultPrime}
	d := layoutDigest(bundle.Spec(base))
	if got := sandboxLayout(bundle.DefaultPrime); got != d {
		t.Errorf("sandboxLayout = %q, want %q (the digest of any bundle with that primed set)", got, d)
	}
	// What may differ between the checkpointed bundle and a restored one.
	restored := bundle.Options{ID: "b_2_", Dir: "/s/b", RootFS: "/r", NodeID: "n2",
		CPUMillis: 2000, MemoryBytes: 1 << 30, NetNSPath: "/var/run/netns/gss-b",
		Hosts: []bundle.HostEntry{{IP: "10.0.0.1", Name: "api.example"}}, Prime: bundle.DefaultPrime}
	if got := layoutDigest(bundle.Spec(restored)); got != d {
		t.Errorf("the digest changes with ID, directory, limits, network or hosts: %q, want %q", got, d)
	}
	if sandboxLayout(bundle.DefaultPrime+",sqlite3") == d {
		t.Error("the digest does not change with the primed set (GSS_PRIME)")
	}
	for name, change := range map[string]func(*specs.Spec){
		"user":              func(s *specs.Spec) { s.Process.User.UID = 0 },
		"args":              func(s *specs.Spec) { s.Process.Args = append(s.Process.Args, "-v") },
		"env":               func(s *specs.Spec) { s.Process.Env = append(s.Process.Env, "GSS_X=1") },
		"rlimits":           func(s *specs.Spec) { s.Process.Rlimits[0].Hard = 2048 },
		"no_new_privileges": func(s *specs.Spec) { s.Process.NoNewPrivileges = false },
		"capabilities":      func(s *specs.Spec) { s.Process.Capabilities.Bounding = []string{"CAP_NET_RAW"} },
		"mount type":        func(s *specs.Spec) { s.Mounts[3].Type = "bind" },
		"mount destination": func(s *specs.Spec) { s.Mounts[3].Destination = "/scratch" },
		"mount order":       func(s *specs.Spec) { s.Mounts[3], s.Mounts[4] = s.Mounts[4], s.Mounts[3] },
		"extra mount":       func(s *specs.Spec) { s.Mounts = append(s.Mounts, specs.Mount{Destination: "/x", Type: "tmpfs"}) },
	} {
		s := bundle.Spec(base)
		change(s)
		if layoutDigest(s) == d {
			t.Errorf("the digest does not change with the %s", name)
		}
	}
}

// syncRecorder is Manager.fsync for tests: it records each path, and marks a
// sync of the checkpoint directory made after the image was published.
type syncRecorder struct {
	mu     sync.Mutex
	ckdir  string
	image  string // the published image directory
	synced []string
}

func (r *syncRecorder) fsync(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if path == r.ckdir && exists(filepath.Join(r.image, ".complete")) {
		path += " (after the rename)"
	}
	r.synced = append(r.synced, path)
	return nil
}

func (r *syncRecorder) Synced() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.synced)
}

func TestEnsureImageBuildsOnce(t *testing.T) {
	sr := recordSpans(t)
	ck := t.TempDir()
	e, f := imageEnv(t, ck, nil)
	ctx := context.Background()
	key := testImageKey()
	rid := imageRunscID(key)
	syncs := &syncRecorder{ckdir: ck, image: filepath.Join(ck, key)}
	e.m.fsync = syncs.fsync

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
		// The image: a cold start, the readiness probe, the checkpoint.
		"Create id=" + rid + " net=none",
		"Start id=" + rid + " net=none",
		"Exec id=" + rid + " net=none",
		"Checkpoint id=" + rid + " net=none",
		"Delete id=" + rid + " net=none",
		// One restore from it before it is published.
		"Create id=" + rid + " net=none",
		"Restore id=" + rid + " net=none",
		"Exec id=" + rid + " net=none",
		"Delete id=" + rid + " net=none",
	}
	if got := e.rt.Calls(); !slices.Equal(got, want) {
		t.Errorf("runtime calls = %q, want %q", got, want)
	}
	if listedDuringBuild != 0 {
		t.Errorf("List during the build had %d entries, want 0", listedDuringBuild)
	}
	tmp := filepath.Join(ck, fmt.Sprintf("%s.tmp-%d", key, os.Getpid()))
	if got, want := f.Paths(), []string{"Checkpoint " + tmp, "Restore " + tmp}; !slices.Equal(got, want) {
		t.Errorf("image paths = %q, want %q", got, want)
	}
	for _, name := range []string{".complete", "checkpoint.img"} {
		if !exists(filepath.Join(dir, name)) {
			t.Errorf("no %s in %s", name, dir)
		}
	}
	// Every file of the image and its directory reach the disk before the
	// rename, and the checkpoint directory after it.
	synced := syncs.Synced()
	for _, p := range []string{filepath.Join(tmp, "checkpoint.img"), filepath.Join(tmp, ".complete"), tmp} {
		if !slices.Contains(synced, p) {
			t.Errorf("%s not synced: %q", p, synced)
		}
	}
	if len(synced) == 0 || synced[len(synced)-1] != ck+" (after the rename)" {
		t.Errorf("synced = %q, want the checkpoint directory last, after the rename", synced)
	}
	if got := tmpImageDirs(t, ck, key); len(got) != 0 {
		t.Errorf("build directories left: %q", got)
	}
	if !e.m.ImageReady() {
		t.Error("ImageReady = false after EnsureImage")
	}
	// Both image sandboxes are gone: container, bundle, cgroup.
	if got := e.rt.Containers(); len(got) != 0 {
		t.Errorf("containers = %v", got)
	}
	if exists(e.bundleDir(rid)) {
		t.Errorf("bundle %s left", e.bundleDir(rid))
	}
	if n := e.cg.count("Remove " + bundle.CgroupPath("n1", rid)); n != 2 {
		t.Errorf("image sandbox cgroup removed %d times, want 2: %q", n, e.cg.Calls())
	}
	spans := sr.Ended()
	for _, name := range []string{"runsc.create", "runsc.start", "sandbox.ready", "runsc.checkpoint", "runsc.restore", "runsc.delete"} {
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
	key := testImageKey()
	rid := imageRunscID(key)
	// The checkpoint fails, or the image does not restore.
	for _, method := range []string{"Checkpoint", "Restore"} {
		ck := t.TempDir()
		e, _ := imageEnv(t, ck, nil)
		e.rt.Fail[method] = errors.New(method + " failed")
		_, err := e.m.EnsureImage(context.Background())
		if err == nil || !strings.Contains(err.Error(), method+" failed") {
			t.Fatalf("%s failing: EnsureImage = %v, want that error", method, err)
		}
		if exists(filepath.Join(ck, key)) {
			t.Errorf("%s failing: image directory %s exists", method, filepath.Join(ck, key))
		}
		if got := tmpImageDirs(t, ck, key); len(got) != 0 {
			t.Errorf("%s failing: build directories left: %q", method, got)
		}
		if e.m.ImageReady() {
			t.Errorf("%s failing: ImageReady = true after a failed build", method)
		}
		if got := e.rt.Containers(); len(got) != 0 || exists(e.bundleDir(rid)) {
			t.Errorf("%s failing: image sandbox not cleaned up: containers %v", method, got)
		}
	}

	// Start logs a failed build and serves cold starts.
	ck := t.TempDir()
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

// An image is valid only for the rootfs that made it: without a build ID
// there is none (spec D36).
func TestImageNeedsRootfsID(t *testing.T) {
	for name, content := range map[string]*string{"missing": nil, "empty": new(string)} {
		ck := t.TempDir()
		root := t.TempDir()
		if content != nil {
			if err := os.WriteFile(filepath.Join(root, "python.id"), []byte(*content+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		mod := func(c *Config, _ *Deps) {
			c.CheckpointDir = ck
			c.RootFS = filepath.Join(root, "python")
		}
		e := newEnv(t, mod)
		if _, err := e.m.EnsureImage(context.Background()); err == nil || !strings.Contains(err.Error(), "build ID") {
			t.Errorf("%s build ID: EnsureImage = %v, want an error about the build ID", name, err)
		}
		if got := e.rt.Calls(); len(got) != 0 {
			t.Errorf("%s build ID: runtime calls = %q", name, got)
		}
		if ents, _ := os.ReadDir(ck); len(ents) != 0 {
			t.Errorf("%s build ID: checkpoint directory holds %v", name, ents)
		}
		s := startEnv(t, mod)
		if s.m.ImageReady() {
			t.Errorf("%s build ID: ImageReady after Start", name)
		}
		if resp := s.mustCreate(t, "sb-1", 1, nil); resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD {
			t.Errorf("%s build ID: started_as = %v, want COLD", name, resp.GetStartedAs())
		}
	}
}

func TestEnsureImageHonoursCtx(t *testing.T) {
	// Waiting for another builder's lock ends with ctx.
	ck := t.TempDir()
	key := testImageKey()
	unlock, err := lockFile(context.Background(), filepath.Join(ck, key+".lock"))
	if err != nil {
		t.Fatal(err)
	}
	e, _ := imageEnv(t, ck, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	begin := time.Now()
	_, err = e.m.EnsureImage(ctx)
	cancel()
	unlock()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("EnsureImage with the lock held elsewhere = %v, want the deadline", err)
	}
	if d := time.Since(begin); d > 2*time.Second {
		t.Errorf("EnsureImage returned %v after a 100ms deadline", d)
	}
	if got := e.rt.Calls(); !slices.Equal(got, []string{"Version"}) {
		t.Errorf("runtime calls = %q, want [Version]", got)
	}

	// A cancel during the build: Start returns soon, the image sandbox
	// already gone, and the node serves cold starts.
	s, _ := imageEnv(t, t.TempDir(), nil)
	s.rt.Delay["Checkpoint"] = 10 * time.Second
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go func() {
		defer cancel()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) &&
			!slices.ContainsFunc(s.rt.Calls(), func(c string) bool { return strings.HasPrefix(c, "Checkpoint ") }) {
			time.Sleep(5 * time.Millisecond)
		}
	}()
	begin = time.Now()
	if err := s.m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := s.m.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	if d := time.Since(begin); d > 5*time.Second {
		t.Errorf("Start took %v after a cancel during the image build", d)
	}
	if got := s.rt.Containers(); len(got) != 0 {
		t.Errorf("containers when Start returned = %v, want none", got)
	}
	if s.m.ImageReady() {
		t.Error("ImageReady after a cancelled build")
	}
	if got := tmpImageDirs(t, s.m.cfg.CheckpointDir, key); len(got) != 0 {
		t.Errorf("build directories left: %q", got)
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

// A drop of the image while EnsureImage is building (a concurrent Create
// whose restore failed) is not undone when the build finishes.
func TestEnsureImageKeepsConcurrentDrop(t *testing.T) {
	e, _ := imageEnv(t, t.TempDir(), nil)
	var once sync.Once
	e.m.fsync = func(path string) error {
		once.Do(func() { e.m.dropImage("runsc.restore", errors.New("restore failed")) })
		return nil
	}
	if dir, err := e.m.EnsureImage(context.Background()); err == nil {
		t.Errorf("EnsureImage = %q, nil; want an error after the image was dropped", dir)
	}
	if e.m.ImageReady() {
		t.Error("ImageReady after a drop during the build")
	}
}
