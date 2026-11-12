package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// startZygote runs images/python/zygote.py with GSS_PRIME=json on a socket in a
// fresh temp dir and waits for its READY line. It returns the socket path.
func startZygote(t *testing.T) string {
	t.Helper()
	return startZygotePrimed(t, "json")
}

// startZygotePrimed is startZygote with the given GSS_PRIME list.
func startZygotePrimed(t *testing.T, prime string) string {
	t.Helper()
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not on PATH")
	}
	_, file, _, _ := runtime.Caller(0)
	script := filepath.Join(filepath.Dir(file), "..", "..", "images", "python", "zygote.py")
	// Not t.TempDir(): its path includes the test name, and a long name pushes the
	// socket path past macOS's 104-byte sun_path limit. Same parent directory.
	dir, err := os.MkdirTemp("", "gssz")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "z.sock")

	cmd := exec.Command(py, script)
	cmd.Env = append(os.Environ(), "GSS_ZYGOTE_SOCK="+sock, "GSS_PRIME="+prime)
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	ready := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(out).ReadString('\n')
		ready <- line
		_, _ = io.Copy(io.Discard, out)
	}()
	select {
	case line := <-ready:
		if !strings.HasPrefix(line, "READY prime_ms=") {
			t.Fatalf("zygote first line = %q, want READY prime_ms=<n>", line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("zygote did not print READY within 10s")
	}
	return sock
}

type result struct {
	code   int
	stdout string
	stderr string
}

func input(code string, timeoutS float64, env map[string]string) string {
	h, err := json.Marshal(map[string]any{"code_len": len(code), "timeout_s": timeoutS, "env": env})
	if err != nil {
		panic(err)
	}
	return string(h) + "\n" + code
}

// runWith feeds input to run on fresh pipes and collects the outputs.
func runWith(t *testing.T, sock string, input string, connectTimeout time.Duration) result {
	t.Helper()
	r, err := runPipes(sock, input, connectTimeout)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// runPipes is runWith without a *testing.T, so goroutines other than the test's can use it.
func runPipes(sock string, input string, connectTimeout time.Duration) (result, error) {
	var ends []*os.File
	for range 3 {
		r, w, err := os.Pipe()
		if err != nil {
			for _, f := range ends {
				_ = f.Close()
			}
			return result{}, err
		}
		ends = append(ends, r, w)
	}
	inR, inW, outR, outW, errR, errW := ends[0], ends[1], ends[2], ends[3], ends[4], ends[5]

	go func() {
		_, _ = io.WriteString(inW, input)
		_ = inW.Close()
	}()
	var stdout, stderr strings.Builder
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = io.Copy(&stdout, outR) }()
	go func() { defer wg.Done(); _, _ = io.Copy(&stderr, errR) }()

	code := run(inR, outW, errW, sock, connectTimeout)
	_ = inR.Close()
	_ = outW.Close()
	_ = errW.Close()
	wg.Wait()
	_ = outR.Close()
	_ = errR.Close()
	return result{code: code, stdout: stdout.String(), stderr: stderr.String()}, nil
}

func mustPipe(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	return r, w
}

func runCode(t *testing.T, code string) result {
	t.Helper()
	sock := startZygote(t)
	return runWith(t, sock, input(code, 0, nil), 5*time.Second)
}

func TestHello(t *testing.T) {
	r := runCode(t, `print("hello")`)
	if r.code != 0 || r.stdout != "hello\n" {
		t.Fatalf("got code %d stdout %q stderr %q; want 0, %q", r.code, r.stdout, r.stderr, "hello\n")
	}
}

func TestExitCode(t *testing.T) {
	r := runCode(t, "import sys; sys.exit(7)")
	if r.code != 7 {
		t.Fatalf("got code %d stderr %q; want 7", r.code, r.stderr)
	}
}

func TestException(t *testing.T) {
	r := runCode(t, `raise ValueError("boom")`)
	if r.code != 1 {
		t.Fatalf("got code %d; want 1", r.code)
	}
	for _, want := range []string{"ValueError: boom", `File "<sandbox>", line 1`} {
		if !strings.Contains(r.stderr, want) {
			t.Errorf("stderr %q does not contain %q", r.stderr, want)
		}
	}
	if strings.Contains(r.stderr, "zygote.py") {
		t.Errorf("stderr %q mentions zygote.py", r.stderr)
	}
}

func TestStdinPassthrough(t *testing.T) {
	sock := startZygote(t)
	r := runWith(t, sock, input("import sys; print(sys.stdin.read().upper())", 0, nil)+"abc", 5*time.Second)
	if r.code != 0 || r.stdout != "ABC\n" {
		t.Fatalf("got code %d stdout %q stderr %q; want 0, %q", r.code, r.stdout, r.stderr, "ABC\n")
	}
}

func TestEnv(t *testing.T) {
	sock := startZygote(t)
	r := runWith(t, sock, input(`import os; print(os.environ["GSS_T"])`, 0, map[string]string{"GSS_T": "x"}), 5*time.Second)
	if r.code != 0 || r.stdout != "x\n" {
		t.Fatalf("got code %d stdout %q stderr %q; want 0, %q", r.code, r.stdout, r.stderr, "x\n")
	}
}

func TestTimeout(t *testing.T) {
	sock := startZygote(t)
	start := time.Now()
	r := runWith(t, sock, input("import time; time.sleep(30)", 1, nil), 5*time.Second)
	if el := time.Since(start); el > 3*time.Second {
		t.Fatalf("took %v; want < 3s", el)
	}
	if r.code != 137 {
		t.Fatalf("got code %d stderr %q; want 137", r.code, r.stderr)
	}
}

func TestKilledBySignal(t *testing.T) {
	r := runCode(t, "import os,signal; os.kill(os.getpid(), signal.SIGTERM)")
	if r.code != 143 {
		t.Fatalf("got code %d stderr %q; want 143", r.code, r.stderr)
	}
}

func TestStderrSeparate(t *testing.T) {
	r := runCode(t, `import sys; sys.stdout.write("o"); sys.stderr.write("e")`)
	if r.code != 0 || r.stdout != "o" || r.stderr != "e" {
		t.Fatalf("got code %d stdout %q stderr %q; want 0, %q, %q", r.code, r.stdout, r.stderr, "o", "e")
	}
}

func TestConcurrent(t *testing.T) {
	sock := startZygote(t)
	const n = 16
	results := make([]result, n)
	errs := make([]error, n)
	start := time.Now()
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = runPipes(sock, input(fmt.Sprintf("import time; time.sleep(0.2); print(%d)", i), 0, nil), 5*time.Second)
		}()
	}
	wg.Wait()
	if el := time.Since(start); el >= 2*time.Second {
		t.Errorf("took %v; want < 2s", el)
	}
	for i, r := range results {
		if errs[i] != nil {
			t.Errorf("run %d: %v", i, errs[i])
			continue
		}
		if want := fmt.Sprintf("%d\n", i); r.code != 0 || r.stdout != want {
			t.Errorf("run %d: code %d stdout %q stderr %q; want 0, %q", i, r.code, r.stdout, r.stderr, want)
		}
	}
}

// rawClient is a test client that speaks the zygote socket protocol directly.
type rawClient struct {
	conn            *net.UnixConn
	inW, outR, errR *os.File
}

// sendRaw connects to the zygote and sends fresh pipe ends plus payload. Only the
// zygote holds the child ends afterwards, so outR and errR reach EOF when it is done.
func sendRaw(t *testing.T, sock string, payload []byte) *rawClient {
	t.Helper()
	inR, inW := mustPipe(t)
	outR, outW := mustPipe(t)
	errR, errW := mustPipe(t)
	t.Cleanup(func() { _ = inW.Close(); _ = outR.Close(); _ = errR.Close() })

	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	conn := c.(*net.UnixConn)
	t.Cleanup(func() { _ = conn.Close() })
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	rights := syscall.UnixRights(int(inR.Fd()), int(outW.Fd()), int(errW.Fd()))
	if _, _, err := conn.WriteMsgUnix(lenBuf[:], rights, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	_ = inR.Close()
	_ = outW.Close()
	_ = errW.Close()
	return &rawClient{conn: conn, inW: inW, outR: outR, errR: errR}
}

// reply reads the exit code; ok is false if the zygote closed without sending one.
func (c *rawClient) reply(t *testing.T) (code int32, ok bool) {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var buf [4]byte
	if _, err := io.ReadFull(c.conn, buf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, false
		}
		t.Fatalf("read reply: %v", err)
	}
	return int32(binary.BigEndian.Uint32(buf[:])), true
}

func readAll(t *testing.T, f *os.File) string {
	t.Helper()
	_ = f.SetReadDeadline(time.Now().Add(5 * time.Second))
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read pipe: %v", err)
	}
	return string(b)
}

func TestClientGone(t *testing.T) {
	sock := startZygote(t)
	payload, err := json.Marshal(map[string]any{
		"code":      `import time; print("started", flush=True); time.sleep(30)`,
		"timeout_s": 0,
		"env":       map[string]string{},
	})
	if err != nil {
		t.Fatal(err)
	}
	c := sendRaw(t, sock, payload)
	conn, outR := c.conn, c.outR

	got := make(chan string, 2)
	go func() {
		br := bufio.NewReader(outR)
		line, _ := br.ReadString('\n')
		got <- line
		_, _ = io.Copy(io.Discard, br)
		got <- "EOF"
	}()
	select {
	case line := <-got:
		if line != "started\n" {
			t.Fatalf("first stdout line %q; want %q", line, "started\n")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start within 5s")
	}
	_ = conn.Close()
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("stdout did not reach EOF within 2s of closing the connection; worker still alive")
	}
}

func TestBadHeader(t *testing.T) {
	sock := startZygote(t)
	r := runWith(t, sock, "not json\n", 5*time.Second)
	if r.code != 125 || !strings.HasPrefix(r.stderr, "pyfork:") {
		t.Fatalf("got code %d stderr %q; want 125 and a pyfork: message", r.code, r.stderr)
	}
}

func TestNoZygote(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "missing.sock")
	start := time.Now()
	r := runWith(t, sock, input(`print("hello")`, 0, nil), 200*time.Millisecond)
	if el := time.Since(start); el > time.Second {
		t.Fatalf("took %v; want < 1s", el)
	}
	if r.code != 125 || !strings.HasPrefix(r.stderr, "pyfork:") {
		t.Fatalf("got code %d stderr %q; want 125 and a pyfork: message", r.code, r.stderr)
	}
}

func TestLargeCode(t *testing.T) {
	code := "# " + strings.Repeat("x", 300<<10) + "\nprint(\"ok\")\n"
	r := runCode(t, code)
	if r.code != 0 || r.stdout != "ok\n" {
		t.Fatalf("got code %d stdout %q stderr %q; want 0, %q", r.code, r.stdout, r.stderr, "ok\n")
	}
}

func TestBadEnvName(t *testing.T) {
	sock := startZygote(t)
	for _, name := range []string{"", "A=B", "A\x00B"} {
		r := runWith(t, sock, input(`print("ran")`, 0, map[string]string{name: "x"}), 5*time.Second)
		if r.code != 125 || !strings.HasPrefix(r.stderr, "pyfork:") || r.stdout != "" {
			t.Errorf("env name %q: got code %d stdout %q stderr %q; want 125 and a pyfork: message", name, r.code, r.stdout, r.stderr)
		}
	}
}

func TestWorkerSetupFailure(t *testing.T) {
	sock := startZygote(t)
	payload := []byte(`{"code": "print('ran')", "timeout_s": 0, "env": {"A=B": "x"}}`)
	c := sendRaw(t, sock, payload)
	code, ok := c.reply(t)
	if !ok || code != 125 {
		t.Fatalf("reply %d (sent %v); want 125", code, ok)
	}
	if out := readAll(t, c.outR); out != "" {
		t.Errorf("stdout %q; want empty, the code must not run", out)
	}
	if e := readAll(t, c.errR); !strings.HasPrefix(e, "zygote: worker setup:") {
		t.Errorf("stderr %q; want a zygote: worker setup: line", e)
	}
}

func TestZygoteBadTimeout(t *testing.T) {
	sock := startZygote(t)
	for _, timeout := range []string{"-1", "NaN", "86401", "1" + strings.Repeat("0", 400)} {
		c := sendRaw(t, sock, []byte(`{"code": "print('ran')", "timeout_s": `+timeout+`, "env": {}}`))
		if code, ok := c.reply(t); ok {
			t.Errorf("timeout_s %.20s: zygote replied %d; want no reply", timeout, code)
		}
		if out := readAll(t, c.outR); out != "" {
			t.Errorf("timeout_s %.20s: stdout %q; want empty", timeout, out)
		}
		if e := readAll(t, c.errR); !strings.HasPrefix(e, "zygote:") {
			t.Errorf("timeout_s %.20s: stderr %q; want a zygote: line", timeout, e)
		}
	}
}

func TestBadTimeout(t *testing.T) {
	sock := startZygote(t)
	for _, timeout := range []string{"-1", "86401"} {
		in := `{"code_len": 11, "timeout_s": ` + timeout + "}\n" + `print("ran")`
		r := runWith(t, sock, in, 5*time.Second)
		if r.code != 125 || !strings.HasPrefix(r.stderr, "pyfork:") {
			t.Errorf("timeout_s %s: got code %d stderr %q; want 125 and a pyfork: message", timeout, r.code, r.stderr)
		}
	}
}

func TestCodeTooLarge(t *testing.T) {
	sock := startZygote(t)
	r := runWith(t, sock, fmt.Sprintf(`{"code_len": %d}`+"\n", 16<<20+1), 5*time.Second)
	if r.code != 125 || !strings.HasPrefix(r.stderr, "pyfork:") {
		t.Fatalf("got code %d stderr %q; want 125 and a pyfork: message", r.code, r.stderr)
	}
}

func TestBadFlag(t *testing.T) {
	if _, _, err := parseFlags([]string{"--no-such-flag"}, ""); err == nil {
		t.Error("unknown flag: no error")
	}
	if _, _, err := parseFlags([]string{"--connect-timeout", "soon"}, ""); err == nil {
		t.Error("bad duration: no error")
	}
	if _, _, err := parseFlags([]string{"extra"}, ""); err == nil {
		t.Error("positional argument: no error")
	}
	sock, timeout, err := parseFlags(nil, "")
	if err != nil || sock != "/tmp/gss-zygote.sock" || timeout != 30*time.Second {
		t.Errorf("defaults: %q %v %v", sock, timeout, err)
	}
	if sock, _, _ := parseFlags(nil, "/x.sock"); sock != "/x.sock" {
		t.Errorf("env default: %q; want /x.sock", sock)
	}
}

func TestTimeoutWorkerChangedGroup(t *testing.T) {
	sock := startZygote(t)
	start := time.Now()
	code := "import os, time\nos.setpgid(0, os.getpgid(os.getppid()))\ntime.sleep(5)\nprint(\"survived\")"
	r := runWith(t, sock, input(code, 1, nil), 5*time.Second)
	if el := time.Since(start); el > 3*time.Second {
		t.Errorf("took %v; want < 3s", el)
	}
	if r.code != 137 || strings.Contains(r.stdout, "survived") {
		t.Fatalf("got code %d stdout %q stderr %q; want 137 and no output", r.code, r.stdout, r.stderr)
	}
}

func TestTimeoutKillsChildren(t *testing.T) {
	sock := startZygote(t)
	path := filepath.Join(t.TempDir(), "child-ran")
	code := "import subprocess, sys, time\n" +
		"subprocess.Popen([sys.executable, '-c', \"import os, time; time.sleep(3); open(os.environ['GSS_P'], 'w').write('x')\"],\n" +
		"    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)\n" +
		"time.sleep(30)\n"
	r := runWith(t, sock, input(code, 1, map[string]string{"GSS_P": path}), 5*time.Second)
	if r.code != 137 {
		t.Fatalf("got code %d stderr %q; want 137", r.code, r.stderr)
	}
	time.Sleep(3500 * time.Millisecond)
	if _, err := os.Stat(path); err == nil {
		t.Fatal("the child wrote its file: the process group was not killed")
	}
}

func TestBackgroundChildSurvivesNormalExit(t *testing.T) {
	sock := startZygote(t)
	path := filepath.Join(t.TempDir(), "child-ran")
	code := "import subprocess, sys\n" +
		"subprocess.Popen([sys.executable, '-c', \"import os, time; time.sleep(1); open(os.environ['GSS_P'], 'w').write('done')\"],\n" +
		"    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)\n"
	start := time.Now()
	r := runWith(t, sock, input(code, 0, map[string]string{"GSS_P": path}), 5*time.Second)
	if el := time.Since(start); r.code != 0 || el > 900*time.Millisecond {
		t.Fatalf("got code %d after %v stderr %q; want 0 at once", r.code, el, r.stderr)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if b, err := os.ReadFile(path); err == nil && string(b) == "done" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the background child did not write its file within 3s")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestThreadOutputNotLost(t *testing.T) {
	r := runCode(t, "import threading, time\ndef f():\n    time.sleep(0.2)\n    print(\"late\")\nthreading.Thread(target=f).start()\n")
	if r.code != 0 || r.stdout != "late\n" {
		t.Fatalf("got code %d stdout %q stderr %q; want 0, %q", r.code, r.stdout, r.stderr, "late\n")
	}
}

func TestAtexitRuns(t *testing.T) {
	r := runCode(t, "import atexit\natexit.register(lambda: print(\"bye\"))\nprint(\"main\")\n")
	if r.code != 0 || r.stdout != "main\nbye\n" {
		t.Fatalf("got code %d stdout %q stderr %q; want 0, %q", r.code, r.stdout, r.stderr, "main\nbye\n")
	}
}

func TestUnclosedFileIsFlushed(t *testing.T) {
	sock := startZygote(t)
	path := filepath.Join(t.TempDir(), "out.txt")
	code := "import os\ndef g(): pass\nf = open(os.environ[\"GSS_P\"], \"w\")\nf.write(\"data\")\n"
	r := runWith(t, sock, input(code, 0, map[string]string{"GSS_P": path}), 5*time.Second)
	if r.code != 0 {
		t.Fatalf("got code %d stderr %q; want 0", r.code, r.stderr)
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != "data" {
		t.Fatalf("file holds %q (%v); want %q", b, err, "data")
	}
}

func TestPickleUserClass(t *testing.T) {
	r := runCode(t, "import pickle\nclass C:\n    x = 1\nprint(pickle.loads(pickle.dumps(C())).x)\n")
	if r.code != 0 || r.stdout != "1\n" {
		t.Fatalf("got code %d stdout %q stderr %q; want 0, %q", r.code, r.stdout, r.stderr, "1\n")
	}
}

func TestImportFromCwd(t *testing.T) {
	sock := startZygote(t)
	code := "import os\nos.chdir(os.environ[\"GSS_D\"])\nopen(\"m.py\", \"w\").write(\"V = 5\\n\")\nimport m\nprint(m.V)\n"
	r := runWith(t, sock, input(code, 0, map[string]string{"GSS_D": t.TempDir()}), 5*time.Second)
	if r.code != 0 || r.stdout != "5\n" {
		t.Fatalf("got code %d stdout %q stderr %q; want 0, %q", r.code, r.stdout, r.stderr, "5\n")
	}
}

func TestArgv(t *testing.T) {
	r := runCode(t, "import sys\nprint(sys.argv == [\"-c\"])\n")
	if r.code != 0 || r.stdout != "True\n" {
		t.Fatalf("got code %d stdout %q stderr %q; want 0, %q", r.code, r.stdout, r.stderr, "True\n")
	}
}

func TestSystemExitNonInt(t *testing.T) {
	r := runCode(t, `import sys; sys.exit("msg")`)
	if r.code != 1 || r.stderr != "msg\n" {
		t.Fatalf("got code %d stderr %q; want 1, %q", r.code, r.stderr, "msg\n")
	}
}

func TestSyntaxError(t *testing.T) {
	r := runCode(t, "x = (")
	if r.code != 1 || !strings.Contains(r.stderr, "SyntaxError") || strings.Contains(r.stderr, "zygote.py") {
		t.Fatalf("got code %d stderr %q; want 1 and a SyntaxError without zygote.py", r.code, r.stderr)
	}
}

func TestUnclosedFileIsFlushedNoFunction(t *testing.T) {
	sock := startZygote(t)
	path := filepath.Join(t.TempDir(), "out.txt")
	// No function in __main__ here: the file object is not part of a reference cycle.
	code := "import os\nf = open(os.environ[\"GSS_P\"], \"w\")\nf.write(\"data\")\n"
	r := runWith(t, sock, input(code, 0, map[string]string{"GSS_P": path}), 5*time.Second)
	if r.code != 0 {
		t.Fatalf("got code %d stderr %q; want 0", r.code, r.stderr)
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != "data" {
		t.Fatalf("file holds %q (%v); want %q", b, err, "data")
	}
}

func TestDelSeesGlobals(t *testing.T) {
	r := runCode(t, "import os\nclass T:\n    def __del__(self):\n        print(os.getpid() > 0)\nt = T()\n")
	if r.code != 0 || r.stdout != "True\n" || r.stderr != "" {
		t.Fatalf("got code %d stdout %q stderr %q; want 0, %q, no stderr", r.code, r.stdout, r.stderr, "True\n")
	}
}

func TestDaemonThreadNoNoise(t *testing.T) {
	sock := startZygotePrimed(t, "json,decimal,statistics,csv,datetime,collections")
	// A tiny switch interval lets the daemon thread run while the worker shuts down,
	// as a large primed heap does in the real image (a long gc pass).
	code := "import sys, threading, time\n" +
		"sys.setswitchinterval(1e-6)\n" +
		"def bg():\n" +
		"    while True:\n" +
		"        time.sleep(0)\n" +
		"threading.Thread(target=bg, daemon=True).start()\n" +
		"time.sleep(0.01)\n"
	for i := range 10 {
		r := runWith(t, sock, input(code, 0, nil), 5*time.Second)
		if r.code != 0 || r.stderr != "" {
			t.Fatalf("run %d: got code %d stderr %q; want 0 and no stderr", i, r.code, r.stderr)
		}
	}
}

func TestNoopExecIsFast(t *testing.T) {
	sock := startZygotePrimed(t, "json,re,decimal,statistics,csv,datetime,collections,itertools,functools")
	const n = 20
	times := make([]time.Duration, n)
	for i := range n {
		start := time.Now()
		r := runWith(t, sock, input("pass", 0, nil), 5*time.Second)
		times[i] = time.Since(start)
		if r.code != 0 {
			t.Fatalf("run %d: got code %d stderr %q; want 0", i, r.code, r.stderr)
		}
	}
	slices.Sort(times)
	median := (times[n/2-1] + times[n/2]) / 2
	t.Logf("no-op exec: median %v, min %v, max %v over %d runs", median, times[0], times[n-1], n)
	if median >= 100*time.Millisecond {
		t.Fatalf("median no-op exec %v; want < 100ms", median)
	}
}

// twoDraws runs code twice on one zygote primed with prime and returns the
// two outputs. Every Exec is a fork of the same primed interpreter, as every
// sandbox restored from one checkpoint image is.
func twoDraws(t *testing.T, prime, code string) (string, string) {
	t.Helper()
	sock := startZygotePrimed(t, prime)
	var out [2]string
	for i := range out {
		r := runWith(t, sock, input(code, 0, nil), 5*time.Second)
		if r.code != 0 || r.stdout == "" {
			t.Fatalf("exec %d: code %d stdout %q stderr %q", i+1, r.code, r.stdout, r.stderr)
		}
		out[i] = r.stdout
	}
	return out[0], out[1]
}

func TestNumpyRandomReseeded(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not on PATH")
	}
	if out, err := exec.Command(py, "-c", "import numpy.random").CombinedOutput(); err != nil {
		t.Skipf("numpy is not importable by the host python3 (%v: %s); the sandbox rootfs has it", err, strings.TrimSpace(string(out)))
	}
	// numpy.random is primed explicitly: numpy 2 loads it lazily, and in the
	// sandbox pandas loads it at import.
	a, b := twoDraws(t, "numpy,numpy.random", "import numpy; print(numpy.random.randint(0, 2**62))")
	if a == b {
		t.Errorf("two Execs drew the same numpy.random.randint: %q", a)
	}
}

func TestRandomReseeded(t *testing.T) {
	a, b := twoDraws(t, "random", "import random; print(random.getrandbits(64))")
	if a == b {
		t.Errorf("two Execs drew the same random.getrandbits(64): %q", a)
	}
}
