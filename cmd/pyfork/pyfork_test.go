package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// startZygote runs images/python/zygote.py on a socket in a fresh temp dir and
// waits for its READY line. It returns the socket path.
func startZygote(t *testing.T) string {
	t.Helper()
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not on PATH")
	}
	_, file, _, _ := runtime.Caller(0)
	script := filepath.Join(filepath.Dir(file), "..", "..", "images", "python", "zygote.py")
	sock := filepath.Join(t.TempDir(), "z.sock")

	cmd := exec.Command(py, script)
	cmd.Env = append(os.Environ(), "GSS_ZYGOTE_SOCK="+sock, "GSS_PRIME=json")
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
	inR, inW := mustPipe(t)
	outR, outW := mustPipe(t)
	errR, errW := mustPipe(t)

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
	return result{code: code, stdout: stdout.String(), stderr: stderr.String()}
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
	start := time.Now()
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = runWith(t, sock, input(fmt.Sprintf("import time; time.sleep(0.2); print(%d)", i), 0, nil), 5*time.Second)
		}()
	}
	wg.Wait()
	if el := time.Since(start); el >= 2*time.Second {
		t.Errorf("took %v; want < 2s", el)
	}
	for i, r := range results {
		if want := fmt.Sprintf("%d\n", i); r.code != 0 || r.stdout != want {
			t.Errorf("run %d: code %d stdout %q stderr %q; want 0, %q", i, r.code, r.stdout, r.stderr, want)
		}
	}
}

func TestClientGone(t *testing.T) {
	sock := startZygote(t)
	inR, inW := mustPipe(t)
	defer inW.Close()
	outR, outW := mustPipe(t)
	defer outR.Close()
	errR, errW := mustPipe(t)
	defer errR.Close()

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]any{
		"code":      `import time; print("started", flush=True); time.sleep(30)`,
		"timeout_s": 0,
		"env":       map[string]string{},
	})
	if err != nil {
		t.Fatal(err)
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	rights := syscall.UnixRights(int(inR.Fd()), int(outW.Fd()), int(errW.Fd()))
	if _, _, err := conn.(*net.UnixConn).WriteMsgUnix(lenBuf[:], rights, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	// Only the worker holds the pipe ends now.
	_ = inR.Close()
	_ = outW.Close()
	_ = errW.Close()

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
