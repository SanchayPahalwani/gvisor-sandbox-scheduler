// Command pyfork runs one Python program in the sandbox's primed interpreter.
//
// It reads a request from stdin: one line of JSON
// {"code_len": <int>, "timeout_s": <number, 0 = none>, "env": {"K": "V"}}, a newline,
// exactly code_len bytes of UTF-8 Python source, then the program's own stdin. It hands
// its stdin, stdout and stderr to the zygote over a Unix socket (SCM_RIGHTS), so the
// program's output goes straight to pyfork's caller, and exits with the program's exit
// code (128 + signal if it was killed by a signal, 125 for pyfork's own failures).
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"time"
	"unicode/utf8"
)

const (
	exitInternal  = 125
	defaultSocket = "/tmp/gss-zygote.sock"
	maxHeaderLen  = 1 << 20
	maxCodeLen    = 64 << 20
	retryInterval = 5 * time.Millisecond
)

func main() {
	sock := os.Getenv("GSS_ZYGOTE_SOCK")
	if sock == "" {
		sock = defaultSocket
	}
	socket := flag.String("socket", sock, "zygote Unix socket path")
	connectTimeout := flag.Duration("connect-timeout", 30*time.Second, "how long to keep retrying the connect")
	flag.Parse()
	os.Exit(run(os.Stdin, os.Stdout, os.Stderr, *socket, *connectTimeout))
}

func run(stdin, stdout, stderr *os.File, socket string, connectTimeout time.Duration) int {
	code, err := forward(stdin, stdout, stderr, socket, connectTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "pyfork: %v\n", err)
		return exitInternal
	}
	return code
}

type header struct {
	CodeLen  *int              `json:"code_len"`
	TimeoutS float64           `json:"timeout_s"`
	Env      map[string]string `json:"env"`
}

type request struct {
	Code     string            `json:"code"`
	TimeoutS float64           `json:"timeout_s"`
	Env      map[string]string `json:"env"`
}

func forward(stdin, stdout, stderr *os.File, socket string, connectTimeout time.Duration) (int, error) {
	req, err := readRequest(stdin)
	if err != nil {
		return 0, err
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("encode request: %w", err)
	}
	conn, err := dial(socket, connectTimeout)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	rights := syscall.UnixRights(int(stdin.Fd()), int(stdout.Fd()), int(stderr.Fd()))
	if _, _, err := conn.WriteMsgUnix(lenBuf[:], rights, nil); err != nil {
		return 0, fmt.Errorf("send descriptors: %w", err)
	}
	if _, err := conn.Write(payload); err != nil {
		return 0, fmt.Errorf("send request: %w", err)
	}
	var codeBuf [4]byte
	if _, err := io.ReadFull(conn, codeBuf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, errors.New("zygote closed the connection without an exit code")
		}
		return 0, fmt.Errorf("read exit code: %w", err)
	}
	code := int(int32(binary.BigEndian.Uint32(codeBuf[:])))
	if code < 0 {
		return 128 - code, nil
	}
	return code, nil
}

// readRequest reads the header line one byte at a time and then exactly code_len bytes,
// so everything after the code stays in fd 0 for the program.
func readRequest(stdin io.Reader) (request, error) {
	var line bytes.Buffer
	var b [1]byte
	for {
		n, err := stdin.Read(b[:])
		if n == 1 {
			if b[0] == '\n' {
				break
			}
			if line.Len() >= maxHeaderLen {
				return request{}, fmt.Errorf("header line longer than %d bytes", maxHeaderLen)
			}
			line.WriteByte(b[0])
			continue
		}
		if err == io.EOF {
			return request{}, errors.New("stdin ended before the header line")
		}
		if err != nil {
			return request{}, fmt.Errorf("read header: %w", err)
		}
	}
	var h header
	if err := json.Unmarshal(line.Bytes(), &h); err != nil {
		return request{}, fmt.Errorf("bad header: %w", err)
	}
	switch {
	case h.CodeLen == nil:
		return request{}, errors.New("bad header: code_len is missing")
	case *h.CodeLen < 0 || *h.CodeLen > maxCodeLen:
		return request{}, fmt.Errorf("bad header: code_len %d is outside 0..%d", *h.CodeLen, maxCodeLen)
	case h.TimeoutS < 0:
		return request{}, fmt.Errorf("bad header: timeout_s %v is negative", h.TimeoutS)
	}
	code := make([]byte, *h.CodeLen)
	if _, err := io.ReadFull(stdin, code); err != nil {
		return request{}, fmt.Errorf("read code (%d bytes): %w", *h.CodeLen, err)
	}
	if !utf8.Valid(code) {
		return request{}, errors.New("code is not valid UTF-8")
	}
	env := h.Env
	if env == nil {
		env = map[string]string{}
	}
	return request{Code: string(code), TimeoutS: h.TimeoutS, Env: env}, nil
}

// dial connects to the zygote, retrying every 5 ms until connectTimeout has passed.
func dial(socket string, connectTimeout time.Duration) (*net.UnixConn, error) {
	addr := &net.UnixAddr{Name: socket, Net: "unix"}
	deadline := time.Now().Add(connectTimeout)
	for {
		conn, err := net.DialUnix("unix", nil, addr)
		if err == nil {
			return conn, nil
		}
		if !time.Now().Add(retryInterval).Before(deadline) {
			return nil, fmt.Errorf("connect to zygote: %w", err)
		}
		time.Sleep(retryInterval)
	}
}
