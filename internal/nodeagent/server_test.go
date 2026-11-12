package nodeagent

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// dialServer serves srv over an in-memory connection and returns a client.
// The connection and the server are closed at cleanup, before anything the
// test registered earlier (such as the manager's Close).
func dialServer(t *testing.T, srv gssv1.NodeServiceServer) gssv1.NodeServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer(ServerOptions()...)
	gssv1.RegisterNodeServiceServer(s, srv)
	go func() { _ = s.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		s.Stop()
	})
	return gssv1.NewNodeServiceClient(conn)
}

// syncBuffer is a log destination that the server's goroutines write to
// while the test reads it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func (s *syncBuffer) Lines() []string { return strings.Split(strings.TrimSpace(s.String()), "\n") }

// echoExec is a fake ExecFunc: argv "echo ..." prints its arguments, and
// anything else (the readiness probe) exits 0.
func echoExec(ctx context.Context, ref runsc.Ref, s runsc.ExecSpec) (int, error) {
	if len(s.Args) > 0 && s.Args[0] == "echo" {
		fmt.Fprintln(s.Stdout, strings.Join(s.Args[1:], " "))
	}
	return 0, nil
}

// serverEnv starts a manager with echoExec and returns a client of its
// NodeService and the server's log.
func serverEnv(t *testing.T) (*env, gssv1.NodeServiceClient, *syncBuffer) {
	t.Helper()
	e := newEnv(t, nil)
	e.rt.ExecFunc = echoExec
	e.start(t)
	logs := &syncBuffer{}
	srv := NewServer(e.m).(*server)
	srv.log = slog.New(slog.NewTextHandler(logs, nil))
	return e, dialServer(t, srv), logs
}

// recvAll reads an Exec stream to its end and returns its events as
// "stdout ...", "stderr ..." and "exit <code> <timed_out> <oom_killed>",
// and the status that ended the stream (nil for a clean end).
func recvAll(stream grpc.ServerStreamingClient[gssv1.ExecEvent]) ([]string, error) {
	var evs []string
	for {
		ev, err := stream.Recv()
		if err == io.EOF {
			return evs, nil
		}
		if err != nil {
			return evs, err
		}
		switch {
		case ev.GetExit() != nil:
			x := ev.GetExit()
			evs = append(evs, fmt.Sprintf("exit %d %v %v", x.GetCode(), x.GetTimedOut(), x.GetOomKilled()))
		case ev.GetStdout() != nil:
			evs = append(evs, "stdout "+string(ev.GetStdout()))
		default:
			evs = append(evs, "stderr "+string(ev.GetStderr()))
		}
	}
}

func TestServerCreateExecDestroy(t *testing.T) {
	e, c, _ := serverEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := c.CreateSandbox(ctx, &gssv1.NodeCreateRequest{SandboxId: "sb-1", Incarnation: 2})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStartedAs() != gssv1.StartMode_START_MODE_COLD || resp.GetReadyLatency() == nil {
		t.Errorf("response = %v, want COLD with a ready_latency", resp)
	}

	stream, err := c.Exec(ctx, argvReq("sb-1", "echo", "hello"))
	if err != nil {
		t.Fatal(err)
	}
	evs, err := recvAll(stream)
	if err != nil {
		t.Fatalf("Exec stream: %v (events %q)", err, evs)
	}
	if want := []string{"stdout hello\n", "exit 0 false false"}; !slices.Equal(evs, want) {
		t.Errorf("events = %q, want %q", evs, want)
	}

	// The request's incarnation reaches the manager: an older one leaves
	// incarnation 2 alone.
	if _, err := c.DestroySandbox(ctx, &gssv1.DestroySandboxRequest{SandboxId: "sb-1", Incarnation: 1}); err != nil {
		t.Fatal(err)
	}
	if got := e.listed(); !slices.Equal(got, []string{"sb-1/2"}) {
		t.Fatalf("after Destroy of incarnation 1: List = %q, want [sb-1/2]", got)
	}
	if _, err := c.DestroySandbox(ctx, &gssv1.DestroySandboxRequest{SandboxId: "sb-1", Incarnation: 2}); err != nil {
		t.Fatal(err)
	}
	if got := e.listed(); len(got) != 0 {
		t.Errorf("after Destroy of incarnation 2: List = %q", got)
	}
	if got := e.rt.Containers(); len(got) != 0 {
		t.Errorf("containers after Destroy = %v", got)
	}
}

func TestServerStatusCodes(t *testing.T) {
	_, c, _ := serverEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := c.Exec(ctx, argvReq("nope", "true"))
	if err != nil {
		t.Fatal(err)
	}
	evs, err := recvAll(stream)
	wantCode(t, err, codes.NotFound, "Exec in an unknown sandbox")
	if len(evs) != 0 {
		t.Errorf("events before the status: %q", evs)
	}

	// Other manager statuses pass through unchanged.
	_, err = c.CreateSandbox(ctx, &gssv1.NodeCreateRequest{SandboxId: "bad_id"})
	wantCode(t, err, codes.InvalidArgument, "Create with an invalid sandbox ID")
	if _, err := c.CreateSandbox(ctx, &gssv1.NodeCreateRequest{SandboxId: "sb-1", Incarnation: 1}); err != nil {
		t.Fatal(err)
	}
	_, err = c.CreateSandbox(ctx, &gssv1.NodeCreateRequest{SandboxId: "sb-1", Incarnation: 1})
	wantCode(t, err, codes.AlreadyExists, "second Create of the same incarnation")
	stream, err = c.Exec(ctx, &gssv1.ExecRequest{SandboxId: "sb-1"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = recvAll(stream)
	wantCode(t, err, codes.InvalidArgument, "Exec with no command")
}

func TestServerLogs(t *testing.T) {
	_, c, logs := serverEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := c.CreateSandbox(ctx, &gssv1.NodeCreateRequest{SandboxId: "sb-1", Incarnation: 1}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"sb-1", "nope"} { // the second fails
		stream, err := c.Exec(ctx, pyReq(id, "pass"))
		if err != nil {
			t.Fatal(err)
		}
		recvAll(stream)
	}
	if _, err := c.DestroySandbox(ctx, &gssv1.DestroySandboxRequest{SandboxId: "sb-1"}); err != nil {
		t.Fatal(err)
	}

	lines := logs.Lines()
	want := []string{
		`level=INFO msg=create sandbox=sb-1 incarnation=1 mode=cold ms=`,
		`level=WARN msg="exec failed" sandbox=nope kind=python ms=`,
		`level=INFO msg=destroy sandbox=sb-1 incarnation=0 ms=`,
	}
	if len(lines) != len(want) {
		t.Fatalf("log has %d lines, want %d:\n%s", len(lines), len(want), strings.Join(lines, "\n"))
	}
	for i, w := range want {
		if !strings.Contains(lines[i], w) {
			t.Errorf("log line %d = %q, want it to contain %q", i, lines[i], w)
		}
	}
	if !strings.Contains(lines[1], "code=NotFound") {
		t.Errorf("exec failure line %q has no code=NotFound", lines[1])
	}
}
