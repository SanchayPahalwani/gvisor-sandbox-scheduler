package nodeagent

import (
	"context"
	"log/slog"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// MaxMessageBytes is the largest gRPC message a node agent sends or
// receives: room for python_code up to pyfork's 16 MiB limit plus stdin.
// Clients of NodeService set the same limit with grpc.WithDefaultCallOptions
// (grpc.MaxCallSendMsgSize, grpc.MaxCallRecvMsgSize).
const MaxMessageBytes = 20 << 20

// keepaliveMinTime is the shortest interval between a client's keepalive
// pings that the server accepts; a client that pings more often is cut off.
const keepaliveMinTime = time.Second

// ServerOptions are the gRPC server options of a node agent: the otelgrpc
// stats handler (spans for each RPC, continuing the caller's trace), message
// limits of MaxMessageBytes, and a keepalive policy that lets the scheduler
// ping its connections once a second, also while no RPC is open.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(MaxMessageBytes),
		grpc.MaxSendMsgSize(MaxMessageBytes),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             keepaliveMinTime,
			PermitWithoutStream: true,
		}),
	}
}

// server is the NodeService of one node. Each RPC goes to the Manager, whose
// errors are already gRPC statuses and are returned as they are.
type server struct {
	gssv1.UnimplementedNodeServiceServer
	m   *Manager
	log *slog.Logger
}

// NewServer returns the NodeService for m. It logs one line per create and
// destroy, and one per Exec that ends with an error, to slog's default logger
// as it is when NewServer is called.
func NewServer(m *Manager) gssv1.NodeServiceServer {
	return &server{m: m, log: slog.Default()}
}

func (s *server) CreateSandbox(ctx context.Context, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
	begin := time.Now()
	resp, err := s.m.Create(ctx, req)
	if err != nil {
		// The mode is the one requested; none ran.
		s.log.Warn("create failed", "sandbox", req.GetSandboxId(), "incarnation", req.GetIncarnation(),
			"mode", modeName(req.GetSpec().GetStartMode()), "ms", sinceMS(begin),
			"code", status.Code(err), "err", status.Convert(err).Message())
		return nil, err
	}
	s.log.Info("create", "sandbox", req.GetSandboxId(), "incarnation", req.GetIncarnation(),
		"mode", modeName(resp.GetStartedAs()), "ms", sinceMS(begin))
	return resp, nil
}

// Exec streams the Exec's events. Manager.Exec calls send from one goroutine
// at a time, as a server stream requires.
func (s *server) Exec(req *gssv1.ExecRequest, stream grpc.ServerStreamingServer[gssv1.ExecEvent]) error {
	begin := time.Now()
	err := s.m.Exec(stream.Context(), req, stream.Send)
	if err != nil {
		s.log.Warn("exec failed", "sandbox", req.GetSandboxId(), "kind", execKind(req), "ms", sinceMS(begin),
			"code", status.Code(err), "err", status.Convert(err).Message())
	}
	return err
}

func (s *server) DestroySandbox(ctx context.Context, req *gssv1.DestroySandboxRequest) (*gssv1.DestroySandboxResponse, error) {
	begin := time.Now()
	if err := s.m.Destroy(ctx, req.GetSandboxId(), req.GetIncarnation()); err != nil {
		s.log.Warn("destroy failed", "sandbox", req.GetSandboxId(), "incarnation", req.GetIncarnation(),
			"ms", sinceMS(begin), "code", status.Code(err), "err", status.Convert(err).Message())
		return nil, err
	}
	s.log.Info("destroy", "sandbox", req.GetSandboxId(), "incarnation", req.GetIncarnation(), "ms", sinceMS(begin))
	return &gssv1.DestroySandboxResponse{}, nil
}

// execKind names the command of an Exec as the node.exec span does.
func execKind(req *gssv1.ExecRequest) string {
	switch req.GetCommand().(type) {
	case *gssv1.ExecRequest_Argv:
		return "argv"
	case *gssv1.ExecRequest_PythonCode:
		return "python"
	}
	return "none"
}

func sinceMS(t time.Time) int64 { return time.Since(t).Milliseconds() }
