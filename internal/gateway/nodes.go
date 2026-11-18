package gateway

import (
	"context"
	"errors"
	"sync"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/nodeagent"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// Keepalive on the connections to nodes (D37): a ping after 10 s
// without activity, the shortest interval grpc-go allows a client, and the
// connection closed when no answer comes within 1 s. A node that vanishes
// without closing its connection is noticed by keepalive after about 11 s;
// a proxied Exec ends sooner, as soon as the scheduler marks the sandbox
// LOST (Server.watch).
const (
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = time.Second
)

// reconnectParams cap the wait between connection attempts to a node at a
// second (grpc-go's default grows to two minutes), so a node agent that
// comes back at the same address is reachable again at once.
var reconnectParams = grpc.ConnectParams{
	Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second},
	MinConnectTimeout: 5 * time.Second,
}

// NodeClient reaches node agents; tests fake it. A NodeClient that caches
// connections may also have a Forget(addr string) method, which the gateway
// calls when a node registers again at another address.
type NodeClient interface {
	Create(ctx context.Context, addr string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error)
	Exec(ctx context.Context, addr string, req *gssv1.ExecRequest) (grpc.ServerStreamingClient[gssv1.ExecEvent], error)
	Destroy(ctx context.Context, addr string, sandboxID string, incarnation int32) error // 0 = whatever is live
}

// grpcNodes keeps one connection per node address, made on first use.
type grpcNodes struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewGRPCNodes returns a NodeClient over plaintext gRPC (D7) with one
// cached connection per address, keepalive, a reconnect backoff of at most
// a second, the otelgrpc client handler (spans that carry the trace to the
// node) and nodeagent.MaxMessageBytes limits. The value also implements io.Closer,
// which closes every connection.
func NewGRPCNodes() NodeClient {
	return &grpcNodes{conns: map[string]*grpc.ClientConn{}}
}

func (g *grpcNodes) client(addr string) (gssv1.NodeServiceClient, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	conn := g.conns[addr]
	if conn == nil {
		var err error
		conn, err = grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time: keepaliveTime, Timeout: keepaliveTimeout, PermitWithoutStream: true,
			}),
			grpc.WithConnectParams(reconnectParams),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(nodeagent.MaxMessageBytes), grpc.MaxCallRecvMsgSize(nodeagent.MaxMessageBytes)))
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "node at %s: %v", addr, err)
		}
		g.conns[addr] = conn
	}
	return gssv1.NewNodeServiceClient(conn), nil
}

func (g *grpcNodes) Create(ctx context.Context, addr string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
	c, err := g.client(addr)
	if err != nil {
		return nil, err
	}
	return c.CreateSandbox(ctx, req)
}

func (g *grpcNodes) Exec(ctx context.Context, addr string, req *gssv1.ExecRequest) (grpc.ServerStreamingClient[gssv1.ExecEvent], error) {
	c, err := g.client(addr)
	if err != nil {
		return nil, err
	}
	return c.Exec(ctx, req)
}

func (g *grpcNodes) Destroy(ctx context.Context, addr, sandboxID string, incarnation int32) error {
	c, err := g.client(addr)
	if err != nil {
		return err
	}
	_, err = c.DestroySandbox(ctx, &gssv1.DestroySandboxRequest{SandboxId: sandboxID, Incarnation: incarnation})
	return err
}

// Forget closes and drops the connection to addr, if there is one.
func (g *grpcNodes) Forget(addr string) {
	g.mu.Lock()
	conn := g.conns[addr]
	delete(g.conns, addr)
	g.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
}

// Close closes every connection.
func (g *grpcNodes) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	var errs []error
	for addr, conn := range g.conns {
		errs = append(errs, conn.Close())
		delete(g.conns, addr)
	}
	return errors.Join(errs...)
}
