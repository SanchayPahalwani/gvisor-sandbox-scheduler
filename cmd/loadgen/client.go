package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
)

// Per-call deadlines. They only keep a hung call from stopping a run; none is near
// what a healthy call takes.
const (
	createTimeout  = 60 * time.Second
	execTimeout    = 60 * time.Second
	destroyTimeout = 30 * time.Second
	queryTimeout   = 10 * time.Second
)

// A forced-warm create fails with FailedPrecondition when the scheduler's warm count
// was stale; it is retried after staleWarmPause, at most staleWarmRetries times.
const (
	staleWarmRetries = 10
	staleWarmPause   = 500 * time.Millisecond
)

// helloCode is the program every start measurement runs.
const helloCode = `print("hello")`

// dial connects to the scheduler with the otelgrpc client handler: with
// telemetry on, each call is the root span of its trace (spec 9).
func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// sleepCtx waits for d or until ctx ends.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// cleanupCtx is for destroying sandboxes after ctx was cancelled (Ctrl-C).
func cleanupCtx(ctx context.Context) context.Context { return context.WithoutCancel(ctx) }

func createSandbox(ctx context.Context, c gssv1.SandboxServiceClient, spec *gssv1.SandboxSpec) (*gssv1.Sandbox, error) {
	ctx, cancel := context.WithTimeout(ctx, createTimeout)
	defer cancel()
	return c.CreateSandbox(ctx, &gssv1.CreateSandboxRequest{Spec: spec})
}

// destroySandbox destroys whatever incarnation of id is live.
func destroySandbox(ctx context.Context, c gssv1.SandboxServiceClient, id string) error {
	ctx, cancel := context.WithTimeout(ctx, destroyTimeout)
	defer cancel()
	_, err := c.DestroySandbox(ctx, &gssv1.DestroySandboxRequest{SandboxId: id})
	return err
}

func getSandbox(ctx context.Context, c gssv1.SandboxServiceClient, id string) (*gssv1.Sandbox, error) {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	return c.GetSandbox(ctx, &gssv1.GetSandboxRequest{SandboxId: id})
}

func listNodes(ctx context.Context, c gssv1.SandboxServiceClient) ([]*gssv1.Node, error) {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	resp, err := c.ListNodes(ctx, &gssv1.ListNodesRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetNodes(), nil
}

// execCode runs python code in sandbox id and returns when its exit event arrives.
// Per spec D34 a stream that ends with a gRPC status, or without an exit event, did
// not see the program exit: that is an error, returned with its status code. A
// non-zero exit, a timeout or an OOM kill is an error too, since every program the
// load generator runs prints one line and exits 0.
func execCode(ctx context.Context, c gssv1.SandboxServiceClient, id, code string, timeout time.Duration) (exitAt time.Time, err error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stream, err := c.Exec(ctx, &gssv1.ExecRequest{SandboxId: id, Command: &gssv1.ExecRequest_PythonCode{PythonCode: code}})
	if err != nil {
		return time.Time{}, err
	}
	var exit *gssv1.ExitStatus
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if exit != nil {
				break // the result is in; how the stream closed after it does not change it
			}
			return time.Time{}, err
		}
		if e := ev.GetExit(); e != nil && exit == nil {
			exit, exitAt = e, time.Now()
		}
	}
	switch {
	case exit == nil:
		return time.Time{}, errors.New("exec stream ended without an exit event")
	case exit.GetTimedOut():
		return time.Time{}, errors.New("program timed out")
	case exit.GetOomKilled():
		return time.Time{}, errors.New("program was OOM-killed")
	case exit.GetCode() != 0:
		return time.Time{}, fmt.Errorf("program exited with code %d", exit.GetCode())
	}
	return exitAt, nil
}

// cycle is one create -> Exec -> destroy.
type cycle struct {
	sb            *gssv1.Sandbox
	createMS      float64 // client-side CreateSandbox round trip of the attempt that succeeded
	readyMS       float64 // start_latency in the response: node agent receives Create -> ready
	firstResultMS float64 // Create sent -> exit event of the Exec
	retries       int     // forced-warm creates retried after FailedPrecondition
	// cleanupErr is a destroy that failed after the cycle had already failed; it is an
	// error of its own (a sandbox may be left behind).
	cleanupErr error
}

// runCycle creates a sandbox from spec, runs code in it and destroys it. A cycle with
// any failed step is an error and its figures must not be used. A forced start mode
// must come back as the path that ran, so a figure is never filed under the wrong mode.
func runCycle(ctx context.Context, c gssv1.SandboxServiceClient, spec func() *gssv1.SandboxSpec, code string) (cycle, error) {
	var res cycle
	var sb *gssv1.Sandbox
	var sent time.Time
	var s *gssv1.SandboxSpec
	for {
		s = spec()
		sent = time.Now()
		var err error
		sb, err = createSandbox(ctx, c, s)
		res.createMS = ms(time.Since(sent))
		if err == nil {
			break
		}
		if s.GetStartMode() == gssv1.StartMode_START_MODE_WARM && status.Code(err) == codes.FailedPrecondition && res.retries < staleWarmRetries {
			res.retries++
			if err := sleepCtx(ctx, staleWarmPause); err != nil {
				return res, err
			}
			continue
		}
		return res, fmt.Errorf("create: %w", err)
	}
	res.sb = sb
	fail := func(err error) (cycle, error) {
		if derr := destroySandbox(cleanupCtx(ctx), c, sb.GetId()); derr != nil {
			res.cleanupErr = fmt.Errorf("destroy %s after the failure: %w", sb.GetId(), derr)
		}
		return res, err
	}
	if forced := s.GetStartMode(); forced != gssv1.StartMode_START_MODE_AUTO && sb.GetStartedAs() != forced {
		return fail(fmt.Errorf("sandbox %s was forced to %s but started as %s", sb.GetId(), modeName(forced), modeName(sb.GetStartedAs())))
	}
	if sb.GetStartLatency() == nil {
		return fail(fmt.Errorf("sandbox %s: the response has no start_latency", sb.GetId()))
	}
	res.readyMS = ms(sb.GetStartLatency().AsDuration())
	exitAt, err := execCode(ctx, c, sb.GetId(), code, execTimeout)
	if err != nil {
		return fail(fmt.Errorf("exec in %s: %w", sb.GetId(), err))
	}
	res.firstResultMS = ms(exitAt.Sub(sent))
	if err := destroySandbox(cleanupCtx(ctx), c, sb.GetId()); err != nil {
		return res, fmt.Errorf("destroy %s: %w", sb.GetId(), err)
	}
	return res, nil
}
