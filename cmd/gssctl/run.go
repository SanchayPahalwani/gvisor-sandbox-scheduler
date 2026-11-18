package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
)

// errOutput marks a failed write of the program's output to gssctl's own stdout or
// stderr (a closed pipe, a terminal gone).
var errOutput = errors.New("cannot write the program's output")

// execStream runs req, copying the program's output to stdout and stderr as it
// arrives, and returns its exit status and when it arrived. Per D34 a stream
// that ends without an exit event did not see the program exit: that is an error,
// the stream's status if it ended with one. A failed write of the output ends the
// stream with an error that wraps errOutput.
func (c *cli) execStream(ctx context.Context, sc gssv1.SandboxServiceClient, req *gssv1.ExecRequest) (*gssv1.ExitStatus, time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, req.GetTimeout().AsDuration()+execSlack)
	defer cancel()
	stream, err := sc.Exec(ctx, req)
	if err != nil {
		return nil, time.Time{}, err
	}
	var exit *gssv1.ExitStatus
	var exitAt time.Time
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if exit != nil {
				break // the result is in; how the stream closed after it does not change it
			}
			return nil, time.Time{}, err
		}
		var werr error
		switch e := ev.GetEvent().(type) {
		case *gssv1.ExecEvent_Stdout:
			_, werr = c.stdout.Write(e.Stdout)
		case *gssv1.ExecEvent_Stderr:
			_, werr = c.stderr.Write(e.Stderr)
		case *gssv1.ExecEvent_Exit:
			if exit == nil {
				exit, exitAt = e.Exit, c.now()
			}
		}
		if werr != nil {
			return nil, time.Time{}, fmt.Errorf("%w: %v", errOutput, werr)
		}
	}
	if exit == nil {
		return nil, time.Time{}, errors.New("the exec stream ended without the program's exit")
	}
	return exit, exitAt, nil
}

// exitNotes says on stderr why a program ended when it did not exit by itself.
func (c *cli) exitNotes(exit *gssv1.ExitStatus, timeout time.Duration) {
	if exit.GetTimedOut() {
		fmt.Fprintf(c.stderr, "gssctl: the program timed out after %v and was killed (exit code %d)\n", timeout, exit.GetCode())
	}
	if exit.GetOomKilled() {
		fmt.Fprintf(c.stderr, "gssctl: the program ran out of memory and was killed (exit code %d)\n", exit.GetCode())
	}
}

func createSandbox(ctx context.Context, sc gssv1.SandboxServiceClient, spec *gssv1.SandboxSpec) (*gssv1.Sandbox, error) {
	ctx, cancel := context.WithTimeout(ctx, createTimeout)
	defer cancel()
	sb, err := sc.CreateSandbox(ctx, &gssv1.CreateSandboxRequest{Spec: spec})
	if err != nil {
		return nil, fmt.Errorf("create: %s", describe(err))
	}
	return sb, nil
}

// destroySandbox destroys whatever incarnation of id is live.
func destroySandbox(ctx context.Context, sc gssv1.SandboxServiceClient, id string) error {
	ctx, cancel := context.WithTimeout(ctx, destroyTimeout)
	defer cancel()
	if _, err := sc.DestroySandbox(ctx, &gssv1.DestroySandboxRequest{SandboxId: id}); err != nil {
		return fmt.Errorf("destroy %s: %s", id, describe(err))
	}
	return nil
}

func (c *cli) run(ctx context.Context, args []string) (int, error) {
	fs := c.flagSet("run")
	sf := addSpecFlags(fs)
	cf := addCmdFlags(fs)
	keep := fs.Bool("keep", false, "keep the sandbox afterwards (it lives until its TTL or gssctl destroy)")
	if err := parse(fs, args); err != nil {
		return 0, err
	}
	spec, err := sf.spec()
	if err != nil {
		return 0, err
	}
	req, err := cf.request(fs, args)
	if err != nil {
		return 0, err
	}
	conn, sc, err := c.connect()
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	sent := c.now()
	// A signal does not cancel the create (createTimeout bounds it): a sandbox it
	// makes is destroyed below, not left to its TTL.
	sb, err := createSandbox(context.WithoutCancel(ctx), sc, spec)
	if err != nil {
		return 0, err
	}
	var exit *gssv1.ExitStatus
	var exitAt time.Time
	var execErr error
	if ctx.Err() == nil {
		req.SandboxId = sb.GetId()
		exit, exitAt, execErr = c.execStream(ctx, sc, req)
	}
	fate := "kept"
	if !*keep {
		// Also after a signal or a failed write: the sandbox would otherwise live
		// until its TTL.
		fate = "destroyed"
		if err := destroySandbox(context.WithoutCancel(ctx), sc, sb.GetId()); err != nil {
			fmt.Fprintf(c.stderr, "gssctl: %v; it is removed when its TTL ends\n", err)
			fate = "not destroyed"
		}
	}
	switch {
	case errors.Is(execErr, errOutput):
		return 0, execErr
	case exit == nil && ctx.Err() != nil:
		fmt.Fprintf(c.stderr, "gssctl: interrupted (%v); sandbox %s %s\n", context.Cause(ctx), sb.GetId(), fate)
		return exitInterrupted, nil
	case execErr != nil:
		return 0, fmt.Errorf("exec in sandbox %s on node %s: %s", sb.GetId(), sb.GetNodeId(), describe(execErr))
	}
	c.exitNotes(exit, cf.timeout)
	ready := "-"
	if sb.GetStartLatency() != nil {
		ready = msString(sb.GetStartLatency().AsDuration())
	}
	fmt.Fprintf(c.stderr, "sandbox %s node=%s group=%s start=%s ready=%s total=%s\n",
		sb.GetId(), sb.GetNodeId(), sb.GetNodeGroup(), modeName(sb.GetStartedAs()), ready, msString(exitAt.Sub(sent)))
	return int(exit.GetCode()), nil
}

// msString renders d in whole milliseconds, "85ms".
func msString(d time.Duration) string {
	return fmt.Sprintf("%dms", d.Round(time.Millisecond).Milliseconds())
}

func (c *cli) create(ctx context.Context, args []string) error {
	fs := c.flagSet("create")
	sf := addSpecFlags(fs)
	if err := parse(fs, args); err != nil {
		return err
	}
	if err := noArgs(fs); err != nil {
		return err
	}
	spec, err := sf.spec()
	if err != nil {
		return err
	}
	conn, sc, err := c.connect()
	if err != nil {
		return err
	}
	defer conn.Close()
	sb, err := createSandbox(ctx, sc, spec)
	if err != nil {
		return err
	}
	fmt.Fprintln(c.stdout, sb.GetId())
	return nil
}

func (c *cli) exec(ctx context.Context, args []string) (int, error) {
	id, rest, err := sandboxID("exec", args)
	if err != nil {
		return 0, err
	}
	fs := c.flagSet("exec")
	cf := addCmdFlags(fs)
	if err := parse(fs, rest); err != nil {
		return 0, err
	}
	req, err := cf.request(fs, rest)
	if err != nil {
		return 0, err
	}
	req.SandboxId = id
	conn, sc, err := c.connect()
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	exit, _, err := c.execStream(ctx, sc, req)
	switch {
	case errors.Is(err, errOutput):
		return 0, err
	case err != nil && ctx.Err() != nil:
		fmt.Fprintf(c.stderr, "gssctl: interrupted (%v)\n", context.Cause(ctx))
		return exitInterrupted, nil
	case err != nil:
		return 0, fmt.Errorf("exec in sandbox %s: %s", id, describe(err))
	}
	c.exitNotes(exit, cf.timeout)
	return int(exit.GetCode()), nil
}

func (c *cli) destroy(ctx context.Context, args []string) error {
	id, rest, err := sandboxID("destroy", args)
	if err != nil {
		return err
	}
	fs := c.flagSet("destroy")
	if err := parse(fs, rest); err != nil {
		return err
	}
	if err := noArgs(fs); err != nil {
		return err
	}
	conn, sc, err := c.connect()
	if err != nil {
		return err
	}
	defer conn.Close()
	return destroySandbox(ctx, sc, id)
}
