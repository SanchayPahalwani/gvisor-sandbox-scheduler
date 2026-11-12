package nodeagent

import (
	"context"

	"github.com/SanchayPahalwani/gvisor-sandbox-scheduler/internal/runsc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	// The global tracer is looked up on each use so a provider set later
	// (internal/telemetry, or a test) takes effect.
	return otel.Tracer("gss").Start(ctx, name, trace.WithAttributes(attrs...))
}

// endSpan records err, if any, on the span and ends it.
func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
	}
	span.End()
}

// tracedRuntime wraps every Runtime call in a span named runsc.<method>.
type tracedRuntime struct{ rt runsc.Runtime }

var _ runsc.Runtime = tracedRuntime{}

func refSpan(ctx context.Context, method string, ref runsc.Ref) (context.Context, trace.Span) {
	return startSpan(ctx, "runsc."+method, attribute.String("runsc.id", ref.ID))
}

func (t tracedRuntime) Create(ctx context.Context, ref runsc.Ref, bundleDir, logPath string) error {
	ctx, span := refSpan(ctx, "create", ref)
	err := t.rt.Create(ctx, ref, bundleDir, logPath)
	endSpan(span, err)
	return err
}

func (t tracedRuntime) Start(ctx context.Context, ref runsc.Ref) error {
	ctx, span := refSpan(ctx, "start", ref)
	err := t.rt.Start(ctx, ref)
	endSpan(span, err)
	return err
}

func (t tracedRuntime) Restore(ctx context.Context, ref runsc.Ref, bundleDir, imagePath string) error {
	ctx, span := refSpan(ctx, "restore", ref)
	err := t.rt.Restore(ctx, ref, bundleDir, imagePath)
	endSpan(span, err)
	return err
}

func (t tracedRuntime) Checkpoint(ctx context.Context, ref runsc.Ref, imagePath string) error {
	ctx, span := refSpan(ctx, "checkpoint", ref)
	err := t.rt.Checkpoint(ctx, ref, imagePath)
	endSpan(span, err)
	return err
}

func (t tracedRuntime) Exec(ctx context.Context, ref runsc.Ref, spec runsc.ExecSpec) (int, error) {
	ctx, span := refSpan(ctx, "exec", ref)
	code, err := t.rt.Exec(ctx, ref, spec)
	span.SetAttributes(attribute.Int("exec.exit_code", code))
	endSpan(span, err)
	return code, err
}

func (t tracedRuntime) KillProcess(ctx context.Context, ref runsc.Ref, internalPID int) error {
	ctx, span := refSpan(ctx, "kill_process", ref)
	err := t.rt.KillProcess(ctx, ref, internalPID)
	endSpan(span, err)
	return err
}

func (t tracedRuntime) Delete(ctx context.Context, ref runsc.Ref) error {
	ctx, span := refSpan(ctx, "delete", ref)
	err := t.rt.Delete(ctx, ref)
	endSpan(span, err)
	return err
}

func (t tracedRuntime) State(ctx context.Context, ref runsc.Ref) (runsc.State, error) {
	ctx, span := refSpan(ctx, "state", ref)
	st, err := t.rt.State(ctx, ref)
	endSpan(span, err)
	return st, err
}

func (t tracedRuntime) List(ctx context.Context) ([]string, error) {
	ctx, span := startSpan(ctx, "runsc.list")
	ids, err := t.rt.List(ctx)
	endSpan(span, err)
	return ids, err
}

func (t tracedRuntime) Version(ctx context.Context) (string, error) {
	ctx, span := startSpan(ctx, "runsc.version")
	v, err := t.rt.Version(ctx)
	endSpan(span, err)
	return v, err
}

func (t tracedRuntime) RemoveRoot(ctx context.Context) error {
	ctx, span := startSpan(ctx, "runsc.remove_root")
	err := t.rt.RemoveRoot(ctx)
	endSpan(span, err)
	return err
}
