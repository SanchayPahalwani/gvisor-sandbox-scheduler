// Package telemetry sets up OpenTelemetry for every gss binary and holds the
// gss_ metrics (spec section 9). It is the only package that installs SDK
// providers; everything else uses the global tracer and meter "gss".
package telemetry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// endpointEnv turns export on (spec D25: benchmarks run with it unset). The
// exporters read it themselves: an http:// endpoint is plaintext gRPC.
const endpointEnv = "OTEL_EXPORTER_OTLP_ENDPOINT"

// metricInterval is how often the metrics are exported.
const metricInterval = 5 * time.Second

// Setup installs global tracer and meter providers that export OTLP over gRPC
// when OTEL_EXPORTER_OTLP_ENDPOINT is set, and does nothing otherwise.
// attrs are added to the resource (for example node.id, node.group).
//
// With export on, spans go through a batch span processor with a
// parent-based always-on sampler, metrics through a periodic reader every
// 5 s, and the propagator is W3C trace context plus baggage. The resource has
// service.name, a random service.instance.id (attrs may override it) and
// attrs. shutdown flushes and stops both providers; with export off it does
// nothing and returns nil.
func Setup(ctx context.Context, service string, attrs ...attribute.KeyValue) (shutdown func(context.Context) error, err error) {
	if os.Getenv(endpointEnv) == "" {
		return func(context.Context) error { return nil }, nil
	}
	res, err := newResource(ctx, service, attrs)
	if err != nil {
		return nil, fmt.Errorf("telemetry: resource: %w", err)
	}
	texp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("telemetry: trace exporter: %w", err)
	}
	mexp, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("telemetry: metric exporter: %w", err), texp.Shutdown(ctx))
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(texp),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
		sdktrace.WithResource(res),
	)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(mexp, sdkmetric.WithInterval(metricInterval))),
		sdkmetric.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx))
	}, nil
}

// newResource describes the process: the SDK, service.name, a random
// service.instance.id, then attrs. The metrics store keys series on the
// instance, so processes of one service (the node agents) do not mix.
func newResource(ctx context.Context, service string, attrs []attribute.KeyValue) (*resource.Resource, error) {
	var b [8]byte
	rand.Read(b[:]) // crypto/rand.Read never returns an error
	kvs := append([]attribute.KeyValue{
		semconv.ServiceName(service),
		semconv.ServiceInstanceID(hex.EncodeToString(b[:])),
	}, attrs...)
	return resource.New(ctx, resource.WithTelemetrySDK(), resource.WithAttributes(kvs...))
}
