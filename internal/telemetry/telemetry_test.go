package telemetry

import (
	"context"
	"fmt"
	"maps"
	"net"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
)

func TestSetupNoEndpointIsNoop(t *testing.T) {
	t.Setenv(endpointEnv, "") // restored after the test
	os.Unsetenv(endpointEnv)
	// Propagators are compared by their fields: a composite one is a slice,
	// which == cannot compare.
	tp, mp, fields := otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetTextMapPropagator().Fields()

	shutdown, err := Setup(context.Background(), "gss-test", attribute.String("node.id", "n1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("shutdown = %v, want nil", err)
	}
	if otel.GetTracerProvider() != tp || otel.GetMeterProvider() != mp {
		t.Error("Setup replaced a global provider with no endpoint set")
	}
	if got := otel.GetTextMapPropagator().Fields(); !slices.Equal(got, fields) {
		t.Errorf("propagator fields %q, were %q: Setup set a propagator with no endpoint set", got, fields)
	}
	_, span := otel.Tracer("gss").Start(context.Background(), "test.span")
	defer span.End()
	if span.IsRecording() {
		t.Error("a span is recording with no endpoint set")
	}
}

// collector is an in-process OTLP receiver for traces and metrics.
type collector struct {
	mu      sync.Mutex
	spans   []*coltracepb.ExportTraceServiceRequest
	metrics []*colmetricpb.ExportMetricsServiceRequest
}

type traceService struct {
	coltracepb.UnimplementedTraceServiceServer
	c *collector
}

func (s traceService) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	s.c.spans = append(s.c.spans, req)
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

type metricsService struct {
	colmetricpb.UnimplementedMetricsServiceServer
	c *collector
}

func (s metricsService) Export(ctx context.Context, req *colmetricpb.ExportMetricsServiceRequest) (*colmetricpb.ExportMetricsServiceResponse, error) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	s.c.metrics = append(s.c.metrics, req)
	return &colmetricpb.ExportMetricsServiceResponse{}, nil
}

// startCollector serves a collector on a loopback port until the test ends
// and returns it with its address.
func startCollector(t *testing.T) (*collector, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c := &collector{}
	srv := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(srv, traceService{c: c})
	colmetricpb.RegisterMetricsServiceServer(srv, metricsService{c: c})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return c, lis.Addr().String()
}

// span returns the resource of the first exported span called name.
func (c *collector) span(name string) (*resourcepb.Resource, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, req := range c.spans {
		for _, rs := range req.GetResourceSpans() {
			for _, ss := range rs.GetScopeSpans() {
				for _, s := range ss.GetSpans() {
					if s.GetName() == name {
						return rs.GetResource(), true
					}
				}
			}
		}
	}
	return nil, false
}

// histogramPoint returns the resource of the first exported histogram called
// name that has a data point.
func (c *collector) histogramPoint(name string) (*resourcepb.Resource, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, req := range c.metrics {
		for _, rm := range req.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					if m.GetName() == name && len(m.GetHistogram().GetDataPoints()) > 0 {
						return rm.GetResource(), true
					}
				}
			}
		}
	}
	return nil, false
}

func resourceAttr(r *resourcepb.Resource, key string) string {
	for _, kv := range r.GetAttributes() {
		if kv.GetKey() == key {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}

func checkResource(t *testing.T, what string, r *resourcepb.Resource) {
	t.Helper()
	if got := resourceAttr(r, "service.name"); got != "gss-test" {
		t.Errorf("%s: service.name = %q, want gss-test", what, got)
	}
	if got := resourceAttr(r, "node.id"); got != "n1" {
		t.Errorf("%s: node.id = %q, want n1", what, got)
	}
	if got := resourceAttr(r, "service.instance.id"); got == "" {
		t.Errorf("%s: no service.instance.id in %v", what, r.GetAttributes())
	}
}

// restoreGlobals puts the global providers and propagator back when the
// test ends.
func restoreGlobals(t *testing.T) {
	tp, mp, prop := otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(tp)
		otel.SetMeterProvider(mp)
		otel.SetTextMapPropagator(prop)
	})
}

func TestSetupWithEndpoint(t *testing.T) {
	c, addr := startCollector(t)
	t.Setenv(endpointEnv, "http://"+addr)
	restoreGlobals(t)

	ctx := context.Background()
	shutdown, err := Setup(ctx, "gss-test", attribute.String("node.id", "n1"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdown(sctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})

	_, span := otel.Tracer("gss").Start(ctx, "test.span")
	if !span.IsRecording() || !span.SpanContext().IsSampled() {
		t.Error("a root span is not recorded and sampled")
	}
	span.End()
	// Parent-based sampling: a child of a remote parent that was not sampled
	// is not sampled either.
	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{1}, Remote: true,
	})
	_, child := otel.Tracer("gss").Start(trace.ContextWithRemoteSpanContext(ctx, parent), "test.child")
	if child.SpanContext().IsSampled() {
		t.Error("the child of an unsampled remote parent is sampled")
	}
	child.End()
	StartDuration().Record(ctx, 0.2, metric.WithAttributes(KeyMode.String("cold"), KeyNode.String("n1")))

	fields := otel.GetTextMapPropagator().Fields()
	for _, f := range []string{"traceparent", "baggage"} {
		if !slices.Contains(fields, f) {
			t.Errorf("propagator fields %q lack %q", fields, f)
		}
	}

	// Neither is flushed by hand: the batch span processor and the 5 s
	// periodic reader send them.
	deadline := time.Now().Add(10 * time.Second)
	var spanRes, metricRes *resourcepb.Resource
	for spanRes == nil || metricRes == nil {
		if time.Now().After(deadline) {
			t.Fatalf("after 10 s: span arrived %v, histogram point arrived %v", spanRes != nil, metricRes != nil)
		}
		time.Sleep(50 * time.Millisecond)
		if r, ok := c.span("test.span"); ok {
			spanRes = r
		}
		if r, ok := c.histogramPoint(startDurationName); ok {
			metricRes = r
		}
	}
	checkResource(t, "span", spanRes)
	checkResource(t, "metric", metricRes)
	if _, ok := c.span("test.child"); ok {
		t.Error("the unsampled child span was exported")
	}
}

// point is one data point: its attributes as "k=v,k=v" (sorted) and value.
type point struct {
	attrs string
	value float64
}

func attrString(set attribute.Set) string {
	var kvs []string
	for _, kv := range set.ToSlice() {
		kvs = append(kvs, string(kv.Key)+"="+kv.Value.Emit())
	}
	sort.Strings(kvs)
	return strings.Join(kvs, ",")
}

func TestInstrumentNames(t *testing.T) {
	restoreGlobals(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	ctx := context.Background()
	StartDuration().Record(ctx, 0.03, metric.WithAttributes(KeyMode.String("warm"), KeyNode.String("a1")))
	ExecDuration().Record(ctx, 0.1, metric.WithAttributes(KeyKind.String("python")))
	PlacementDuration().Record(ctx, 0.001)
	PlacementFailures().Add(ctx, 1, metric.WithAttributes(KeyReason.String("Capacity")))
	Reschedules().Add(ctx, 2)
	NodeDeaths().Add(ctx, 1)
	if err := RegisterNodeGauges("a1", func() int64 { return 3 }, func() int64 { return 2 }); err != nil {
		t.Fatal(err)
	}
	if err := RegisterClusterGauges(func() map[[2]string]int64 {
		return map[[2]string]int64{{"a", "ready"}: 2, {"b", "dead"}: 1}
	}); err != nil {
		t.Fatal(err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string][]point{}
	bounds := map[string][]float64{}
	for _, sm := range rm.ScopeMetrics {
		if sm.Scope.Name != "gss" {
			t.Errorf("scope %q, want gss", sm.Scope.Name)
		}
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Histogram[float64]:
				for _, dp := range d.DataPoints {
					got[m.Name] = append(got[m.Name], point{attrString(dp.Attributes), dp.Sum})
					bounds[m.Name] = dp.Bounds
				}
			case metricdata.Sum[int64]:
				if !d.IsMonotonic {
					t.Errorf("%s is not monotonic", m.Name)
				}
				for _, dp := range d.DataPoints {
					got[m.Name] = append(got[m.Name], point{attrString(dp.Attributes), float64(dp.Value)})
				}
			case metricdata.Gauge[int64]:
				for _, dp := range d.DataPoints {
					got[m.Name] = append(got[m.Name], point{attrString(dp.Attributes), float64(dp.Value)})
				}
			default:
				t.Errorf("%s: unexpected data %T", m.Name, m.Data)
			}
		}
	}
	want := map[string][]point{
		"gss_start_duration_seconds":     {{"mode=warm,node=a1", 0.03}},
		"gss_exec_duration_seconds":      {{"kind=python", 0.1}},
		"gss_placement_duration_seconds": {{"", 0.001}},
		"gss_placement_failures_total":   {{"reason=Capacity", 1}},
		"gss_reschedules_total":          {{"", 2}},
		"gss_node_deaths_total":          {{"", 1}},
		"gss_sandboxes":                  {{"node=a1", 3}},
		"gss_warm_pool_ready":            {{"node=a1", 2}},
		"gss_nodes":                      {{"group=a,state=ready", 2}, {"group=b,state=dead", 1}},
	}
	for name, pts := range got {
		sort.Slice(pts, func(i, j int) bool { return pts[i].attrs < pts[j].attrs })
		got[name] = pts
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("metrics =\n%v\nwant\n%v", got, want)
	}
	wantBounds := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	for _, name := range []string{startDurationName, execDurationName, placementDurationName} {
		if !slices.Equal(bounds[name], wantBounds) {
			t.Errorf("%s bounds = %v, want %v", name, bounds[name], wantBounds)
		}
	}
	if names := slices.Sorted(maps.Keys(want)); !slices.Equal(metricNames, names) {
		t.Errorf("metricNames = %q, want %q", metricNames, names)
	}
}
