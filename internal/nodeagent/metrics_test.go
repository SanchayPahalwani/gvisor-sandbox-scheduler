package nodeagent

import (
	"context"
	"strings"
	"testing"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
)

// recordMetrics makes a manual reader's meter provider the global one for
// the test.
func recordMetrics(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	r := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = mp.Shutdown(context.Background())
	})
	return r
}

type histPoint struct {
	count uint64
	sum   float64
}

// histogram returns the data points of histogram name by their attributes
// ("k=v,k=v").
func histogram(t *testing.T, r *sdkmetric.ManualReader, name string) map[string]histPoint {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]histPoint{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if h, ok := m.Data.(metricdata.Histogram[float64]); ok && m.Name == name {
				for _, dp := range h.DataPoints {
					out[attrKey(dp.Attributes)] = histPoint{dp.Count, dp.Sum}
				}
			}
		}
	}
	return out
}

func attrKey(s attribute.Set) string {
	var kvs []string
	for _, kv := range s.ToSlice() { // sorted by key
		kvs = append(kvs, string(kv.Key)+"="+kv.Value.Emit())
	}
	return strings.Join(kvs, ",")
}

func TestStartDurationMetric(t *testing.T) {
	r := recordMetrics(t)
	e := poolEnv(t, 1, nil)
	e.waitWarm(t, 1)
	warm := e.mustCreate(t, "sb-1", 1, nil)
	cold := e.mustCreate(t, "sb-2", 1, coldSpec)
	re, _ := restoreEnv(t, nil)
	restored := re.mustCreate(t, "sb-3", 1, nil)
	// A Create that fails is not measured.
	_, err := e.create("sb-4", 1, spec(64000, 256<<20))
	wantCode(t, err, codes.ResourceExhausted, "64 CPUs")

	got := histogram(t, r, "gss_start_duration_seconds")
	want := map[string]*gssv1.NodeCreateResponse{
		"mode=warm,node=n1": warm, "mode=cold,node=n1": cold, "mode=restore,node=n1": restored,
	}
	if len(got) != len(want) {
		t.Errorf("points = %v, want one per mode %v", got, want)
	}
	for attrs, resp := range want {
		p := got[attrs]
		if p.count != 1 || p.sum != resp.GetReadyLatency().AsDuration().Seconds() {
			t.Errorf("%s: %d points summing to %v s, want one of the ready_latency %v", attrs, p.count, p.sum, resp.GetReadyLatency().AsDuration())
		}
	}
}

func TestExecDurationMetric(t *testing.T) {
	r := recordMetrics(t)
	e := readyEnv(t, nil) // the readiness probe is no Exec call
	for _, req := range []*gssv1.ExecRequest{argvReq("sb-1", "true"), pyReq("sb-1", "print(1)")} {
		var c collector
		if err := e.m.Exec(context.Background(), req, c.send); err != nil {
			t.Fatal(err)
		}
	}
	// An Exec that ends without an exit status is not measured.
	var c collector
	wantCode(t, e.m.Exec(context.Background(), argvReq("sb-9", "true"), c.send), codes.NotFound, "Exec in an unknown sandbox")

	got := histogram(t, r, "gss_exec_duration_seconds")
	if len(got) != 2 || got["kind=argv"].count != 1 || got["kind=python"].count != 1 {
		t.Errorf("points = %v, want one for kind=argv and one for kind=python", got)
	}
}

func TestLive(t *testing.T) {
	e := poolEnv(t, 1, nil)
	e.waitWarm(t, 1)
	if n := e.m.Live(); n != 0 {
		t.Errorf("Live = %d with only a pool sandbox, want 0", n)
	}
	e.mustCreate(t, "sb-1", 1, coldSpec)
	e.mustCreate(t, "sb-2", 1, nil) // claims the pool sandbox
	if n := e.m.Live(); n != 2 {
		t.Errorf("Live = %d, want 2", n)
	}
	if err := e.m.Destroy(context.Background(), "sb-1", 0); err != nil {
		t.Fatal(err)
	}
	if n := e.m.Live(); n != 1 {
		t.Errorf("Live = %d after a destroy, want 1", n)
	}
}
