package gateway

import (
	"context"
	"fmt"
	"strings"
	"testing"

	gssv1 "github.com/SanchayPahalwani/gvisor-sandbox-scheduler/api/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func attrKey(s attribute.Set) string {
	var kvs []string
	for _, kv := range s.ToSlice() { // sorted by key
		kvs = append(kvs, string(kv.Key)+"="+kv.Value.Emit())
	}
	return strings.Join(kvs, ",")
}

// points returns the gss_ metrics by name and then by attributes ("k=v,k=v"):
// a counter's value, a histogram's count.
func points(t *testing.T, r *sdkmetric.ManualReader) map[string]map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if !strings.HasPrefix(m.Name, "gss_") {
				continue // otelgrpc's
			}
			byAttrs := map[string]int64{}
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					byAttrs[attrKey(dp.Attributes)] = dp.Value
				}
			case metricdata.Histogram[float64]:
				for _, dp := range d.DataPoints {
					byAttrs[attrKey(dp.Attributes)] = int64(dp.Count)
				}
			}
			out[m.Name] = byAttrs
		}
	}
	return out
}

func TestPlacementMetrics(t *testing.T) {
	r := recordMetrics(t)
	e := newEnv(t, nil)
	_, err := e.create(nil)
	wantCode(t, err, codes.ResourceExhausted, "no nodes")
	e.register("n1")
	e.register("n2")
	e.mustCreate(nil)
	_, err = e.create(spec(64000, 256*mib))
	wantCode(t, err, codes.ResourceExhausted, "64 CPUs")
	_, err = e.create(&gssv1.SandboxSpec{NodeGroup: "zz"})
	wantCode(t, err, codes.ResourceExhausted, "group zz")
	// Both nodes fail their Create: two placements, then a third that finds
	// every node excluded.
	e.nodes.setCreate(func(ctx context.Context, a string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
		return nil, status.Error(codes.Internal, "broken")
	})
	_, err = e.create(nil)
	wantCode(t, err, codes.Unavailable, "every node failing")
	// A forced mode the node cannot serve ends the placement at once.
	e.nodes.setCreate(func(ctx context.Context, a string, req *gssv1.NodeCreateRequest) (*gssv1.NodeCreateResponse, error) {
		return nil, status.Error(codes.FailedPrecondition, "warm pool empty")
	})
	_, err = e.create(&gssv1.SandboxSpec{StartMode: gssv1.StartMode_START_MODE_WARM})
	wantCode(t, err, codes.FailedPrecondition, "forced warm")

	got := points(t, r)
	wantFailures := map[string]int64{"reason=no_nodes": 1, "reason=Capacity": 1, "reason=NodeGroup": 1, "reason=node_error": 2}
	if fmt.Sprint(got["gss_placement_failures_total"]) != fmt.Sprint(wantFailures) {
		t.Errorf("gss_placement_failures_total = %v, want %v", got["gss_placement_failures_total"], wantFailures)
	}
	// One per policy run: 1 + 1 + 1 + 1 + 3 + 1.
	if n := got["gss_placement_duration_seconds"][""]; n != 8 {
		t.Errorf("gss_placement_duration_seconds has %d points, want 8", n)
	}
}

func TestRescheduleMetrics(t *testing.T) {
	r := recordMetrics(t)
	e := newEnv(t, nil).run()
	e.register("n1")
	e.register("n2")
	a, b := e.mustCreate(nil), e.mustCreate(nil)
	// Run starts both counters at 0, so the first death shows as a step.
	eventually(t, "counters at 0", func() bool {
		got := points(t, r)
		d, okd := got["gss_node_deaths_total"][""]
		s, oks := got["gss_reschedules_total"][""]
		return okd && oks && d == 0 && s == 0
	})
	if got, want := fmt.Sprint(e.srv.NodeCounts()), fmt.Sprint(map[[2]string]int64{{"g1", "ready"}: 2, {"g1", "dead"}: 0}); got != want {
		t.Errorf("NodeCounts = %s, want %s", got, want)
	}

	e.kill("n2")
	for _, sb := range []*gssv1.Sandbox{a, b} {
		eventually(t, sb.GetId()+" READY on n2", func() bool {
			got := e.get(sb.GetId())
			return got.GetState() == gssv1.SandboxState_SANDBOX_STATE_READY && got.GetNodeId() == "n2"
		})
	}
	got := points(t, r)
	if d, s := got["gss_node_deaths_total"][""], got["gss_reschedules_total"][""]; d != 1 || s != 2 {
		t.Errorf("node deaths %d, reschedules %d; want 1 and 2", d, s)
	}
	if got, want := fmt.Sprint(e.srv.NodeCounts()), fmt.Sprint(map[[2]string]int64{{"g1", "ready"}: 1, {"g1", "dead"}: 1}); got != want {
		t.Errorf("NodeCounts = %s, want %s", got, want)
	}
}
