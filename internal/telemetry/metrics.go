package telemetry

import (
	"context"
	"slices"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// The gss_ metrics (spec section 9). Each accessor asks the global meter
// "gss" for its instrument on every call, so a provider installed later
// (Setup, or a test) takes effect; the SDK and the global delegate both
// hand back the instrument they already made for a name. The names carry
// Prometheus suffixes already (_seconds, _total), so the metrics store keeps
// them as they are.
const (
	meterName = "gss"

	startDurationName     = "gss_start_duration_seconds"
	execDurationName      = "gss_exec_duration_seconds"
	placementDurationName = "gss_placement_duration_seconds"
	placementFailuresName = "gss_placement_failures_total"
	reschedulesName       = "gss_reschedules_total"
	nodeDeathsName        = "gss_node_deaths_total"
	sandboxesName         = "gss_sandboxes"
	warmPoolReadyName     = "gss_warm_pool_ready"
	nodesName             = "gss_nodes"
)

// metricNames is every metric, sorted; histogramNames the histograms among
// them, which the store exports as _bucket, _count and _sum series.
var (
	metricNames = slices.Sorted(slices.Values([]string{
		startDurationName, execDurationName, placementDurationName,
		placementFailuresName, reschedulesName, nodeDeathsName,
		sandboxesName, warmPoolReadyName, nodesName,
	}))
	histogramNames = []string{startDurationName, execDurationName, placementDurationName}
)

// Attribute keys of the metrics; the dashboard groups by them.
const (
	KeyMode   attribute.Key = "mode"   // start path: cold, restore or warm
	KeyNode   attribute.Key = "node"   // node ID
	KeyKind   attribute.Key = "kind"   // Exec kind: argv or python
	KeyReason attribute.Key = "reason" // placement failure: a filter name, no_nodes or node_error
	KeyGroup  attribute.Key = "group"  // node group
	KeyState  attribute.Key = "state"  // node state: ready or dead
)

// durationBounds are the bucket boundaries of every histogram, in seconds.
var durationBounds = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

func meter() metric.Meter { return otel.Meter(meterName) }

func histogram(name, desc string) metric.Float64Histogram {
	h, err := meter().Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(durationBounds...))
	if err != nil {
		otel.Handle(err)
	}
	if h == nil {
		return noop.Float64Histogram{}
	}
	return h
}

func counter(name, desc string) metric.Int64Counter {
	c, err := meter().Int64Counter(name, metric.WithDescription(desc))
	if err != nil {
		otel.Handle(err)
	}
	if c == nil {
		return noop.Int64Counter{}
	}
	return c
}

// StartDuration is gss_start_duration_seconds {mode, node}: a node's Create,
// from the call to a ready sandbox.
func StartDuration() metric.Float64Histogram {
	return histogram(startDurationName, "Time from a node Create call to a ready sandbox, by start path")
}

// ExecDuration is gss_exec_duration_seconds {kind}: a node's Exec, from the
// call to the exit status.
func ExecDuration() metric.Float64Histogram {
	return histogram(execDurationName, "Time from a node Exec call to its exit status")
}

// PlacementDuration is gss_placement_duration_seconds: one run of the
// placement policy over the scheduler's view.
func PlacementDuration() metric.Float64Histogram {
	return histogram(placementDurationName, "Time the scheduler takes to choose a node")
}

// PlacementFailures is gss_placement_failures_total {reason}: placements
// that ended without a sandbox started.
func PlacementFailures() metric.Int64Counter {
	return counter(placementFailuresName, "Placements that ended without a started sandbox: the filter that rejected the most nodes, no_nodes or node_error")
}

// Reschedules is gss_reschedules_total: reschedules started.
func Reschedules() metric.Int64Counter {
	return counter(reschedulesName, "Reschedules started")
}

// NodeDeaths is gss_node_deaths_total: nodes declared dead.
func NodeDeaths() metric.Int64Counter {
	return counter(nodeDeathsName, "Nodes declared dead")
}

// RegisterNodeGauges registers gss_sandboxes {node} and gss_warm_pool_ready
// {node}, read from sandboxes and warm at each collection.
func RegisterNodeGauges(node string, sandboxes func() int64, warm func() int64) error {
	m := meter()
	sb, err := m.Int64ObservableGauge(sandboxesName, metric.WithDescription("Live sandboxes on the node: starting or ready"))
	if err != nil {
		return err
	}
	w, err := m.Int64ObservableGauge(warmPoolReadyName, metric.WithDescription("Warm pool sandboxes ready to be claimed"))
	if err != nil {
		return err
	}
	attrs := metric.WithAttributeSet(attribute.NewSet(KeyNode.String(node)))
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(sb, sandboxes(), attrs)
		o.ObserveInt64(w, warm(), attrs)
		return nil
	}, sb, w)
	return err
}

// RegisterClusterGauges registers gss_nodes {group, state}, read from nodes
// at each collection: the count per [group, state].
func RegisterClusterGauges(nodes func() map[[2]string]int64) error {
	_, err := meter().Int64ObservableGauge(nodesName, metric.WithDescription("Registered nodes by group and state"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			for k, n := range nodes() {
				o.Observe(n, metric.WithAttributes(KeyGroup.String(k[0]), KeyState.String(k[1])))
			}
			return nil
		}))
	return err
}
