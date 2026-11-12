#!/usr/bin/env bash
# Local failure-domain test (spec 8.6): kills every node of group b under a steady
# workload, once without client retries and once with them, each on a fresh default
# four-node cluster. Both groups are processes in one VM on one laptop. Linux only;
# make bench-grouploss runs it (from macOS inside the VM) after make build.
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cluster=scripts/cluster.sh
loadgen=bin/loadgen
# Spec D35: under bin-packing all sessions fit on one node, so the scheduler runs with
# the spread policy and the sessions land on both groups. loadgen records it.
policy=spread

if [[ "$(uname -s)" != Linux ]]; then
	echo "group-loss.sh: run this on Linux (make bench-grouploss does that from macOS)" >&2
	exit 1
fi
if [[ -n "${OTEL_EXPORTER_OTLP_ENDPOINT:-}" ]]; then
	echo "group-loss.sh: OTEL_EXPORTER_OTLP_ENDPOINT is set; benchmarks run with observability off (spec call D25): unset it" >&2
	exit 1
fi
if command -v nerdctl >/dev/null 2>&1 && nerdctl ps --format '{{.Names}}' 2>/dev/null | grep -qx gss-obs; then
	echo "group-loss.sh: the gss-obs observability container is running; benchmarks run with observability off (spec call D25): make obs-down" >&2
	exit 1
fi

cd "$repo"
if [[ ! -x "$loadgen" ]]; then
	echo "group-loss.sh: $loadgen not found: run make build first" >&2
	exit 1
fi
# The test runs on the default topology (two groups, a and b, two nodes each).
unset GSS_NODES GSS_NODE_CPU GSS_NODE_MEM GSS_NODE_MAX GSS_POOL

# On failure, do not leave the cluster running.
finish() {
	local status=$?
	if [[ $status -ne 0 ]]; then
		"$cluster" down || true
	fi
}
trap finish EXIT

# Summaries go to stdout and are printed together at the end; progress and the
# cluster script's output go to stderr as they happen.
"$cluster" down
GSS_SCHED_ARGS="--policy $policy" "$cluster" up
without_retry=$("$loadgen" grouploss --retry=false --sched-policy "$policy" --out results/grouploss.json)
"$cluster" down
GSS_SCHED_ARGS="--policy $policy" "$cluster" up
with_retry=$("$loadgen" grouploss --retry=true --sched-policy "$policy" --out results/grouploss-retry.json)
"$cluster" down

printf '%s\n\n%s\n' "$without_retry" "$with_retry"
