#!/usr/bin/env bash
# Captures the hardware (make hardware), then runs every benchmark in order, each
# writing under results/: make sim, bench-start, bench-throughput, bench-density,
# bench-grouploss. Stops at the first failure. Linux only (make bench runs it inside the
# VM from macOS). From macOS, make bench captures the hardware on the host first, with
# the host's part as well as the VM's, and sets GSS_HARDWARE_CAPTURED=1 so that it is not
# captured again here with the VM part only. It does not start the observability
# container; the benchmarks refuse to run while it is up (spec D25).
set -uo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo"

if [[ "$(uname -s)" != Linux ]]; then
	echo "bench-all.sh: run this on Linux (make bench does that from macOS)" >&2
	exit 1
fi

targets=(sim bench-start bench-throughput bench-density bench-grouploss)
if [[ "${GSS_HARDWARE_CAPTURED:-0}" != 1 ]]; then
	targets=(hardware "${targets[@]}")
fi
for target in "${targets[@]}"; do
	echo "== make $target" >&2
	if ! make "$target"; then
		echo "bench-all.sh: make $target failed; stopping. Later benchmarks were not run, and results/ may still hold output from an earlier run for them." >&2
		exit 1
	fi
done
echo "bench-all.sh: all benchmarks finished; run make results to render docs/results.md" >&2
