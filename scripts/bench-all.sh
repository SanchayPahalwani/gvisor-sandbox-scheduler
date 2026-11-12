#!/usr/bin/env bash
# Runs every benchmark in order, each writing under results/: make sim, bench-start,
# bench-throughput, bench-density, bench-grouploss. Stops at the first failure. Linux
# only (make bench runs it inside the VM from macOS). It does not start the
# observability container; the benchmarks refuse to run while it is up (spec D25).
set -uo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo"

if [[ "$(uname -s)" != Linux ]]; then
	echo "bench-all.sh: run this on Linux (make bench does that from macOS)" >&2
	exit 1
fi

for target in sim bench-start bench-throughput bench-density bench-grouploss; do
	echo "== make $target" >&2
	if ! make "$target"; then
		echo "bench-all.sh: make $target failed; stopping. Later benchmarks were not run, and results/ may still hold output from an earlier run for them." >&2
		exit 1
	fi
done
echo "bench-all.sh: all benchmarks finished; run make results to render docs/results.md" >&2
