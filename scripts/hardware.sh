#!/usr/bin/env bash
# Captures the hardware the benchmarks ran on into results/hardware.json.
#
#   scripts/hardware.sh          write results/hardware.json
#   scripts/hardware.sh --part   print only the Linux part (an object) to stdout
#
# On macOS the file has "host" (model name, chip, core counts, memory, OS version) and
# "vm", taken by running this script with --part inside the Lima VM gss. If the VM is
# not running, "vm" is null and "vm_note" says so; the script does not start the VM.
# On Linux the file has "vm" (or "machine" when systemd-detect-virt reports no
# virtualization). Nothing that identifies the owner or the machine is recorded: no
# hostname, user name, serial number, UUID or address.
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
export PATH="/usr/local/go/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$PATH"

# json_str prints its argument as a JSON string.
json_str() {
	local s=$1
	s=${s//\\/\\\\}
	s=${s//\"/\\\"}
	s=$(printf '%s' "$s" | tr -d '\000-\037')
	printf '"%s"' "$s"
}

# linux_part prints the Linux object.
linux_part() {
	local kernel distro vcpus mem_kb runsc gov virt
	kernel=$(uname -r)
	distro=$(. /etc/os-release 2>/dev/null && printf '%s' "${PRETTY_NAME:-unknown}") || distro=unknown
	vcpus=$(nproc)
	mem_kb=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)
	if command -v runsc >/dev/null 2>&1; then
		runsc=$(runsc --version 2>/dev/null | head -n 1) || runsc="runsc: version unavailable"
	else
		runsc="runsc: not installed"
	fi
	if command -v go >/dev/null 2>&1; then
		gov=$(go version | awk '{print $3}')
	else
		gov="go: not installed"
	fi
	virt=$(systemd-detect-virt 2>/dev/null || true)
	[[ -n "$virt" ]] || virt=unknown
	cat <<JSON
{
  "kernel": $(json_str "$kernel"),
  "distro": $(json_str "$distro"),
  "vcpus": $vcpus,
  "memory_mib": $((mem_kb / 1024)),
  "runsc": $(json_str "$runsc"),
  "go": $(json_str "$gov"),
  "virtualization": $(json_str "$virt"),
  "nodes_are": "processes in one VM"
}
JSON
}

# mac_host prints the host object. Only the named fields of system_profiler are read.
mac_host() {
	local hw model chip perf eff mem os
	hw=$(system_profiler SPHardwareDataType 2>/dev/null || true)
	model=$(awk -F': ' '/^ *Model Name:/ {print $2; exit}' <<<"$hw")
	chip=$(awk -F': ' '/^ *Chip:/ {print $2; exit}' <<<"$hw")
	[[ -n "$chip" ]] || chip=$(sysctl -n machdep.cpu.brand_string)
	[[ -n "$model" ]] || model=unknown
	perf=$(sysctl -n hw.perflevel0.physicalcpu 2>/dev/null || sysctl -n hw.physicalcpu)
	eff=$(sysctl -n hw.perflevel1.physicalcpu 2>/dev/null || echo 0)
	mem=$(( $(sysctl -n hw.memsize) / 1073741824 ))
	os="$(sw_vers -productName) $(sw_vers -productVersion)"
	cat <<JSON
{
    "model_name": $(json_str "$model"),
    "chip": $(json_str "$chip"),
    "performance_cores": $perf,
    "efficiency_cores": $eff,
    "memory_gib": $mem,
    "os": $(json_str "$os")
  }
JSON
}

indent() { sed '2,$s/^/  /'; }

if [[ "${1:-}" == "--part" ]]; then
	linux_part
	exit 0
fi

out=$repo/results/hardware.json
mkdir -p "$repo/results"
now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
tmp=$(mktemp "$repo/results/.hardware.XXXXXX")
trap 'rm -f "$tmp"' EXIT

case "$(uname -s)" in
Darwin)
	vm=null
	note=""
	if ! command -v limactl >/dev/null 2>&1; then
		note="limactl not installed, so the VM part was not captured"
	elif [[ "$(limactl list --format '{{.Status}}' gss 2>/dev/null || true)" != "Running" ]]; then
		note="the Lima VM gss was not running, so the VM part was not captured"
	else
		errf=$(mktemp)
		if part=$(limactl shell --workdir "$repo" gss -- scripts/hardware.sh --part 2>"$errf") && [[ "$part" == "{"* ]]; then
			vm=$(printf '%s\n' "$part" | indent)
		else
			note="running the capture inside the VM gss failed, so the VM part was not captured"
			last=$(grep -v '^[[:space:]]*$' "$errf" | tail -n 1 || true)
			[[ -z "$last" ]] || note="$note: $last"
		fi
		rm -f "$errf"
	fi
	{
		echo "{"
		echo "  \"captured_at\": \"$now\","
		echo "  \"host\": $(mac_host),"
		if [[ "$vm" == null ]]; then
			echo "  \"vm\": null,"
			echo "  \"vm_note\": $(json_str "$note")"
		else
			echo "  \"vm\": $vm"
		fi
		echo "}"
	} >"$tmp"
	;;
Linux)
	virt=$(systemd-detect-virt 2>/dev/null || true)
	key=vm
	[[ "$virt" != none ]] || key=machine
	{
		echo "{"
		echo "  \"captured_at\": \"$now\","
		echo "  \"$key\": $(linux_part | indent)"
		echo "}"
	} >"$tmp"
	;;
*)
	echo "hardware.sh: unsupported OS $(uname -s)" >&2
	exit 1
	;;
esac

mv "$tmp" "$out"
trap - EXIT
echo "wrote $out"
