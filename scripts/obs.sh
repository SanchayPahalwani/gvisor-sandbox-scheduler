#!/usr/bin/env bash
# Runs the observability container (spec call D24): grafana/otel-lgtm, one
# development image with an OpenTelemetry collector, Tempo, a
# Prometheus-compatible store and Grafana, as the rootless nerdctl container
# gss-obs of the VM user. Grafana is published on port 3300 (container port
# 3000, which is often taken on the host; Lima forwards guest ports to the
# same host port), OTLP on 4317 (gRPC) and 4318 (HTTP). Grafana provisions
# deploy/grafana/dashboard.json through deploy/grafana/dashboards.yaml.
#
#   scripts/obs.sh up       start it, wait until Grafana answers, print the URL
#   scripts/obs.sh down     stop and remove it
#   scripts/obs.sh status   say whether it runs (exit 1 if not)
#
# On macOS it runs itself in the VM gss, which must be running (it does not
# start the VM). Benchmarks refuse to run while the container is up (spec D25).
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
name=gss-obs
image=${GSS_OBS_IMAGE:-docker.io/grafana/otel-lgtm:0.40.0}
grafana_port=3300
health_url="http://127.0.0.1:${grafana_port}/api/health"
# The first start pulls the image before Grafana can answer.
up_timeout=300
# Where the image's Grafana reads dashboard providers, per its README.
prov=/otel-lgtm/grafana/conf/provisioning/dashboards

usage() {
	echo "usage: scripts/obs.sh up|down|status" >&2
	exit 2
}
[[ $# -eq 1 ]] || usage
case $1 in
up | down | status) ;;
*) usage ;;
esac

if [[ "$(uname -s)" == Darwin ]]; then
	if [[ "$(limactl list --format '{{.Status}}' gss 2>/dev/null)" != Running ]]; then
		echo "obs.sh: the VM gss is not running (make vm starts it)" >&2
		exit 1
	fi
	exec limactl shell --workdir "$repo" gss -- env GSS_OBS_IMAGE="$image" "$repo/scripts/obs.sh" "$1"
fi

if [[ $EUID -eq 0 ]]; then
	echo "obs.sh: run this as the VM user, not root: gss-obs is a rootless nerdctl container" >&2
	exit 1
fi
if ! command -v nerdctl >/dev/null 2>&1; then
	echo "obs.sh: nerdctl not found; run this in the VM gss (lima/gss.yaml installs rootless containerd)" >&2
	exit 1
fi

# The container list is read into a variable first: under pipefail,
# `nerdctl ps | grep -q` can fail when grep exits early and nerdctl dies of
# SIGPIPE.
names() { nerdctl ps "$@" --format '{{.Names}}' 2>/dev/null || true; }
running() { grep -qx "$name" <<<"$(names)"; }
present() { grep -qx "$name" <<<"$(names -a)"; }

case $1 in
up)
	if running; then
		echo "obs.sh: $name is already running" >&2
	else
		if present; then
			nerdctl rm -f "$name" >/dev/null
		fi
		nerdctl run -d --name "$name" \
			-p "${grafana_port}:3000" -p 4317:4317 -p 4318:4318 \
			-e GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH="$prov/gss/dashboard.json" \
			-v "$repo/deploy/grafana/dashboards.yaml:$prov/gss.yaml:ro" \
			-v "$repo/deploy/grafana/dashboard.json:$prov/gss/dashboard.json:ro" \
			"$image" >/dev/null
	fi
	deadline=$((SECONDS + up_timeout))
	until curl -fsS -o /dev/null "$health_url" 2>/dev/null; do
		if ((SECONDS >= deadline)); then
			echo "obs.sh: Grafana did not answer at $health_url within ${up_timeout}s (nerdctl logs $name)" >&2
			exit 1
		fi
		sleep 1
	done
	echo "Grafana: http://127.0.0.1:${grafana_port} (dashboard \"gVisor sandbox scheduler\")"
	echo "Send telemetry to it with:"
	echo "export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317"
	;;
down)
	if present; then
		nerdctl rm -f "$name" >/dev/null
		echo "obs.sh: $name removed" >&2
	else
		echo "obs.sh: $name is not running" >&2
	fi
	;;
status)
	if running; then
		echo "$name is running: Grafana http://127.0.0.1:${grafana_port}, OTLP 127.0.0.1:4317 (gRPC) and 4318 (HTTP)"
	else
		echo "$name is not running"
		exit 1
	fi
	;;
esac
