# One Makefile for macOS and Linux. On macOS, targets in LINUX_TARGETS re-run
# themselves inside the Lima VM "gss". Add new Linux-only targets to LINUX_TARGETS.
SHELL := /bin/bash
UNAME := $(shell uname -s)

PKG ?= ./...
RUN ?= .
# Variables forwarded into the VM invocation. Append new ones here. The GSS_* ones are
# the cluster topology read by scripts/cluster.sh (an empty one means its default);
# OTEL_EXPORTER_OTLP_ENDPOINT turns on OTLP export in the units it starts (spec 9),
# and the benchmark targets refuse to run with it set (spec D25).
FORWARD_VARS := PKG RUN FORCE GSS_NODES GSS_NODE_CPU GSS_NODE_MEM GSS_NODE_MAX GSS_POOL GSS_SCHED_ARGS OTEL_EXPORTER_OTLP_ENDPOINT
FORWARD := $(foreach v,$(FORWARD_VARS),$(v)='$(value $(v))')

LINUX_TARGETS := build test-integration rootfs bench bench-start bench-throughput bench-density bench-grouploss up down status demo

.PHONY: vm vm-stop vm-delete proto test lint sim hardware results $(LINUX_TARGETS)

ifeq ($(UNAME),Darwin)
$(LINUX_TARGETS):
	limactl shell --workdir $(CURDIR) gss -- make $@ $(FORWARD)

# The demo starts the VM first if it is not running.
demo: vm

# make bench captures the hardware on the host first (the host part and, through the
# VM, the VM part) and tells scripts/bench-all.sh in the VM not to capture it again.
bench: hardware
bench: FORWARD += GSS_HARDWARE_CAPTURED=1

vm:
	@if ! limactl list -q 2>/dev/null | grep -qx gss; then \
		limactl start --name=gss --tty=false --set '.mounts=[{"location":"$(CURDIR)","writable":true}]' lima/gss.yaml; \
	elif [ "$$(limactl list --format '{{.Status}}' gss)" != "Running" ]; then \
		limactl start gss; \
	else \
		echo "VM gss is running"; \
	fi

vm-stop:
	limactl stop gss

vm-delete:
	limactl delete -f gss
else
export PATH := /usr/local/go/bin:$(PATH)

build:
	@mkdir -p bin
ifneq ($(wildcard cmd/*/),)
	CGO_ENABLED=0 go build -o bin/ ./cmd/...
else
	@echo "no cmd/ packages yet"
endif

test-integration:
	sudo env PATH=$$PATH GOCACHE=/var/cache/gss/go-build GOMODCACHE=/var/cache/gss/go-mod go test -tags integration -count=1 -p 1 -timeout 20m $(value PKG) -run '$(value RUN)'

rootfs:
	scripts/build-rootfs.sh

# Local cluster (spec section 11): one scheduler and one node agent per entry of
# GSS_NODES, as transient systemd units. scripts/cluster.sh lists its settings.
# up returns once every node is READY with its warm pool full.
up: build
	scripts/cluster.sh up

down:
	scripts/cluster.sh down

status:
	scripts/cluster.sh status

# Builds, builds the sandbox rootfs if it is missing or stale, brings the cluster up
# (four nodes in groups a and b by default) and runs the two examples through gssctl.
# Each run prints the program's output, then the node, the start path and the latency.
demo: build rootfs
	scripts/cluster.sh up
	@echo
	bin/gssctl run -f examples/hello.py
	@echo
	bin/gssctl run -f examples/pandas_demo.py
	@echo
	@echo "The cluster is still running: make status shows it, make down stops it."

# Benchmarks (spec section 10). Each one starts from a fresh cluster so runs do not
# share state, and refuses to run with OTLP export on or the gss-obs observability
# container running (spec call D25). start and throughput use the default four nodes;
# density uses one large node with no warm pool.
BENCH_NO_OTEL = if [ -n "$${OTEL_EXPORTER_OTLP_ENDPOINT:-}" ]; then \
	echo "OTEL_EXPORTER_OTLP_ENDPOINT is set; benchmarks run with observability off (spec call D25): unset it" >&2; exit 1; fi; \
	if command -v nerdctl >/dev/null 2>&1 && nerdctl ps --format '{{.Names}}' 2>/dev/null | grep -qx gss-obs; then \
	echo "the gss-obs observability container is running; benchmarks run with observability off (spec call D25): make obs-down" >&2; exit 1; fi
# GSS_SCHED_ARGS is cleared too: these benchmarks use the default placement policy
# (only the group-loss test runs spread, spec D35).
DEFAULT_TOPOLOGY := env -u GSS_NODES -u GSS_NODE_CPU -u GSS_NODE_MEM -u GSS_NODE_MAX -u GSS_POOL -u GSS_SCHED_ARGS
DENSITY_TOPOLOGY := env -u GSS_SCHED_ARGS GSS_NODES="a1:a:7101" GSS_NODE_CPU=64000 GSS_NODE_MEM=65536 GSS_NODE_MAX=200 GSS_POOL=0

bench-start: build
	@$(BENCH_NO_OTEL)
	$(DEFAULT_TOPOLOGY) scripts/cluster.sh down
	$(DEFAULT_TOPOLOGY) scripts/cluster.sh up
	bin/loadgen start --out results/start.json
	$(DEFAULT_TOPOLOGY) scripts/cluster.sh down

bench-throughput: build
	@$(BENCH_NO_OTEL)
	$(DEFAULT_TOPOLOGY) scripts/cluster.sh down
	$(DEFAULT_TOPOLOGY) scripts/cluster.sh up
	bin/loadgen throughput --out results/throughput.json
	$(DEFAULT_TOPOLOGY) scripts/cluster.sh down

bench-density: build
	@$(BENCH_NO_OTEL)
	$(DENSITY_TOPOLOGY) scripts/cluster.sh down
	$(DENSITY_TOPOLOGY) scripts/cluster.sh up
	bin/loadgen density --out results/density.json
	$(DENSITY_TOPOLOGY) scripts/cluster.sh down

# The hardware capture, then every benchmark in order, stopping at the first failure;
# does not start the observability container.
bench:
	scripts/bench-all.sh

# Local failure-domain test (spec 8.6); the script brings its own clusters up and down.
bench-grouploss: build
	@$(BENCH_NO_OTEL)
	scripts/group-loss.sh

vm vm-stop vm-delete:
	@echo "not needed on Linux"
endif

# The protoc plugins are built from the versions pinned by the go.mod tool directives.
proto:
	@mkdir -p bin/tools
	go build -o bin/tools/protoc-gen-go google.golang.org/protobuf/cmd/protoc-gen-go
	go build -o bin/tools/protoc-gen-go-grpc google.golang.org/grpc/cmd/protoc-gen-go-grpc
	protoc --plugin=protoc-gen-go=bin/tools/protoc-gen-go --plugin=protoc-gen-go-grpc=bin/tools/protoc-gen-go-grpc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/v1/sandbox.proto

# -race needs cgo; a Linux machine without a C compiler has cgo off, so skip it there.
test:
	@if [ "$$(go env CGO_ENABLED)" = "1" ]; then \
		go test -race ./...; \
	else \
		echo "race detector skipped: cgo is off (no C compiler)"; \
		go test ./...; \
	fi

lint:
	@out="$$(gofmt -l $$(git ls-files '*.go'))"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
	go vet ./...

# Runs anywhere; not a Linux target.
sim:
	go run ./cmd/schedsim --out results/schedsim.json

# Hardware capture and results rendering run anywhere. On macOS, hardware.sh writes the
# host part and runs itself in the VM for the rest if the VM is running (it does not
# start it); on Linux it writes the VM part.
hardware:
	scripts/hardware.sh

# Renders docs/results.md and the README table from results/*.json (spec call D26).
results:
	go run ./cmd/mkresults --results results --out docs/results.md --readme README.md

# Observability container (spec call D24): grafana/otel-lgtm as the rootless nerdctl
# container gss-obs in the VM, Grafana on port 3300 with deploy/grafana/dashboard.json.
# Not Linux targets: on macOS scripts/obs.sh runs itself in the VM.
.PHONY: obs-up obs-down
obs-up:
	scripts/obs.sh up

obs-down:
	scripts/obs.sh down
