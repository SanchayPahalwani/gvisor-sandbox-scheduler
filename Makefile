# One Makefile for macOS and Linux. On macOS, targets in LINUX_TARGETS re-run
# themselves inside the Lima VM "gss". Add new Linux-only targets to LINUX_TARGETS.
SHELL := /bin/bash
UNAME := $(shell uname -s)

PKG ?= ./...
RUN ?= .
# Variables forwarded into the VM invocation. Append new ones here.
FORWARD_VARS := PKG RUN FORCE
FORWARD := $(foreach v,$(FORWARD_VARS),$(v)='$(value $(v))')

LINUX_TARGETS := build test-integration rootfs bench-start bench-throughput bench-density bench-grouploss

.PHONY: vm vm-stop vm-delete proto test lint sim $(LINUX_TARGETS)

ifeq ($(UNAME),Darwin)
$(LINUX_TARGETS):
	limactl shell --workdir $(CURDIR) gss -- make $@ $(FORWARD)

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

# Benchmarks (spec section 10). Each one starts from a fresh cluster so runs do not
# share state, and refuses to run with OTLP export on or the gss-obs observability
# container running (spec call D25). start and throughput use the default four nodes;
# density uses one large node with no warm pool.
BENCH_NO_OTEL = if [ -n "$${OTEL_EXPORTER_OTLP_ENDPOINT:-}" ]; then \
	echo "OTEL_EXPORTER_OTLP_ENDPOINT is set; benchmarks run with observability off (spec call D25): unset it" >&2; exit 1; fi; \
	if command -v nerdctl >/dev/null 2>&1 && nerdctl ps --format '{{.Names}}' 2>/dev/null | grep -qx gss-obs; then \
	echo "the gss-obs observability container is running; benchmarks run with observability off (spec call D25): make obs-down" >&2; exit 1; fi
DEFAULT_TOPOLOGY := env -u GSS_NODES -u GSS_NODE_CPU -u GSS_NODE_MEM -u GSS_NODE_MAX -u GSS_POOL
DENSITY_TOPOLOGY := env GSS_NODES="a1:a:7101" GSS_NODE_CPU=64000 GSS_NODE_MEM=65536 GSS_NODE_MAX=200 GSS_POOL=0

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

test:
	go test -race ./...

lint:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
	go vet ./...

# Runs anywhere; not a Linux target.
sim:
	go run ./cmd/schedsim --out results/schedsim.json
