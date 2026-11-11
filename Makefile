# One Makefile for macOS and Linux. On macOS, targets in LINUX_TARGETS re-run
# themselves inside the Lima VM "gss". Add new Linux-only targets to LINUX_TARGETS.
SHELL := /bin/bash
UNAME := $(shell uname -s)

PKG ?= ./...
RUN ?= .
# Variables forwarded into the VM invocation. Append new ones here.
FORWARD_VARS := PKG RUN FORCE
FORWARD := $(foreach v,$(FORWARD_VARS),$(v)='$(value $(v))')

LINUX_TARGETS := build test-integration rootfs

.PHONY: vm vm-stop vm-delete proto test lint $(LINUX_TARGETS)

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
