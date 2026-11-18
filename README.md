# gvisor-sandbox-scheduler

A small reference implementation of sandboxes for code written by an AI agent. A Go node agent runs each sandbox as a gVisor (`runsc`) container built from an OCI bundle, with CPU and memory limits, no network unless the request carries an egress allowlist, and three ways to start: cold, restored from a checkpoint of a Python interpreter that has already done its imports, or claimed from a warm pool. A Go scheduler takes sandbox requests over gRPC, places each one with a filter and score policy modelled on kube-scheduler, proxies the command output streams, and starts the sandboxes of a node that stops heartbeating again on other nodes. Everything runs on one laptop, in one Linux VM; the "nodes" are processes in that VM.

Status in one line: the code is written, reviewed and unit-tested on macOS, and the integration tests, the demo, the benchmarks and the telemetry check have run in a Linux VM on one laptop (see [Status](#status)).

## How the pieces fit

```
 gssctl, loadgen         scheduler (one process)               node agent (one per node)          sandbox (runsc)
+---------------+  gRPC  +---------------------------+  gRPC  +---------------------------+ exec +------------------+
| Create        |------->| gateway: Create, Exec,    |------->| Manager: start paths,     |----->| PID 1: zygote.py |
| Exec (stream) |<-------|   Destroy; proxies Exec   |<-------|   limits, TTL reaper      |<-----| /gss/pyfork      |
| Destroy       |        | sched: filter + score     |        | warm pool, checkpoint     |      | /work and /tmp   |
+---------------+        | cluster: nodes, sandboxes,|        |   image                   |      |   on tmpfs       |
                         |   heartbeats, reschedule  |<-------| heartbeat client      (1) |      +------------------+
                         +---------------------------+        | netpolicy: netns, veth,   |
                                      |                       |   nftables allowlist      |
                                      |                       +---------------------------+
                                      |  OTLP, only when OTEL_EXPORTER_OTLP_ENDPOINT is set  |
                                      +-------------> grafana/otel-lgtm <-------------------+

(1) Register once, then Heartbeat every second: the live sandboxes and the warm count go up,
    the IDs the node must destroy come back.
cmd/schedsim runs the same placement policy against 1,000 synthetic nodes, with no I/O.
```

A request from `gssctl run -c 'print(1)'` goes to the scheduler, which picks a node, reserves room on it and asks its node agent to create the sandbox. The node agent claims a warm sandbox, restores one from the checkpoint image, or starts one cold, and answers once a no-op Python run has completed inside it. The scheduler then proxies the Exec stream: the code runs in a fork of the already-primed interpreter, and stdout, stderr and the exit code come back the same way. [docs/walkthrough.md](docs/walkthrough.md) follows that request through the code.

## Quick start

On a Mac with Apple silicon:

```sh
brew install lima go
make vm      # create and start the Lima VM "gss" from lima/gss.yaml
make demo    # build, build the sandbox rootfs, start a local cluster, run two examples
```

`make demo` also starts the VM if it is not running; `make vm` on its own creates or starts the VM and runs nothing in it. On macOS every target that needs Linux re-runs itself inside the VM with `limactl shell`; on a Linux machine the same targets run directly (they need root through `sudo`, `runsc`, Go, nftables and iproute2, and `CONTAINER_CLI=docker` if you build the rootfs with Docker instead of nerdctl). On a Linux host that runs Docker, Docker sets the iptables `FORWARD` policy to drop, which also drops all sandbox egress (nftables forwards a packet only if every forward chain accepts it): run `sudo iptables -P FORWARD ACCEPT` first; the node agent logs a warning naming any such chain.

After the demo:

```sh
make status   # the systemd units and the scheduler's view of the nodes
make down     # stop the cluster and remove what it left
make vm-stop  # stop the VM
```

Rebuild the sandbox image only with the cluster down: after a change to `images/python` or `cmd/pyfork`, run `make down` before `make rootfs` or `make demo`. The build refuses to replace the tree while the cluster runs (`FORCE=1` overrides) and does nothing when the image is unchanged, so `make demo` still runs against a running cluster.

Other targets: `make test` (unit tests, any OS; runs with the race detector when cgo is available, and without it, with a note, on a machine that has no C compiler), `make lint`, `make test-integration` (Linux, root, `runsc`; from macOS it runs in the VM), `make sim`, `make bench`, `make hardware`, `make results`, `make obs-up` and `make obs-down` (the `grafana/otel-lgtm` container: a development image, not a monitoring setup to keep), `make proto`, `make vm-delete`.

### What gets installed

On the host: Homebrew `go` (1.27.4) was installed for this project. `lima` (2.2.1) and `protobuf` (`protoc`, only needed by `make proto`) were already present. Nothing else is installed on the host, and the host's Docker is not used.

Inside the VM, [lima/gss.yaml](lima/gss.yaml) installs from official sources: Go 1.27.4 from go.dev; gVisor `runsc` release-20261102 from gVisor's release bucket, with a sha512 check; and the Ubuntu packages ca-certificates, curl, wget, zstd, make, git, jq, nftables, iproute2 and python3. Lima itself ships rootless containerd and nerdctl, which build the sandbox root filesystem and run the Grafana container.

## What `make demo` shows

`make demo` runs, inside the VM:

1. `make build`: every binary under `bin/`.
2. `make rootfs`: the sandbox root filesystem from [images/python/Dockerfile](images/python/Dockerfile) (`python:3.13-slim` with numpy and pandas, plus `/gss/zygote.py` and `/gss/pyfork`), exported to `/var/lib/gss/rootfs/python`. It is skipped when the build ID already matches. The first run pulls the base image and installs the packages, so it is the slow one.
3. `scripts/cluster.sh up`: one scheduler and four node agents (`a1`, `a2` in group `a`; `b1`, `b2` in group `b`), each a transient systemd unit. On first start one agent builds the checkpoint image and the others wait for it. `up` returns once every node is READY with its warm pool full.
4. `bin/gssctl run -f examples/hello.py` and `bin/gssctl run -f examples/pandas_demo.py`. Each prints the program's output, then one line on stderr of the form

   ```
   sandbox sb-<id> node=<node> group=<group> start=<cold|restore|warm> ready=<ms> total=<ms>
   ```

   `ready` is the node-side time from receiving Create to a ready sandbox; `total` is from sending Create to receiving the exit event. With the default settings every node keeps two warm sandboxes, so the expected start path is `warm`. The demo has run in the VM; no output is pasted here, because the `ready` and `total` figures differ from run to run.

The cluster keeps running afterwards. `gssctl ps`, `gssctl nodes` and `gssctl explain <id>` (the nodes considered and the per-plugin scores of the top candidates) work against it.

## Results

The block below is written by `make results` from the JSON files under `results/`; no number in it is typed by hand. The numbers are from one run of each benchmark on the machine named below.

<!-- results:begin -->
| Metric | Value | Source |
| --- | --- | --- |
| Start latency, cold (ready p50 / p95) | 1463 / 2110 ms | `start.json` |
| Start latency, restore (ready p50 / p95) | 144 / 230 ms | `start.json` |
| Start latency, warm (ready p50 / p95) | 0.1 / 0.5 ms | `start.json` |
| Throughput (create, Exec, destroy) | 7.1 sandboxes/s (8 workers, 1m0s) | `throughput.json` |
| Sandboxes per node | 20 within 2x of the first step's restore-start p95; the ramp reached 60 with no failed start or Exec | `density.json` |
| Placement, default policy, fill scenario | 38253 decisions/s | `schedsim.json` |
| Group-loss recovery (local failure-domain test) | 3567 ms without retry, 3905 ms with retry | `grouploss.json, grouploss-retry.json` |

Hardware: MacBook Pro, Apple M3 Pro, 6 performance + 6 efficiency cores, 18 GiB, macOS 26.5.2; VM: 4 vCPUs, 5910 MiB, Linux 6.8.0-147-generic, Ubuntu 24.04.5 LTS, runsc version release-20261102.0, go1.27.4, virtualization apple. Full tables, commands and caveats: [docs/results.md](docs/results.md).
<!-- results:end -->

**What the results show.** Restore gets a sandbox ready roughly an order of magnitude faster than a cold start (see the start latency rows). A warm claim makes no `runsc` call, so its time to ready is dominated by bookkeeping, and the time to first result is then mostly the Exec itself. Recovery from losing a whole node group is bounded by the 3 s failure threshold plus the time to start the lost sandboxes again, and with client retries no request was lost. The placement policy's cost per decision is small next to the cost of starting a sandbox. All of this is one run on one laptop, with nodes sharing the VM's vCPUs; [docs/results.md](docs/results.md) has the full tables and caveats.

The benchmarks ran on one laptop: an Apple M3 Pro (arm64) with one Lima VM (Apple Virtualization.framework, Ubuntu 24.04 arm64, 4 vCPU, 6 GiB). All nodes are processes in that VM and share its vCPUs; their capacity is declared with flags. gVisor is runsc release-20261102.0 on the systrap platform, the only platform the benchmarks use; Go is 1.27.4. `make hardware` records the exact machine in `results/hardware.json` (`make bench` runs it first), and `make results` prints it under every table. `make results` also writes `docs/results.md` with the full tables, commands and caveats; it is committed. [docs/design.md](docs/design.md#11-benchmarks) says what each benchmark measures and how to read it.

## Repository map

| Path | What it holds |
| --- | --- |
| [api/v1](api/v1) | `sandbox.proto` (SandboxService, NodeService, ClusterService), the generated Go, defaults and validation |
| [cmd/gssctl](cmd/gssctl) | The command-line client: `run`, `create`, `exec`, `destroy`, `ps`, `nodes`, `explain` |
| [cmd/scheduler](cmd/scheduler) | The scheduler binary: gateway, policy and cluster state in one process |
| [cmd/nodeagent](cmd/nodeagent) | The node agent binary |
| [cmd/pyfork](cmd/pyfork) | Static client that runs inside the sandbox and hands a request to the zygote |
| [cmd/loadgen](cmd/loadgen) | Benchmarks: `start`, `throughput`, `density`, `grouploss` |
| [cmd/schedsim](cmd/schedsim) | The placement policy against 1,000 synthetic nodes |
| [cmd/mkresults](cmd/mkresults) | Renders `docs/results.md` and the README block from `results/*.json` |
| [internal/sched](internal/sched) | The placement policy: filters and scorers, pure functions, no I/O |
| [internal/cluster](internal/cluster) | Scheduler state: node registry, sandbox table, failure detector, heartbeat reconciliation |
| [internal/gateway](internal/gateway) | The scheduler's gRPC services: place, reserve, start, proxy Exec, reschedule |
| [internal/nodeagent](internal/nodeagent) | Sandbox manager, warm pool, checkpoint image, Exec, TTL reaper, heartbeats |
| [internal/runsc](internal/runsc) | Thin wrapper over the `runsc` command line, and an in-memory fake for unit tests |
| [internal/bundle](internal/bundle) | The OCI bundle (`config.json`, hosts file) of a sandbox |
| [internal/netpolicy](internal/netpolicy) | Network namespace, veth pair and nftables rules for the egress allowlist |
| [internal/telemetry](internal/telemetry) | OpenTelemetry setup and the `gss_*` metrics |
| [internal/sim](internal/sim), [internal/bench](internal/bench) | Synthetic fleets; latency recording and result files |
| [images/python](images/python) | The sandbox root filesystem: `Dockerfile` and `zygote.py` |
| [deploy/grafana](deploy/grafana) | Dashboard and provisioning for the `grafana/otel-lgtm` container |
| [lima/gss.yaml](lima/gss.yaml) | The VM definition and its provisioning |
| [scripts](scripts) | Rootfs build, local cluster, benchmarks, local failure-domain test, hardware capture, observability container |
| [examples](examples) | The two programs `make demo` runs |
| [docs](docs) | The documents listed below |

## Design documents

- [docs/design.md](docs/design.md): every design choice with its reason, the alternative, and what would change the answer. Isolation (gVisor, Firecracker, runc), the gVisor platform, the sandbox, the network, start paths, the scheduler, failure handling, the mapping to Kubernetes, observability, benchmarks, what is not built, known limits.
- [docs/walkthrough.md](docs/walkthrough.md): one `gssctl run -c 'print(1)'` followed through the code, then what happens when the node dies mid-session. About 30 minutes.
- [docs/prior-art.md](docs/prior-art.md): existing projects in the same space and how this one differs.
- `docs/results.md`: benchmark tables, commands and caveats, written by `make results`.

## Scope

This is a reference implementation for reading and discussion. Not built:

- Multi-tenant authentication and authorization. Every gRPC call is plaintext and unauthenticated.
- Kubernetes integration. [docs/design.md](docs/design.md#9-mapping-to-kubernetes) maps each part to its Kubernetes counterpart instead.
- Production hardening: no TLS, no rate limits, no quotas, no audit log, no upgrade path.
- Placement across cloud providers or regions. There is no cloud code and no cloud account; [docs/design.md](docs/design.md#12-not-built) lists what it would take. The two node groups are processes in one VM, and the test that kills one of them is a local failure-domain test: it exercises the scheduler's detection and rescheduling and nothing about networks, regions or providers.
- A highly available scheduler, or scheduler state that survives a restart other than what the nodes report back.
- State-preserving migration. A sandbox that is started again after its node died is a fresh sandbox with the same ID and spec; its files and interpreter state are gone.

## Status

- All code is written and reviewed, and is on `main` as local commits. The repository has not been pushed.
- Unit tests pass on macOS (arm64): `go test -race ./...`.
- The Linux batch ran in the Lima VM on 2026-11-15 (Ubuntu 24.04 arm64 on an Apple M3 Pro, `runsc` release-20261102.0, systrap platform):
  - `make test-integration` passed for every package. A verbose rerun counted 36 integration tests passed, none skipped, none failed.
  - `make demo` ran. Four nodes became READY, and both examples ran in gVisor sandboxes started from the warm pool. Exit codes pass through; `--allow` reaches the allowed address, a sandbox with no allowlist gets "Network is unreachable", and an address off the list times out. `make down` left nothing behind.
  - Telemetry was checked with `make obs-up`: Grafana answers, the dashboard is provisioned, Prometheus has every `gss_` metric with its labels, and one `gssctl run` gives traces that cross `gssctl`, the scheduler and the node agent down to the `runsc` spans.
  - `make bench` produced every results file, and `make results` rendered `docs/results.md` and the block above. The group-loss run first failed because `systemctl kill` cannot signal gVisor's `runsc-fd-parking` helper processes; `scripts/cluster.sh` was fixed (commit d5d5cc4) and the run then passed.
- CI: [.github/workflows/ci.yml](.github/workflows/ci.yml) defines a unit job and an integration job. No CI job has run on a hosted runner, because the repository has not been pushed.
- There are no users, and the project has never run on more than one machine.

## License

Apache License 2.0; see [LICENSE](LICENSE).

This project is independent of the gVisor project and is not affiliated with it. "gVisor" in the name says which runtime the code drives.
