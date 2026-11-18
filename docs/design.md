# Design

This document explains the design of `gvisor-sandbox-scheduler`, one decision at a time: what was chosen, why, the main alternative, and what would change the answer. The design rests on 37 numbered calls, D1 to D37. Each one is explained below in the section where it belongs, and the [index at the end](#index-of-calls) says where to find it. Decisions that are not numbered calls get the same treatment.

The numbers in this document are design parameters: timeouts, weights, sizes and defaults from the code. None is a measurement. Measured numbers will appear only in `docs/results.md`, which `make results` writes from the JSON files under `results/`, and the benchmarks ran once, on one laptop (see the README's Status section). A few behaviours were observed by hand while the design was made; where they shaped a decision they are described in words, as single observations and not as benchmark results.

Contents:

1. [Problem and non-goals](#1-problem-and-non-goals)
2. [Isolation: gVisor, Firecracker or runc](#2-isolation-gvisor-firecracker-or-runc)
3. [Platform: systrap, KVM or ptrace](#3-platform-systrap-kvm-or-ptrace)
4. [The sandbox](#4-the-sandbox)
5. [Network](#5-network)
6. [Start paths](#6-start-paths)
7. [Scheduler](#7-scheduler)
8. [Failure handling](#8-failure-handling)
9. [Mapping to Kubernetes](#9-mapping-to-kubernetes)
10. [Observability](#10-observability)
11. [Benchmarks](#11-benchmarks)
12. [Not built](#12-not-built)
13. [Known limits and what to do next](#13-known-limits-and-what-to-do-next)

[Index of calls](#index-of-calls)

## 1. Problem and non-goals

### The problem

An AI agent that writes code has to run it. The usual pattern is many short programs, often a few lines of Python that load some data, compute and print. A model wrote them, so they are untrusted. Every run needs:

- isolation from the host, from other runs, and from the services on the same machine;
- CPU and memory limits enforced from outside the program;
- no network, unless the caller names specific destinations;
- stdout and stderr streamed back, then an exit code the caller can trust;
- a start fast enough that the agent loop does not wait on it, because the loop pays it on every run.

Above one machine there is a placement problem: which node runs each sandbox, how to keep nodes full without making requests wait, and what happens to running sandboxes when a node dies.

This repository builds both halves at a small scale: a node agent that turns a request into a gVisor sandbox, and a scheduler that places requests on nodes and recovers from node loss.

### Non-goals

- Multi-tenant authentication and authorization. Every call is plaintext and unauthenticated (D7).
- Kubernetes integration. [Section 9](#9-mapping-to-kubernetes) maps each part to its Kubernetes counterpart instead.
- Production hardening, a highly available scheduler, and persistent scheduler state ([section 7.3](#73-state-in-memory-rebuilt-from-heartbeats)).
- Sandbox state that survives node loss (D23).
- Placement across cloud providers or regions ([section 12](#12-not-built)).
- mTLS between the scheduler and the nodes, and an LLM agent demo. Both were stretch items and are not built.

The non-goals are where real systems spend most of their effort. Leaving them out keeps the code to what is specific to sandboxes: start paths, limits, egress, placement and failure handling.

### Where it runs

**D2. One Lima VM, defined in the repository.**

- *Choice.* [lima/gss.yaml](../lima/gss.yaml) describes an Ubuntu 24.04 arm64 guest (4 vCPU, 6 GiB, 20 GiB disk) run with Apple's Virtualization.framework (`vmType: vz`). `make vm` creates and starts it. Provisioning installs Go from go.dev and `runsc` from gVisor's release bucket after a sha512 check.
- *Why.* gVisor needs a Linux kernel and the host is macOS. One declarative file says what is installed, from where, at which version, and a reader can audit it. The guest runs natively on the arm64 CPU.
- *Alternative.* The Docker VM already on the machine (Colima), with the node agent in a privileged container. The agent manages cgroups, network namespaces and nftables on the host it runs on; inside a container that needs a pile of exceptions, and it would change a VM used for other work. Colima with a second profile is a wrapper around Lima aimed at Docker, so a plain Lima file is the simpler thing to read. One VM per node is closer to real nodes, but four guests do not fit in the host's memory next to the existing VM.
- *What would change it.* A Linux workstation (no VM at all; the Makefile already runs directly on Linux, D27), or a host with enough memory for one VM per node, which would give each node its own kernel and make a node failure more than a process failure.

**D3. A node is a node-agent process.**

- *Choice.* All nodes are processes in the one VM. Each has its own node ID, group label, gRPC port, `runsc` root (`/run/gss/<node-id>`), state directory, cgroup subtree (`/gss/<node-id>`) and egress address range. Capacity is declared with flags (`--cpu-millis`, `--memory-mib`, `--max-sandboxes`), not detected. Killing a node means SIGKILL to the agent and `cgroup.kill` on its cgroup subtree, which takes its sandboxes with it (`scripts/cluster.sh kill-node`).
- *Why.* Four nodes in two groups are enough to show filtering, scoring, group constraints and the loss of a whole group. Processes are cheap, and the per-node directories, cgroups and nftables tables keep nodes from seeing each other's sandboxes.
- *Cost.* The nodes share the VM's 4 vCPUs and 6 GiB, so they compete with each other and with the load generator, and the declared capacities can add up to more than the VM has. The caveats that `make results` prints with the tables say so.
- *Alternative.* One VM per node (see D2), or containers as nodes.
- *What would change it.* More than one machine. Capacity would then come from the machine, and a node failure could be a kernel, disk or network failure, not only a process exit.

**D5. No benchmark under emulation.**

- *Choice.* Guest and host are both arm64 and the rootfs is built for `linux/arm64`. Nothing uses Rosetta or QEMU's TCG.
- *Why.* A number measured under emulation measures the emulator.
- *Alternative.* An amd64 guest under emulation, to match the more common server architecture. Rejected for the reason above.
- *What would change it.* Nothing would make emulation acceptable for a benchmark. On an x86 host the same files build an amd64 image natively.

**D27. One Makefile for macOS and Linux.**

- *Choice.* On macOS each target listed in `LINUX_TARGETS` re-runs itself inside the VM (`limactl shell --workdir $(CURDIR) gss -- make <target>`) and forwards a fixed list of variables (`FORWARD_VARS`). On Linux the same target runs directly. Processes in the VM are transient systemd units (`gss-scheduler`, `gss-node-<id>`), so they outlive the shell that started them and log to the journal.
- *Why.* The laptop, a Linux user and CI use one set of targets, documented once.
- *Alternative.* Separate scripts per platform, or a task runner other than make.
- *What would change it.* A Windows host, or a CI system that does not run make.

### Names and prior art

**D1. The repository is named `gvisor-sandbox-scheduler`, not `agent-sandbox`.**

- *Choice and why.* `agent-sandbox` is already the name of a Kubernetes SIG Apps project in the same space (`kubernetes-sigs/agent-sandbox`) and of several packages. [prior-art.md](prior-art.md#the-name) records the search and the candidates that were rejected. The Go module path follows the repository name.
- *Alternative.* `runsc-scheduler` was the fallback; `sandboxd`, `sandbox-fleet` and others were taken in the same domain.
- *What would change it.* The repository moving to another account: a one-line change in `go.mod` plus a rewrite of the import prefix.

**D29. The short prefix is `gss`.**

- *Choice.* The CLI is `gssctl`, the VM `gss`, host state `/var/lib/gss`, runtime directories `/run/gss`, cgroups `/gss/<node-id>/<runsc-id>`, in-sandbox files `/gss/zygote.py` and `/gss/pyfork`, environment `GSS_*`, metrics `gss_*`.
- *Why.* The initials of the repository name, short enough for every path.
- *Alternative.* `sbx` was the first choice and was dropped because it is the CLI name of an existing sandbox product (Docker Sandboxes, per [prior-art.md](prior-art.md)).
- *What would change it.* A clash of `gss` names (a CLI, a metric prefix, a cgroup path) with something on the same machine.

**D28. The prior-art file names existing projects and products.**

- *Choice.* [prior-art.md](prior-art.md) describes other agent sandboxes and the building blocks, each from its own public pages on the research date. No employer, client or internal system is named anywhere in the repository.
- *Why.* A design is easier to judge next to what exists, and naming the projects is the only way the comparison can be checked.
- *Alternative.* A generic comparison with no names, which nobody can verify.
- *What would change it.* Nothing about the naming; the file would only need a new research date when the projects it describes change.

## 2. Isolation: gVisor, Firecracker or runc

The first question for untrusted code is what stands between it and the host kernel.

| | runc (namespaces, cgroups, seccomp) | gVisor (`runsc`) | Firecracker microVM |
| --- | --- | --- | --- |
| Isolation boundary | The host kernel itself. The workload's system calls go to the shared host kernel, filtered by seccomp and scoped by namespaces. | A user-space application kernel (the sentry) implements the Linux system calls for the workload. The sentry runs on the host kernel and uses a much smaller set of host calls. | Hardware virtualization through KVM: a guest kernel per sandbox, and a virtual machine monitor with a minimal device model. |
| Start time class | A process start. | A process start plus the boot of the sentry. | A VM boot, or a resume from a VM snapshot. |
| Memory overhead | Nothing beyond the workload. | The sentry and its helper processes, per sandbox. | A guest kernel and the monitor, per sandbox. |
| Syscall compatibility | The host kernel's, minus whatever the seccomp profile blocks (container runtimes apply a default profile that denies some calls). | gVisor's own implementation of the Linux interface. Most programs run; some system calls, flags and `/proc` files are missing or behave differently. | Complete inside the guest, which runs a real Linux kernel. |
| Needs KVM | No. | No on the systrap platform used here; yes on its KVM platform ([section 3](#3-platform-systrap-kvm-or-ptrace)). | Yes. |
| Snapshot support | `runc checkpoint` and `runc restore`, which rely on CRIU, a separate tool that has to be installed. | Built in: `runsc checkpoint` and `runsc restore` of the whole sandbox. A restore on another machine needs the CPU features present at checkpoint time. | Built in: full and diff snapshots of the VM. |
| Used by, per prior-art | OpenSandbox (its default runtime) | `kubernetes-sigs/agent-sandbox` and GKE Sandbox through RuntimeClass, Agent Substrate, Ray Sandboxes, OpenSandbox, Modal (selectable) | E2B, Vercel Sandbox, Fly.io Machines, Cloudflare Containers, OpenSandbox Fast Sandbox |

Sources: [prior-art.md](prior-art.md), sections "gVisor and runsc", "gVisor checkpoint/restore", "Firecracker", "Kata Containers" and the agent sandbox table. The runc entries (CRIU, the seccomp profile) are general knowledge, not from prior-art. The start time, memory and compatibility rows describe how each design works by construction; none of them is a measurement, and this repository measures gVisor only.

### Why gVisor here

- *No KVM needed.* As configured, the Lima guest exposes no `/dev/kvm`. That is a configuration choice, not a hard limit: Apple's Virtualization.framework offers nested virtualization on M3 and later ([section 3](#3-platform-systrap-kvm-or-ptrace)), so Firecracker could be tried here. What gVisor's systrap platform gives is that it runs in an ordinary VM without it, and should need nothing more than a plain hosted CI runner (the CI integration job has not run yet). The reasons below carry the decision.
- *An OCI runtime.* `runsc` takes a bundle directory and a `config.json` and applies the cgroup limits itself. The node agent stays an OCI-level program, which is also the shape of the Kubernetes route (RuntimeClass, [section 9](#9-mapping-to-kubernetes)).
- *Checkpoint and restore are part of the runtime.* A checkpoint of a running sandbox restores into a different cgroup, with different limits and a different network namespace (all three observed during design). That is what makes the restore start path possible ([section 6](#6-start-paths)).
- *Process-sized sandboxes.* Many sandboxes have to fit in a 6 GiB VM next to each other.
- *The subject of this project* is the scheduler and the start paths. gVisor keeps the isolation layer a command-line tool.

The costs are real: gVisor adds work to every system call, so programs heavy on system calls or I/O run slower than under runc, and some programs hit parts of the Linux interface gVisor does not implement. Both matter less for short data-handling scripts, which spend their time in Python and numpy.

### When Firecracker is the better answer

- The threat model wants a hardware boundary: hostile tenants at scale, or a compliance requirement that names virtualization.
- The workloads need the complete Linux interface (unusual system calls, eBPF, kernel features gVisor lacks).
- The hosts are bare metal or expose KVM.
- The product is pause and resume of whole machines with lazily loaded memory, as E2B's runtime does with `userfaultfd` (per prior-art).
- The workloads are heavy on system calls or I/O.

Kata Containers is the same trade inside Kubernetes: a VM per pod, selected through RuntimeClass. It is not used here.

### When runc is enough

When the code is trusted (a team's own CI jobs), or when another boundary already separates tenants, for example one VM per tenant with containers inside it.

### What would change the answer

A host with KVM, plus workloads that need the complete Linux interface or a hardware boundary. Then Firecracker, or Kata under Kubernetes, and the node agent's runtime interface ([internal/runsc/runsc.go](../internal/runsc/runsc.go)) would get a second implementation. The scheduler would not change.

## 3. Platform: systrap, KVM or ptrace

A gVisor platform is the mechanism that stops the workload's system calls and page faults and hands them to the sentry.

**D4. systrap only.**

- *Choice.* The default platform, systrap. No `--platform` flag is passed.
- *The options.* systrap has been gVisor's default since mid-2023; it uses seccomp to trap the workload's system calls and needs no `/dev/kvm`, so it runs inside an ordinary VM. It was checked on the arm64 guest during design: create, start, exec, checkpoint and restore all work. The KVM platform uses hardware virtualization to run the sentry and the workload; it needs `/dev/kvm`, and gVisor's documentation positions it for bare metal. Inside a VM it needs nested virtualization, which Apple's Virtualization.framework offers on M3 and later but not on earlier Apple silicon; the Lima guest as configured here exposes no `/dev/kvm`. ptrace is deprecated upstream and was not considered.
- *Why.* The default, no KVM, and the same platform on the laptop and on a CI runner.
- *Alternative.* The KVM platform with nested virtualization turned on in the VM.
- *What would change it.* A bare-metal Linux host, or a VM that exposes `/dev/kvm`. Both platforms should then be measured: which one is faster depends on the workload's mix of system calls and memory faults, so the answer would be a new row in the start and Exec tables, not an assumption. The caveats printed with the results state that only systrap was measured.

D5 (no emulation, [section 1](#where-it-runs)) completes the picture: the platform runs on a native arm64 guest.

## 4. The sandbox

### 4.1 Driving runsc

**D9. The node agent runs the `runsc` command line.**

- *Choice.* [internal/runsc](../internal/runsc/cli.go) runs the `runsc` binary for create, start, restore, checkpoint, exec, kill, delete, state, list and version. The same top-level flags (`--root`, `--network`, `--overlay2=none`) go on every command for a given sandbox, because gVisor requires that for restore. The manager is written against a `Runtime` interface, with an in-memory fake for unit tests.
- *Why.* The CLI is gVisor's documented and stable surface. Every call is a child span (`runsc.create`, `runsc.restore`, ...), so a trace shows where start time goes. The fake keeps the manager testable on macOS.
- *Alternative.* containerd with the gVisor shim, which is the route on Kubernetes ([section 9](#9-mapping-to-kubernetes)), or importing gVisor's Go packages. The shim would hide the steps this project exists to show; gVisor's internal packages are not a stable API.
- *Costs found while building.* One process start per call. `runsc` matches container IDs by prefix, so IDs are made prefix-free by construction: `<sandbox-id>_<incarnation>_` for sandboxes started cold or restored, `warm_<hex>_` for the pool (a claimed pool sandbox keeps that ID, with its bundle and cgroup), `img_<key>_<pid>_` for the image builder; sandbox IDs may not contain `_`, and `warm` and `img` are reserved, so no ID can be a prefix of another. `runsc exec` exits 128 when it fails itself, which is also a valid exit code of a program, so on every exit 128 the wrapper asks `runsc state` before it trusts the code. When `runsc exec`'s last line says it lost the sandbox while waiting on the process, and `runsc state` still says running, the wrapper asks again every 50 ms for up to 2 s: `runsc` reports a sandbox as running until every thread of its process has exited, which can be after `runsc exec` saw the control connection close. This was found on a CI runner, where a SIGKILLed sandbox was still reported running when the wrapper asked; in the VM the same happens with 1.5 GiB in the sandbox.
- *What would change it.* Running under Kubernetes (use the shim), or start rates at which a process start per call matters.

### 4.2 Root filesystem

**D10. One read-only root filesystem; writable tmpfs only at `/work` and `/tmp`.**

- *Choice.* `root.path` is the node's shared rootfs directory, `root.readonly` is true, and `runsc` runs with `--overlay2=none`. The only writable places are two tmpfs mounts, `/work` and `/tmp`, 64 MiB each, mode 1777. A per-sandbox `hosts` file is bind-mounted read-only at `/etc/hosts` in every bundle, with or without egress, so the mount list is the same for all sandboxes.
- *Why.* Nothing is copied per sandbox, and no sandbox can change what the next one sees. tmpfs pages live in the sandbox's memory, so writes count against its memory limit and disappear with it. "A sandbox can write to two directories" is easy to state and to test.
- *Alternative.* A writable overlay over the whole root (`--overlay2=root:memory` with a writable root). Code could write anywhere, which helps `pip install`. During design a read-only root stayed read-only whatever the overlay flag said, so the first draft (a per-sandbox overlay) was replaced by explicit tmpfs mounts.
- *What would change it.* Workloads that install packages or write outside `/work`: then a writable root with a memory-backed upper layer, sized and counted against the sandbox's limit.

**D14. The rootfs comes from a Dockerfile, built with rootless nerdctl in the VM.**

- *Choice.* [images/python/Dockerfile](../images/python/Dockerfile) is `python:3.13-slim` plus numpy 2.5.4 and pandas 3.1.1 (pinned), a uid 1000 user, `/work`, `/gss/zygote.py` and `/gss/pyfork`. [scripts/build-rootfs.sh](../scripts/build-rootfs.sh) builds it with the rootless nerdctl that Lima ships and exports it to `/var/lib/gss/rootfs/python`. The build ID, a sha256 over the Dockerfile, `zygote.py`, the `pyfork` binary and the script, goes in `python.id`; the build is skipped when it matches. A new tree replaces the old one, which is removed at once, so the script refuses to build while any `gss-*` systemd unit is active (`FORCE=1` overrides).
- *Why.* A Dockerfile is the form most readers know, and exporting the image to a directory makes "an OCI bundle is a directory plus `config.json`" concrete. The host's Docker is not used.
- *Alternative.* Pulling layers from a registry and unpacking them, as Ray Sandboxes does (per prior-art), or a debootstrap tree.
- *What would change it.* Many images or many machines: then a content-addressed image store with pulls and a local cache.

### 4.3 Process, identity and limits in the bundle

[internal/bundle/bundle.go](../internal/bundle/bundle.go) writes the bundle with the OCI runtime-spec Go types:

| Field | Value |
| --- | --- |
| `process.args` | `["python3", "/gss/zygote.py"]` |
| `process.user`, `cwd` | uid and gid 1000, `/work` (`HOME=/work`) |
| Capabilities, `noNewPrivileges` | none, true |
| `RLIMIT_NOFILE` | 1024 |
| Environment | `PATH`, `HOME`, `LANG`, `PYTHONUNBUFFERED=1`, `OPENBLAS_NUM_THREADS=1`, `OMP_NUM_THREADS=1`, `GSS_PRIME` |
| Namespaces | pid, ipc, uts, mount, network (in egress mode, the path of the prepared namespace) |
| `linux.cgroupsPath` | `/gss/<node-id>/<runsc-id>` |
| CPU | quota = `cpu_millis * 100` per period 100000 |
| Memory | limit = `memory_bytes` |
| Mounts | `/proc`, `/dev`, `/sys` (read-only), tmpfs `/tmp` and `/work`, `/etc/hosts` bind (read-only) |

The process section is the same for every sandbox on a node. That is forced by restore: `runsc restore` refuses a bundle whose `process.args` differ from the checkpointed ones (seen during design), and the credentials, rlimits and environment of a restored process come from the image. So everything that varies per request (limits, network namespace, hosts file) lives outside the process section. The node agent runs as root because `runsc` needs it for cgroups and namespaces; the workload runs as uid 1000 with no capabilities, inside gVisor.

### 4.4 The primed interpreter

**D11. PID 1 is a fork server (the zygote); Python code reaches it through `pyfork`, which passes its file descriptors.**

- *Choice.* [zygote.py](../images/python/zygote.py) imports the modules in `GSS_PRIME`, runs `gc.collect()` and `gc.freeze()`, listens on a Unix socket inside the sandbox, and forks per request. [cmd/pyfork](../cmd/pyfork/main.go) is a small static Go binary: it reads a header line and the code from stdin, connects to the socket (retrying for up to 30 s), passes its own descriptors 0, 1 and 2 with `SCM_RIGHTS`, sends the request, waits for a 4-byte exit code and exits with it. The zygote forks a supervisor per request, and the supervisor forks the worker. The worker `dup2`s the passed descriptors onto 0, 1 and 2 and runs the code as `python3 -c` would: in a fresh `__main__` module, with `sys.argv` `["-c"]` and the working directory first on `sys.path`, then thread shutdown, `atexit` handlers, a collection pass and flushed streams before it exits. Output goes from the worker straight to the `runsc exec` pipes, with no relay and no framing inside the sandbox.
- *Why.* "Restore a primed Python runtime" has to mean an interpreter process whose imports are done; otherwise a checkpoint saves nothing a cold exec would not redo. A fork server is the smallest thing that gives every Exec that interpreter. Passing descriptors avoids a framing protocol and a relay loop.
- *Alternative.* A long-lived agent inside the sandbox that runs commands and multiplexes their output over one connection, which is how several hosted sandboxes do it. It needs a protocol, a relay loop and its own failure handling, and the output would pass through code running next to the workload.
- *Fork safety.* The zygote stays single-threaded and ignores `SIGCHLD` so supervisors are reaped. BLAS threads are off (`OPENBLAS_NUM_THREADS=1`), so the process that forks has no helper threads. `gc.freeze()` moves the primed heap out of the collector's reach, so the worker's exit-time collection does not walk it and forked children keep sharing those pages.
- *What would change it.* Other languages (each needs its own zygote, or a general in-sandbox agent), primed modules that start threads at import (fork is then unsafe), or a need to kill everything one Exec started (see [timeouts](#46-timeouts)).

**D15. The primed set includes numpy and pandas.**

- *Choice.* `GSS_PRIME` defaults to `json, re, math, random, statistics, datetime, collections, itertools, functools, decimal, csv, io, numpy, pandas` (`bundle.DefaultPrime`).
- *Why.* These are what short data-handling scripts written by a model tend to import. pandas is there on purpose: it is the import that makes the difference between a cold and a restored start visible. The results renderer prints the primed set beside the start latencies, so nobody reads them as "Python in general".
- *Alternative.* An empty primed set (a restore would then save only the interpreter start), or several images with different sets.
- *What would change it.* A different workload mix. The agent passes the set to the zygote in `GSS_PRIME`, but neither `cmd/nodeagent` nor its environment can change it: changing it means editing `bundle.DefaultPrime` and rebuilding. The set is part of the image key (D36), so a change builds a new checkpoint image.

### 4.5 Exec paths and the stream contract

Two paths, one streaming contract ([internal/nodeagent/exec.go](../internal/nodeagent/exec.go)):

- `argv`: `runsc exec <id> argv...`, a new process, any program in the image; `env` becomes `--env` flags.
- `python_code`: `runsc exec <id> /gss/pyfork`, with stdin carrying one JSON header line `{"code_len", "timeout_s", "env"}`, then the code, then the request's own stdin bytes.

The agent reads the child's stdout and stderr and sends them as `ExecEvent`s of at most 32 KiB, in arrival order per stream, from a single goroutine, and then one exit event. Request stdin is written to the child and closed.

**D34. An Exec stream ends with exactly one exit event only when the program's exit was seen; otherwise it ends with a gRPC status and no exit event.**

- *Choice.* The node and the gateway send an exit event only for an exit they saw; every other ending is a gRPC status.
- *The statuses.* `NotFound` for an unknown sandbox and for one whose destroy has started when the Exec arrives; `Unavailable` for a sandbox still starting, and for an Exec already running when its sandbox is destroyed or lost; `Internal` for any other runtime failure; `InvalidArgument` and `Canceled` as usual. Through the scheduler, an Exec for a sandbox that is not `READY` yet (starting, or being moved) waits for it, bounded by the request's deadline and a 15 s cap, and ends with `DeadlineExceeded` if it is still not `READY` then.
- *Why.* A client must never mistake a lost sandbox for an exit code. A made-up code such as 137 or -1 cannot be told apart from the program's own. Output already streamed stays valid; the status only says the exit was not seen.
- *Alternative.* Always end with an exit event that carries a "lost" flag. Simpler for clients that ignore errors, but "did this run?" then lives in a field a client can forget to read.
- *The gateway inherits the rule from the node.* It relays whatever the node sends and never invents an exit ([gateway.Exec](../internal/gateway/gateway.go)). Nothing follows an exit because the node sends nothing after its exit; once an exit has gone by, the way the node's stream ends no longer changes the result. When its own state ends an Exec that is running (D37), the status is `Unavailable` whatever the state says, with the state's reason in the message (destroyed, TTL expired, lost, moved); a `NotFound` stays `NotFound` only when the node itself answered it and the state has the sandbox destroyed or no longer knows it.
- *What would change it.* A client API that wants an explicit "exit unknown" marker in the stream, for example a batch interface. The marker would be a field of its own, still never an exit code.

### 4.6 Timeouts

- *Choice.* Default 30 s, maximum 10 min. For `argv` the agent's own timer fires at the timeout: it sends SIGKILL to the process inside the sandbox (`runsc kill --pid`, with the in-sandbox pid that `runsc exec --internal-pid-file` wrote), ends the `runsc exec` client, and reports exit 137 with `timed_out`. For `python_code` the timeout also goes to the zygote in `pyfork`'s header: the supervisor kills the worker's process group, and the worker itself in case it left the group, at the deadline. The agent's own timer fires 2 s later as a backstop. So a Python timeout normally comes back from inside the sandbox as exit 137, and the agent marks it `timed_out` when the elapsed time reached the timeout.
- *The timer is independent of the output stream.* It runs on its own, outside the loop that sends events, so a client that stops reading cannot hold off a timeout.
- *Cancellation.* When the client goes away, the agent kills the process; for `python_code` the supervisor also sees its connection close and kills the worker.
- *Why two timers.* The supervisor knows the worker's process group and kills it with no `runsc` call. The agent's timer covers a zygote that is itself stuck.
- *Alternative.* Only the agent's timer: one more `runsc` call per timeout, and it kills one pid, not a group.
- *What is not covered.* A process that leaves its process group can outlive a timeout; the sandbox's TTL and Destroy are the backstop. A background process that keeps the Exec's stdout or stderr open would hold the stream open after the program exits, so the wrapper stops waiting for the pipes 2 s after the exit. The sandbox survives a timed-out Exec.
- *What would change it.* A need to kill everything an Exec started: each Exec would then need its own container inside the sandbox, or a cgroup of its own.

### 4.7 Limits and what happens over the memory limit

`runsc` creates the sandbox's cgroup v2 from the bundle: `cpu.max` and `memory.max`. Every sandbox runs with swap off (`memory.swap.max` 0, because the bundle's OCI `swap` equals its memory limit), so a host that has swap cannot let a sandbox go past its memory limit; this was found on a CI runner that has swap, where a 192 MiB sandbox held 1 GiB. The sentry, its helper processes and the workload's memory are all charged to that one cgroup, and the host kernel's OOM killer picks the victim inside it.

**D30. The CPU limit is checked from the host's cgroup, not from inside the sandbox.**

- *Choice.* The integration test asserts on `usage_usec` in the sandbox cgroup's `cpu.stat` and only logs what the program measures inside.
- *Why.* Inside gVisor, `time.process_time()` reports the sentry's own accounting, which counts time the host throttled the sandbox as CPU time. During design, a busy loop under a small CPU limit read far more CPU time inside the sandbox than the host's `cpu.stat` showed. The limit is enforced by the host cgroup, so that is where to check it.
- *Alternative.* Assert on the in-sandbox time (wrong), or on wall-clock throughput (indirect).
- *What would change it.* A gVisor release whose accounting matched the host's.

**D31. The smallest sandbox is 192 MiB, not 128 MiB.**

- *Choice.* `gssv1.MinMemoryBytes` is 192 MiB. The benchmarks use it as their small size.
- *Why.* A cold start imports the primed set, and in development runs its peak memory was above 128 MiB: a 128 MiB sandbox was killed before it became ready. 192 MiB leaves room for the imports and a small program.
- *Alternative.* A second image with a smaller primed set for small sandboxes. It doubles the checkpoint images and the start-path matrix for no gain in a local v1.
- *What would change it.* Demand for many tiny sandboxes. A smaller primed set could lower the floor; a restored sandbox does not run the imports again, so restore-only starts might too, but that was not measured.

**D32. A program over its memory limit usually ends the sandbox; both outcomes are reported.**

- *Choice.* The agent reports both cases. It reads the cgroup's `oom_kill` counter (in `memory.events`) before and after each Exec. An exit 137 with a raised counter is returned as that exit with `oom_killed`, and the sandbox is checked once, without waiting; if its process is gone the sandbox is removed, and the next Exec gets `NotFound`. A runtime error, or an exit 128 with a raised counter, can mean the sentry died: the agent then re-checks for a moment (D33); if the sandbox is gone it removes it and ends the Exec with `Unavailable` instead of an exit event, and if it is still there the exit 128 is returned as an exit event (any other runtime error becomes `Internal`).
- *Why both.* In the hand spike the OOM killer took the over-limit process and the sandbox kept working. In the later development runs at 192 MiB and 256 MiB it took the sentry each time, which ends the sandbox. Which process the kernel picks is not under the agent's control.
- *Alternative.* Give the workload a memory budget inside gVisor that is smaller than the cgroup's, so gVisor stops the program before the host kernel has to choose. Not tried.
- *What would change it.* Long sessions in which losing the sandbox and its files on one bad allocation is unacceptable.

**D33. The agent, not the `runsc` wrapper, recognises an OOM-killed sentry.**

- *Choice.* The wait applies only when the Exec did not end with an exit the agent can trust: a runtime error that says the container is not running, or a runtime error or an exit 128 (`runsc`'s own failure code) while `oom_kill` rose. Then the agent re-reads the sandbox state every 50 ms for up to 2 s, because `runsc` briefly still reports a dead sentry as running. If the sandbox is gone it removes it and returns `Unavailable`; otherwise an exit 128 is returned as an exit event and any other runtime error as `Internal` (`exitStatus` and `removeIfGone` in [exec.go](../internal/nodeagent/exec.go)). An exit 137 with a raised `oom_kill` is checked once, without the wait, and the Exec still returns its exit event.
- *Why.* The agent has the cgroup counters. The wrapper stays a thin, generic layer over the CLI.
- *Alternative.* Teach the wrapper about cgroups.
- *What would change it.* A `runsc` that reported a killed sentry at once and distinctly. The wrapper could then map it, and the 2 s re-check could go.

### 4.8 Lifetime and agent restarts

Every sandbox has a TTL, default 10 min, at least 10 s and at most 60 min, counted from the original create and not reset by rescheduling (so the TTL a node is given can be below 10 s; the node accepts it). The scheduler expires the sandbox at its TTL and tells the node to destroy it. The node's own reaper fires 10 s later, so a node that lost contact with the scheduler still cleans up. Destroy is `runsc delete --force` (kill, then delete), a kill of any process left in the cgroup, the network teardown, removal of the bundle and pid files, and removal of the cgroup. Each step is idempotent and remembered once done; a teardown that fails leaves the entry "broken", still counted against the node, and the liveness loop retries it.

**D13. The node agent keeps no state across restarts.**

- *Choice.* On start the agent deletes every container under its `runsc` root and every leftover cgroup, bundle and pid directory; the network policy removes its own leftover namespaces, veths and nftables table. A restarted agent is a new, empty node. Two locks, `<state-dir>/agent.lock` and `/run/gss/<node-id>.lock`, keep two agents from ever sharing a node ID.
- *Why.* One reconciliation path fewer. The scheduler already handles a node that lost its sandboxes: they go missing from its heartbeats and are rescheduled ([section 8](#8-failure-handling)).
- *Alternative.* Recover running sandboxes from `runsc`'s state on disk after an agent restart. Possible, since that state survives, but it is one more path to get right.
- *What would change it.* Agent upgrades on nodes with long-lived sandboxes. The agent should then adopt what `runsc` still runs, the way a kubelet finds its containers again after a restart.

## 5. Network

**No network by default.** The sandbox runs with `runsc --network=none` and has a loopback interface and nothing else. Most generated code needs no network, and no network is the policy that is easiest to get right.

**D12. Egress is a network namespace, a veth pair and an nftables allowlist; hostnames are pinned and there is no DNS.**

- *Choice.* A sandbox that asks for egress gets its own network namespace and veth pair, and the node's nftables table allows only the listed TCP destinations from that veth.
- *Why.* The policy is enforced outside the sandbox, by the host kernel, where the workload cannot reach it. Nothing runs besides nftables, and anything not listed is dropped, so a mistake fails closed.
- *Alternatives and what would change the answer* are after the mechanics, below.

How it works ([internal/netpolicy](../internal/netpolicy/rules.go)):

- For a request with `egress_allow`, the agent creates a network namespace and a veth pair, gives the pair a /30 from the node's `--egress-cidr` range (default `10.200.0.0/20`; `cluster.sh` gives node *i* the range `10.200.<16*i>.0/20`), adds a default route through the host end, and starts the sandbox with `runsc --network=sandbox` in that namespace. gVisor's own network stack takes over the interface, and packets leave through the host kernel, where nftables applies.
- Each node owns one table, `inet gss_<node>`, whose comment names the node and its range. A node refuses to start if another node's table claims an overlapping range, or if the veth name prefixes collide.
- Prerouting, before connection tracking: drop IPv6 from sandbox veths, and drop any packet whose source address is not the guest address of the veth it came in on.
- Forward chain, in order: drop traffic from one sandbox veth to another; accept established and related traffic only if the pair (interface, connection mark) is in the set of live pairs, checked in both directions; drop anything else going into a sandbox veth; for new traffic from a sandbox veth, jump to that sandbox's chain, which accepts TCP to each allowed prefix and port and sets the connection mark to the sandbox's generation number; drop the rest.
- Input chain: drop everything from sandbox veths, so a sandbox cannot reach the node agent, the scheduler or anything else listening on the node.
- Postrouting: masquerade the node's range on the way out.
- Other tables: nftables forwards a packet only if every base chain on the forward hook accepts it, so a chain elsewhere whose policy is drop also drops sandbox egress. Docker sets the iptables `FORWARD` policy to drop, which is such a chain. The node agent does not change other tables; when it starts it lists the forward-hook chains and logs a warning naming each one whose policy is drop.

Two details came out of review:

- *The connection mark.* A /30 is reused after its sandbox is destroyed, and conntrack entries are keyed by addresses and ports, not by interface. Without the mark, a new sandbox could take over a flow that the previous holder of the address had opened, to a destination the new sandbox was not allowed to reach. Every accepted flow now carries the generation of the sandbox that opened it, and only live (veth, generation) pairs pass.
- *The source check lives in nftables.* Strict reverse-path filtering per veth did not hold on the VM, because the host's `net.ipv4.conf.all.rp_filter` value overrides the per-interface one. The node owns its table, so the check moved there.

Hostnames in the allowlist are resolved once by the agent at create time and written to the sandbox's `/etc/hosts`. The sandbox gets no resolver: UDP is dropped and only the listed TCP destinations are accepted. That closes DNS as an exfiltration channel and makes the policy an IP-level fact that nftables can enforce. The costs: a host whose address changes during the sandbox's life still resolves, to the old pinned address, and new connections to it fail, since the allowlist holds only that address too; IPv4 only; TCP only.

Alternatives:

- *gVisor's in-sandbox packet filtering.* It is configured through the workload's capabilities, which the workload does not get, and it would put the policy inside the boundary it is meant to protect.
- *An HTTP(S) forward proxy with a domain allowlist*, which is what sandbox-runtime (`srt`) and Docker Sandboxes do, per [prior-art.md](prior-art.md). See below.

Egress sandboxes always take the cold or restore path, because the warm pool holds no-network sandboxes only (D16). A checkpoint image made without network restores into an egress sandbox; that was checked during design.

**What a proxy would add.**

- *Gains.* A policy by domain that follows changing addresses (content delivery networks), request logs, TLS SNI checks, and credentials injected outside the sandbox so the code never sees them.
- *Costs.* Only HTTP, and TLS through `CONNECT`. A service to run, scale and secure, which sees all egress traffic. The sandbox must be configured to use it, and nftables still has to force all traffic through it, so the proxy adds to the rules here rather than replacing them.
- *What would change the answer.* Allowlists of domains served from rotating addresses, or a requirement to log egress. Then a proxy behind the same nftables rules, with each sandbox allowed to reach only the proxy.

## 6. Start paths

| Path | What happens | Used in `AUTO` when |
| --- | --- | --- |
| warm | Claim a ready sandbox from the node's pool and rewrite its cgroup limits. No `runsc` call. | The pool has a ready sandbox and the request has no egress. |
| restore | `runsc create` with a fresh bundle, `runsc restore --detach` from the checkpoint image, readiness probe. | The node has a checkpoint image. |
| cold | `runsc create`, `runsc start`, interpreter start, imports, readiness probe. | Otherwise. |

`chooseMode` in [manager.go](../internal/nodeagent/manager.go) is the whole policy. `start_mode` in the request forces a path, for benchmarks; a forced path the node cannot serve fails with `FailedPrecondition` instead of silently measuring something else. In `AUTO`, a pool that emptied between the choice and the claim falls through to restore or cold, and a restore that fails in its own steps (`runsc restore` or readiness) is torn down and the cold path runs once.

**Readiness is a no-op Python run.**

- *Choice.* A sandbox is ready when a no-op `python_code` Exec (`pass` through `pyfork`) has completed in it. `pyfork` retries its connect for up to 30 s, so on a cold start this one call returns exactly when the zygote starts listening; a restored zygote is listening already. The bound is 30 s, and 5 s after a restore.
- *Why.* One definition for all three paths makes their latencies comparable, and it tests the whole Exec path rather than "the container is running".
- *Alternative.* Wait for the zygote's `READY` line in the container log (only a cold start prints it), or poll the socket from outside (not possible through gVisor without an Exec).
- Two latencies are recorded for each start: *ready* (the node receives Create until the sandbox is ready) and *first result* (the client sends Create until it has the exit event of a `print("hello")` Exec).

### 6.1 The checkpoint image

*What it contains.* `runsc checkpoint` saves the state of the running sandbox: the sentry's kernel state and the memory of the processes inside it. Here that is the zygote with its imports done, its heap frozen and its socket listening, plus whatever its tmpfs mounts hold (a file written to `/work` before a checkpoint was there after a restore, observed during design). It does not contain the root filesystem, which is the shared read-only directory; the image is only valid for the rootfs build that made it.

*How it is built* ([image.go](../internal/nodeagent/image.go)). On first start, a node agent that finds no image under `--checkpoint-dir/<key>/` takes an exclusive flock on `<key>.lock`, cold-starts a sandbox, waits until it is ready, and runs `runsc checkpoint` into `<key>.tmp-<pid>`. It then restores one sandbox from that directory to see that it works, writes a `.complete` marker (runsc version, rootfs ID, primed set), fsyncs every file and the directory, and renames it to `<key>`. The agents on one VM share the directory: the others wait for the lock and then find the image published. A node that gave up waiting looks again at most once a second.

*How it is used.* `runsc create` with a fresh bundle, then `runsc restore --detach --image-path`. The new bundle may differ from the checkpointed one in cgroup path, limits and network namespace (all three were tried during design); its process section may not. Many sandboxes can be restored from one image at once.

**D36. What a checkpoint image is valid for, and what restored sandboxes share.**

- *Why.* A stale or broken image must never be used: every node would keep failing its restores, or worse, restore a process that does not match its bundle. And tenants must not share random streams.
- *The key.* The first 16 hex characters of a sha256 over: the `runsc` version, the rootfs build ID, the primed set, a digest of the bundle's process section and mount list, and `runsc`'s global flags. On restore the process's credentials, rlimits and environment come from the image, not from the new bundle, so a change to any of those must produce a different image. A rootfs without a build ID gets no image at all.
- *Publishing.* Only after fsync and one successful test restore, so a crash in the middle of a build cannot leave a published image that every node keeps failing to restore.
- *Dropping.* A failed `runsc restore` turns restore off on that node until the agent restarts; so do three restored sandboxes in a row that fail their readiness probe (one slow probe on a loaded node is not a bad image). The image stays on disk.
- *What restored sandboxes share.* They start from the same interpreter memory. CPython reseeds `random` after `fork`; the zygote also reseeds numpy's global random state in every worker, so no two Execs, in one sandbox or across tenants, draw the same numpy random sequence. Still shared, and listed as a known limit: the string hash seed (a tenant could precompute hash collisions against another tenant's dictionaries, bounded by that tenant's own CPU limit and timeout), and the interpreter's address layout and stack canaries, which matter only to an attacker who already has a memory-corruption path into a process in its own sandbox. Android's zygote has the same property.
- *Alternative.* One image per tenant, so nothing is shared, at the cost of one build and one image per tenant; or no checkpoint at all.
- *What would change it.* Tenants that must not share any interpreter secret. Then per-tenant images, or a restore followed by a fresh interpreter, which gives up most of what the restore saves.

**D17. One image per node class, built locally, never moved.**

- *Choice.* Each machine (here, the one VM) builds its own image and never copies it.
- *Why.* There is one machine. A restore on another machine needs the CPU features that were present at checkpoint time (per prior-art), so moving images is a distribution problem of its own.
- *Alternative.* Build once per (runtime version, rootfs, CPU family) and distribute the image like a container layer.
- *What would change it.* More than one machine; [section 12](#12-not-built) lists what distribution needs.

### 6.2 The warm pool

**D16. The pool holds no-network sandboxes of the default size, and a claim rewrites their limits.**

- *Choice.* Each node keeps `--warm-pool-size` (default 2) ready sandboxes with the default spec (500m CPU, 256 MiB) and no network ([pool.go](../internal/nodeagent/pool.go)). A claim takes the oldest one, checks with signal 0 that its process is alive, rewrites `cpu.max` and `memory.max` in its cgroup to the request, and puts it in the table under the requested sandbox ID. There is no `runsc` call on the claim path.
- *Accounting.* Pool sandboxes do not count against the node's schedulable CPU and memory until claimed (an idle zygote uses no CPU), but each one counts against `max_sandboxes`, and they give way to real work: the pool does not refill on a node at `max_sandboxes`, and a Create that finds the node full takes the place of a pool sandbox.
- *Why any size.* The limits live in the host cgroup and can be changed after boot; the kernel accepted the rewrite during design.
- *Why no egress.* Egress needs a namespace and a veth per sandbox, chosen at create. A pooled sandbox would have to carry one and receive its rules at claim time. That is possible, since the rules are on the host side of the veth, but it is not built.
- *A wrinkle.* The sentry reads its memory total at boot, so a claimed sandbox enforces the requested limit while `/proc/meminfo` inside it shows the pool default.
- *Alternative.* One pool per size and network mode, which holds more idle memory.
- *What would change it.* A large share of egress requests (a pool with namespaces made ahead of time), or programs that size themselves from `/proc/meminfo`.

**Why the pool refills by restore.**

- *Choice.* A refill loop starts one pool sandbox at a time: by restore when the node has an image, cold otherwise.
- *Why.* A restore is the cheaper of the two start paths, and it gives an interpreter in exactly the state the checkpoint holds. A warm sandbox is therefore a restore done ahead of time, and the warm and restore paths start from the same state. Refilling cold would redo the imports for every pool sandbox, and that CPU competes with real work on the VM's shared vCPUs.
- *Alternative.* Refill cold (no image needed), or keep no pool and restore on every request. Without a pool the restore is on every request's path; with one it moves off that path until a burst empties the pool.
- *What would change it.* If a restore were as fast as a claim, the pool would only cost memory. The start benchmark measures exactly that difference.

## 7. Scheduler

### 7.1 Three packages

**D6. `sched` decides, `cluster` remembers, `gateway` talks.**

- *Choice.* [internal/sched](../internal/sched/policy.go) is the placement policy: pure functions over node snapshots, with no I/O, no clocks and no goroutines. [internal/cluster](../internal/cluster/state.go) is the state: one mutex, an injected clock, no I/O. [internal/gateway](../internal/gateway/gateway.go) holds the gRPC services, the node calls, the retries and the goroutines. All three ship as one binary.
- *Why.* The policy can be unit-tested and run against 1,000 synthetic nodes in `schedsim` with no network, and the walkthrough can show one request crossing each boundary.
- *Alternative.* One package, or separate processes.
- *What would change it.* The Exec proxy becoming the load: the gateway would then become its own stateless tier (D8) while `sched` stays as it is.

### 7.2 API and transport

**D7. gRPC on both hops, plaintext.**

- *Choice.* One proto package, `gss.v1`, with three services: SandboxService (clients to the scheduler), NodeService (the scheduler to nodes), ClusterService (nodes to the scheduler). Exec is a server stream. Messages may be up to 20 MiB, so that 16 MiB of `python_code` plus stdin fits.
- *Why.* One interface definition for both hops, and a server stream is the natural shape for command output. Plaintext because everything is on loopback in one VM.
- *Alternative.* HTTP and JSON with chunked responses for output; mTLS from the start.
- *What would change it.* Nodes on other machines: then mTLS with a certificate per node and rotation ([section 12](#12-not-built)). Browser clients would need gRPC-Web or plain HTTP.

**D8. The scheduler proxies Exec.**

- *Choice.* Clients send Exec to the scheduler, which opens the node's Exec stream and relays it.
- *Why.* Clients talk to one address, and nodes do not need to be reachable from clients.
- *Cost.* The scheduler process is in the data path for every output stream.
- *Alternative.* Return the node's address and let the client connect directly, as a redirect. It removes the hop, but exposes nodes to clients and puts authentication on every node.
- *What would change it.* Output volume or client count high enough that the scheduler becomes the bottleneck. Because `gateway` is separate from `sched`, the proxy can move to its own stateless tier without touching placement.

### 7.3 State: in memory, rebuilt from heartbeats

**D18. No database; the scheduler's state is soft.**

- *Choice.* Everything lives in memory behind one mutex in `cluster.State`: nodes (ID, group, address, capacity, last heartbeat, state, allocation, sandbox count, warm count, health) and sandboxes (ID, spec, node, state, incarnation, placement record). The scheduler's own record of what it placed is the source of truth for allocation, and heartbeats reconcile it ([section 8.3](#83-reconciliation-on-every-heartbeat)).
- *Why.* Everything the scheduler knows can be rebuilt from heartbeats, because each heartbeat lists the node's sandboxes with their specs, incarnations and remaining TTLs. After a restart, nodes get `NotFound` on their next heartbeat, register again and report at once, and the table refills by adoption. That is the shape of the kubelet and the API server, minus etcd.
- *What a restart loses.* Requests in flight, and the list of recently destroyed IDs.
- *Alternative.* A persistent store, or a replicated log.
- *What would change it.* High availability. Two schedulers need a leader: a lease in a small consistent store, with the standby rebuilding its state from heartbeats exactly as a restarted scheduler does. Audit requirements would need a durable log as well.

Sandbox states: a create adds the sandbox as `PENDING`; `Reserve` moves it to `STARTING` at the next incarnation; `Commit` moves it to `READY`. A dead node or missing reports move it to `LOST`, and the rescheduler takes it from `LOST` back to `STARTING` at a higher incarnation. `FAILED` and `DESTROYED` are final, and their IDs are remembered (tombstones) for the longest TTL plus 10 s, 60 min 10 s: as long as a copy can live on a node, whose own reaper fires at the sandbox's TTL plus 10 s. A report of a destroyed sandbox in that time is answered with a destroy instead of being adopted.

### 7.4 Registration, heartbeats and one failure threshold

Agents call `Register` at start with their ID, group, address, capacity and `max_sandboxes`, then `Heartbeat` every second; the scheduler sets the interval in its answer. Each heartbeat carries the live sandboxes and the number of warm sandboxes ready; the answer carries the IDs the node must destroy. The failure detector runs every 250 ms: a ready node with no heartbeat for 3 s (`--node-timeout`) is dead. It is removed from placement, and every `STARTING` or `READY` sandbox on it goes to `LOST` and to the rescheduler. Detection takes at most the timeout plus one detector tick.

**D19. One threshold, not two.**

- *Choice.* A node is ready or dead; there is no "suspect" state.
- *Why.* Here the price of a false positive is a restarted sandbox. The health score already steers new work away from flaky nodes, and one threshold keeps the recovery-time measurement easy to read.
- *Alternative.* A suspect state that only stops new placements, then eviction after a longer timeout. That is what a production system wants, to ride out pauses without moving work, and it is what Kubernetes does (a node goes NotReady, and its pods are evicted later, after their tolerations run out).
- *What would change it.* Long-lived sandboxes whose state is expensive to lose, or nodes that pause for longer than the timeout. Then two thresholds, and possibly a node that fences itself when it cannot reach the scheduler.

### 7.5 Filters

Placement has the kube-scheduler shape: filters remove nodes that cannot take the request, then scorers rank the rest ([filters.go](../internal/sched/filters.go), [scorers.go](../internal/sched/scorers.go)).

| Filter | Rejects a node when |
| --- | --- |
| `Excluded` | the gateway has excluded it for this placement: a node that already failed this request, or whose room was taken since the snapshot |
| `NodeReady` | it is not ready |
| `NodeGroup` | the request names a group and the node is in another |
| `Capacity` | `req.cpu > cap.cpu - alloc.cpu`, or `req.mem > cap.mem - alloc.mem`, or `sandboxes >= max_sandboxes` |

`Capacity` compares against the free amount rather than adding the request to the allocation, so a huge request cannot overflow an `int64`. If no node passes, the error says how many nodes each filter rejected first, largest first.

A filter returns a bool, and the rejection reason is the filter's name. The alternative, a free-text reason per node, would be more specific; but the per-filter counts already explain a failed placement, and the placement record has no field for per-node reasons.

### 7.6 Scorers and weights

| Scorer | Weight | Score, 0 to 100 |
| --- | --- | --- |
| `BinPack` | 1.0 | `100 * ((alloc.cpu + req.cpu) / cap.cpu + (alloc.mem + req.mem) / cap.mem) / 2`: the mean of CPU and memory utilisation after placing the request. Fuller nodes score higher. |
| `WarmPool` | 1.0 | 100 if the request can use a warm sandbox (no egress, start mode `AUTO` or `WARM`) and the node reports at least one, else 0. |
| `GroupAffinity` | 0.5 | `100 * live sandboxes with the same affinity_key in this node's group / live sandboxes with that key`; 0 when the key is empty or unused. |
| `NodeHealth` | 1.0 | `100 * health`. Health starts at 1 and after each node Create call becomes `0.8 * health + 0.2 * outcome`, with outcome 1 for success and 0 for failure (an exponentially weighted success rate, alpha 0.2). A refusal is not counted: `FailedPrecondition` (a start mode the node cannot serve), `ResourceExhausted` (no room by the node's own count), `InvalidArgument` or `AlreadyExists`. |

The total is the sum of weight times score; the highest total wins, and ties go to the lowest node ID. `Place` also returns, for tracing and for `gssctl explain`, the number of nodes considered, the number that passed the filters, and the weighted per-plugin scores of the top three candidates. The spread policy has one scorer, `LeastAllocated = 100 - BinPack`. `schedsim` also runs a first-fit policy (no scorers; the first feasible node wins) and a random one, as baselines.

**D21. Equal weights for packing, warm hit and health; affinity at half.**

- *Choice.* The weights above, each a scheduler flag (`--w-binpack`, `--w-warm`, `--w-affinity`, `--w-health`).
- *Why.* A warm hit saves a restore; a full node saves money. With equal weights an empty node with a warm sandbox beats a nearly full node without one, so latency wins when the two disagree, and when every node has a warm sandbox the warm term cancels and packing decides. A worked example with the local cluster's defaults (nodes of 4000m and 2048 MiB, a request of 500m and 256 MiB, health 1, no affinity key):
  - an empty node with a warm sandbox: BinPack 12.5 + WarmPool 100 + NodeHealth 100 = 212.5;
  - a node 75 % allocated with no warm sandbox: BinPack 87.5 + 0 + 100 = 187.5. The empty node wins.
  - If the fuller node also had a warm sandbox, it would score 287.5 and win: packing decides.
  - A node whose health has fallen to 0.7 loses 30 points, which outweighs most packing differences.
- *Alternative.* Packing first, with the warm score as a tie-breaker; or a weight learned from measured costs.
- *What would change it.* The start benchmark. If the restore path turned out nearly as fast as a warm claim, the warm weight should drop, and packing should win more often.

### 7.7 Bin-packing or spreading

**D20. Bin-packing is the default.**

- *Choice.* The default policy packs. `--policy spread` switches to `LeastAllocated`.
- *Why.* Sandboxes are short and many. Packing leaves whole nodes empty, which is what lets a fleet scale down and keeps room for large requests.
- *Cost.* Blast radius: a full node that dies takes more sandboxes with it. The tools for controlling that here are the `NodeGroup` filter (hard) and `GroupAffinity` (soft). `schedsim` reports both policies, plus first-fit and random, so the trade is a number in `docs/results.md`.
- *Alternative.* Spread by default, as Kubernetes does (`NodeResourcesFit` scores with `LeastAllocated` unless configured otherwise).
- *What would change it.* Long-lived sessions, where a node loss hurts more than an extra node costs, or a fleet of fixed size, where empty nodes save nothing.

### 7.8 Sampling

**D22. Score every feasible node by default; sampling is an option.**

- *Choice.* `Policy.SampleSize` (`--sample-size`) is 0, which means score all. With a value *k*, `Place` stops after *k* feasible nodes, scanning from an offset that rotates between calls, so successive placements look at different parts of the fleet. `Place` is serialised by a mutex, because the offset and its scratch buffers live in the policy, and it allocates nothing per node.
- *Why.* Measure before optimising. At four nodes sampling changes nothing. `schedsim` reports decisions per second and packing with and without sampling (*k* = 100 against 1,000 nodes), which is the evidence for when sampling starts to matter.
- *Alternative.* kube-scheduler's `percentageOfNodesToScore`, or E2B's approach of sampling K ready nodes and taking the least loaded (per prior-art).
- *What would change it.* Placement time becoming large next to start time at a given fleet size. Then sample, and accept slightly worse packing.

### 7.9 Binding and retries

`placeAndStart` in [gateway.go](../internal/gateway/gateway.go):

1. `place` runs the policy over a snapshot of the state (`cluster.State.View`).
2. `Reserve` re-checks capacity under the state's mutex, charges the request to the chosen node, moves the sandbox to `STARTING` at the next incarnation, and returns the TTL left. The incarnation counts placements, not successful starts: a fresh create whose first node Create failed and that was placed again can be `READY` at incarnation 2 or 3, and a reschedule raises it again.
3. The gateway calls the node's `CreateSandbox` with that incarnation and the remaining TTL, bounded by 60 s.
4. On success: the node's health goes up, and `Commit` moves the sandbox to `READY` if it is still `STARTING` at that incarnation. If it is not (it was destroyed, expired or lost while it started), the gateway destroys that incarnation on the node. A commit of a warm start lowers the node's cached warm count, so the next placement does not count on the same warm sandbox before the next heartbeat.
5. On failure: `Unreserve` releases the room, the node's health goes down (except after a refusal: `FailedPrecondition`, `ResourceExhausted`, `InvalidArgument` or `AlreadyExists`), the node is excluded, and the gateway places again, up to 3 failed node Creates. A `Reserve` conflict (another create took the room since the snapshot, or the node died) excludes the node without counting as a failed attempt. A forced start mode the node cannot serve (`FailedPrecondition`) on a client's create is returned at once and does not lower the node's health.

**Reserve before calling the node.**

- *Why.* The snapshot does not change until the node answers, so concurrent requests would otherwise all pick the same best node.
- *Alternative.* Optimistic binding: the node rejects when full and the scheduler retries. That means more failed calls under load. kube-scheduler's assume cache and Reserve extension point exist for the same reason. The node still admits each request against its own declared capacity, as a second check.
- *What would change it.* Node Creates slow enough, at a request rate high enough, that the scheduler should not hold a request open while the node works. Binding would then be asynchronous: return `STARTING` and let the client wait for `READY`, as Kubernetes does with pod status.

## 8. Failure handling

### 8.1 Node loss

When the detector declares a node dead ([reschedule.go](../internal/gateway/reschedule.go)):

1. Its `STARTING` and `READY` sandboxes go to `LOST`, and their allocations are released.
2. The rescheduler takes each one, at most 8 at once, and places it again with the same ID, the same spec, the next incarnation and the TTL that is left. The dead node fails `NodeReady`, so it is not chosen.
3. An Exec that arrives while its sandbox is `LOST` or `STARTING` waits for `READY`, bounded by the request's deadline and a cap of 15 s, and then runs on the new incarnation.
4. A sandbox that cannot be placed (its group constraint names the dead group, or no node has room) stays where it is and is tried again every second until its TTL ends.

**D23. A rescheduled sandbox is a fresh sandbox.**

- *Choice.* Its files and interpreter state are gone. The client learns this from the `incarnation` field, which went up.
- *Why.* This is the Pod model: identity and spec survive, memory does not. Keeping state would need periodic checkpoints shipped off the node, which costs a pause per checkpoint and storage per sandbox, and a restore elsewhere needs matching CPU features.
- *Alternative.* Periodic checkpoints to shared storage, restored on the new node; or live migration.
- *What would change it.* Long-lived, stateful sessions whose users would rather wait than start over.

**At most 8 reschedules at once.** The surviving nodes should not be hit by every restart in the same instant, which matters more here because every node shares the same vCPUs. The alternative, no cap, finishes sooner when nodes have room and overloads them when they do not. The cap is `--reschedule-concurrency`.

### 8.2 Execs in flight

**D37. A proxied Exec also watches the scheduler's own state.**

- *Choice.* While it relays a node's Exec stream, the gateway checks the state every 250 ms and ends the stream with `Unavailable` as soon as the sandbox is no longer `READY` on that node at that incarnation, a destroy or a TTL expiry included ([gateway.watch](../internal/gateway/gateway.go)). Keepalive on the node connections stays at 10 s.
- *Why.* The first design relied on gRPC keepalive pings every 2 s to notice a node that vanished without closing its connection. grpc-go raises any client ping interval below 10 s to 10 s, so keepalive alone would leave such an Exec hanging for 10 s or more. The state watch bounds it by the 3 s failure threshold.
- *Alternative.* Application-level pings on the stream, or a node-side push.
- *What would change it.* Nothing in grpc-go's floor is likely to; the watch is also what tells an Exec that its sandbox moved, which keepalive cannot.

### 8.3 Reconciliation on every heartbeat

For each heartbeat the scheduler compares the node's list with its own table ([heartbeat.go](../internal/cluster/heartbeat.go)):

- Reported and tombstoned (destroyed or failed within the last 60 min 10 s, the longest TTL plus the node reaper's 10 s): put its ID in the answer's destroy list.
- Reported and unknown: *adopt* it as `READY` on that node, with the TTL the node reports. This is what rebuilds the table after a scheduler restart.
- Reported, but the scheduler has it on another node, at another incarnation, or as `PENDING` or `LOST`: put its ID in the answer's destroy list.
- In the table as `READY` on this node but missing from two heartbeats in a row: `LOST`, then rescheduled. That covers a sandbox that died on its own, for example a sentry killed for memory.

Two rules came from review:

- *A destroy carries an incarnation* (0 means whatever is live). An ID-only destroy from a stale view could remove a newer incarnation that had landed on the same node. The node's heartbeat loop destroys only the incarnation it reported. A client's `DestroySandbox` takes one too, with the same rule: a sandbox at a newer incarnation than the one named is left alone, and the call succeeds.
- *A `LOST` sandbox reported again by its old node is destroyed there and rescheduled fresh, not restored to `READY`.* That keeps `Commit` the only way to `READY` and keeps the incarnation contract simple. The cost: a node that was unreachable only briefly loses sandboxes that were still alive.

### 8.4 A partition that heals

If a dead node comes back (it was a partition, not a crash), its next heartbeat makes it ready again with its allocations reset, and lists sandboxes that the scheduler has already moved. The answer tells it to destroy them: a moved sandbox that was then destroyed keeps its tombstone for the longest TTL plus 10 s, as long as a copy can live on a node. If the node had lost contact for longer than a sandbox's TTL, its own reaper has already destroyed the sandbox at TTL plus 10 s.

### 8.5 Agent restart

A restarted agent is an empty node (D13). Its `READY` sandboxes are missing from two heartbeats, go to `LOST` and are rescheduled. An Exec that was running ends when the connection breaks, or when the node answers `NotFound` for a sandbox the scheduler still has as `READY`; the gateway turns that into `Unavailable` ("the node no longer has it").

### 8.6 Scheduler restart

The state is soft (D18). Nodes get `NotFound` on their next heartbeat, register again, and report at once. Each reported sandbox is adopted with the TTL the node reports. After a restart the first node to report a sandbox is the one adopted, so if a healed node with a stale copy reports before the node holding the newer incarnation, the adopted entry moves to the node reporting the higher incarnation and the stale copy is destroyed; a sandbox this scheduler placed itself is never moved that way. On shutdown the scheduler stops failure detection first: a draining gRPC server takes no new calls, heartbeats included, and a detector still running would declare healthy nodes dead. Reschedules already running are given a drain timeout to finish.

### 8.7 The local failure-domain test

[scripts/group-loss.sh](../scripts/group-loss.sh) and `loadgen grouploss`:

- Two node groups, `a` and `b`, two nodes each, in the one VM. Twelve sessions (long-lived sandboxes with no group constraint), each running `print(1)` every 200 ms. After a 15 s warm-up, every node in group `b` is killed (SIGKILL and `cgroup.kill`), and the sessions run on for 45 s.
- Recorded: *detection time* (kill until the scheduler marks the last `b` node dead); *time to recover* (kill until every affected session has completed an Exec on its new incarnation); *requests lost* (Execs that returned an error), once without client retries and once with up to 3 retries on `Unavailable` or `DeadlineExceeded`, each on a fresh cluster.

**D35. The local failure-domain test runs the scheduler with the spread policy.**

- *Choice.* `group-loss.sh` starts the scheduler with `--policy spread`; every other benchmark uses the default.
- *Why.* The sessions start in `AUTO` mode with no egress, so the `WarmPool` score applies at the same weight as `BinPack`, and every warm commit lowers the scheduler's cached warm count for that node until its next heartbeat refreshes it. With every pool full and no heartbeat in between, the creates go two to each node in turn (`a1`, `a1`, `a2`, `a2`, `b1`, `b1`, `b2`, `b2`) and the rest by packing. Under the default policy the split between the groups therefore depends on when each pool refills and when each heartbeat arrives, not on the recovery being measured, and the number of sessions the test loses would vary from run to run. (`loadgen` refuses a run in which a group got no session.) Spread splits them evenly across the four nodes on every run, and the test then measures how the scheduler detects and recovers from losing half of them.
- *Alternative.* Keep the default policy and constrain half the sessions to group `b` with `node_group`. But that is a hard constraint: those sessions could never move to group `a`, and would wait for the dead group until their TTL.
- *What would change it.* A question about what the default policy does under failure. That is a different test, and its answer would carry the same timing dependence, so it would need many runs rather than one.

It is a local failure-domain test. Both groups share one kernel, one disk and one laptop. It exercises the scheduler's detection and rescheduling logic and says nothing about networks, regions or providers; the results table carries that sentence.

## 9. Mapping to Kubernetes

The same system built on Kubernetes would keep the ideas and swap most of the parts:

| This repository | Kubernetes |
| --- | --- |
| The node agent calls `runsc` with an OCI bundle | A RuntimeClass with `handler: runsc`; the kubelet calls containerd through CRI, and containerd runs `runsc` through the gVisor shim. RuntimeClass also has a `scheduling` field that keeps such pods on nodes that have the runtime. |
| `bundle.Spec` writes `config.json` | containerd generates the OCI spec from the pod spec |
| The node agent registers, then heartbeats every second | The kubelet registers a Node object, renews its Lease, and reports status |
| The heartbeat lists sandboxes; the scheduler adopts and reconciles | The kubelet reports pod status; the API server, backed by etcd, is the source of truth |
| `internal/sched` filters | Filter plugins (`NodeResourcesFit`, `NodeAffinity`, `TaintToleration`, ...) |
| `NodeGroup` filter | `nodeSelector` or required node affinity on a label |
| Scorers with weights | Score plugins, `NormalizeScore`, plugin weights in the scheduler profile. The scorers here already return 0 to 100, so there is no normalisation step. |
| `BinPack` | `NodeResourcesFit` with the `MostAllocated` scoring strategy |
| The spread policy (`LeastAllocated`) | `NodeResourcesFit` with `LeastAllocated`, the default strategy; topology spread constraints, to spread across groups |
| `GroupAffinity` | Preferred inter-pod affinity, with `topologyKey` set to the group label |
| `WarmPool` | No built-in counterpart: a custom Score plugin, or a warm pool controller such as the `SandboxWarmPool` extension of `kubernetes-sigs/agent-sandbox` |
| `NodeHealth` | No direct counterpart; the closest are taints that controllers put on unhealthy nodes |
| `cluster.State.Reserve`, `Unreserve` | The Reserve extension point (and the scheduler's assume cache), Unreserve |
| The synchronous node `CreateSandbox` call, then `Commit` | Bind writes the binding to the API server before the kubelet starts the pod. Here the node call is both the bind and the start, and `Commit` is closer to the pod reporting Ready. |
| `SampleSize` | `percentageOfNodesToScore` |
| Failure detector: one threshold of 3 s | Node lifecycle controller: the node goes NotReady after a grace period, and taint-based eviction removes its pods later, after each pod's toleration. Two thresholds (D19). |
| A rescheduled sandbox keeps its ID, incarnation plus one | A controller (ReplicaSet, StatefulSet) creates a replacement pod; a StatefulSet keeps the name |
| The scheduler's TTL, with the node's reaper as a backstop | `activeDeadlineSeconds` on the pod, enforced by the kubelet |
| The nftables egress allowlist | NetworkPolicy egress rules, enforced by the network plugin; names rather than addresses need a plugin with FQDN policies |

What Kubernetes gives that this does not: durable state in etcd and a control plane that can run several replicas; authentication, RBAC, admission control and quotas; image distribution and caching; pod networking, Services and DNS; volumes and persistent storage; priorities and preemption; cordon, drain and node upgrades; autoscaling; and the tooling around all of it.

What is first-class here and would be extra work there: choosing a start path per request, a warm-pool score in placement, and a checkpoint image per node that every node builds and checks for itself. On Kubernetes, `kubernetes-sigs/agent-sandbox` provides the warm pool as a controller and leaves placement to kube-scheduler (per prior-art).

## 10. Observability

`gssctl`, `loadgen`, `nodeagent` and `scheduler` set up OpenTelemetry through [internal/telemetry](../internal/telemetry/telemetry.go): OTLP over gRPC for traces and metrics, turned on only when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, a no-op otherwise. Only that package installs SDK providers; everything else uses the global tracer and meter named `gss`. gRPC clients and servers use the `otelgrpc` stats handlers, which carry the trace context across both hops, so one RPC is one trace from the client to `runsc`. `gssctl` starts no span of its own, so one `gssctl run` makes three traces, one per call: the create (down to the `runsc` calls on the node), the Exec, and the Destroy.

The trace of a create:

```
SandboxService/CreateSandbox                (the client's RPC span, the root)
  gateway.create
    sched.place        nodes.total, nodes.feasible, node.chosen, score.<plugin> of the winner
    NodeService/CreateSandbox
      node.create      start.mode (and start.fallback after a failed restore)
        warm path:
          pool.claim                        (no runsc call)
        restore and cold paths, in order:
          netpolicy.setup                   (egress only)
          runsc.create
          runsc.restore | runsc.start
          sandbox.ready
            runsc.exec                      (the readiness probe)
          runsc.state
```

Other spans: `gateway.exec`, `node.exec` and `runsc.exec` for an Exec; `gateway.reschedule`; `pool.fill` for a refill; `node.image` for an image build; `node.destroy` with `destroy.reason` (request, ttl, exited, retry, replaced, close, yield).

Metrics, all prefixed `gss_` ([metrics.go](../internal/telemetry/metrics.go)):

| Metric | Type | Labels, or what it counts |
| --- | --- | --- |
| `gss_start_duration_seconds` | histogram | `mode`, `node` |
| `gss_exec_duration_seconds` | histogram | `kind` (argv, python) |
| `gss_placement_duration_seconds` | histogram | |
| `gss_placement_failures_total` | counter | `reason`: the filter that rejected the most nodes, `no_nodes` or `node_error` |
| `gss_reschedules_total` | counter | reschedule attempts, retries included |
| `gss_node_deaths_total` | counter | |
| `gss_sandboxes` | gauge | `node` (live sandboxes) |
| `gss_warm_pool_ready` | gauge | `node` |
| `gss_nodes` | gauge | `group`, `state` |

The dashboard ([deploy/grafana/dashboard.json](../deploy/grafana/dashboard.json)) has nine panels: start latency p50 and p95 by mode, starts per second by mode, live sandboxes per node, warm pool per node, placement duration p95, placement failures by reason, nodes by group and state, reschedules and node deaths, and Exec duration p95 by kind. Each sandbox also carries its placement record, which `gssctl explain <id>` prints: nodes considered, nodes feasible, and the top three candidates with their weighted per-plugin scores. The binaries log one line per create, destroy, placement and failed Exec, and the systemd units put those lines in the journal (`journalctl -u gss-node-a1`).

**D24. `grafana/otel-lgtm` in one container.**

- *Choice.* `make obs-up` runs `grafana/otel-lgtm` (an OpenTelemetry collector, Tempo, a Prometheus-compatible store and Grafana in one image) as a rootless nerdctl container in the VM, with Grafana on port 3300 and the dashboard provisioned from the repository. It is a development image, and the README says so.
- *Why.* One command and one dashboard file.
- *Alternative.* Separate collector, Tempo, Prometheus and Grafana containers: four configurations that teach nothing about sandboxes.
- *What would change it.* Any real deployment: separate components with persistent storage, retention and access control.
- *Verified.* The image tag, its provisioning paths, and the label names OTLP metrics get in its Prometheus store, which the dashboard queries depend on, were checked in the VM on 2026-11-15: Grafana answers, the dashboard is provisioned, and Prometheus has every `gss_` metric with its labels.

**D25. Headline numbers are measured with the observability container stopped and OTLP export off.**

- *Choice.* Every benchmark target refuses to run when `OTEL_EXPORTER_OTLP_ENDPOINT` is set or the `gss-obs` container is running. The dashboard is shown in a separate run.
- *Why.* The container shares the VM's 4 vCPUs and would be part of every density and latency number.
- *Alternative.* Measure with observability on, and state it.
- *What would change it.* The question "what does observability cost?", which needs both runs side by side.

## 11. Benchmarks

This section says what is measured, how, and on what. The numbers are in [docs/results.md](results.md), written by `make results` from the JSON files under `results/`, and each benchmark ran once.

The results bear on these design calls: the start tables on D11 and D15 (the primed interpreter and restore) and D16 (the warm pool); the group-loss table on D19 (the failure threshold); and the placement table and the throughput table on D20 to D22 (the placement policy). They do not show that any of those calls is the best one, only what each costs on this machine.

| Benchmark | Command | What it does | What it reports |
| --- | --- | --- | --- |
| Start paths | `make bench-start` (`loadgen start`) | For each of cold, restore and warm: 30 sequential create, `print("hello")` Exec, destroy cycles with the path forced, constrained to group `a`; the first cycle of each path is a warm-up and is not recorded | p50 and p95 of *ready* and *first result* latency per path |
| Throughput | `make bench-throughput` (`loadgen throughput`) | 8 closed-loop workers for 60 s against the whole cluster, `AUTO` mode | Sandboxes per second, p50 and p95, and the share of each start path |
| Density | `make bench-density` (`loadgen density`) | One node declared with 64000m, 64 GiB and up to 200 sandboxes, no warm pool. The number of live sandboxes is raised in steps (5, 10, 20, 30, 40, 50, 60); at each step 10 restore starts and 10 Execs are measured | The highest step at which p95 start latency stayed within twice the first step's p95 with no failed start, and what gave out first |
| Placement | `make sim` (`schedsim`) | 1,000 synthetic nodes in 4 groups (20 % 8 CPU / 32 GiB, 50 % 16 CPU / 64 GiB, 30 % 32 CPU / 128 GiB), requests drawn from four sizes, fixed seed. *Fill*: place until 70 % of fleet CPU is requested. *Churn*: then 50,000 remove-one-add-one steps. Default, spread, first-fit and random policies, and the default with sampling of 100 | Placement decisions per second (one goroutine, policy only); packing efficiency (requested over capacity of the binding resource on nodes that hold at least one sandbox); share of fleet CPU on empty nodes; nodes used, next to a lower bound |
| Local failure-domain test | `make bench-grouploss` (`scripts/group-loss.sh`) | [Section 8.7](#87-the-local-failure-domain-test) | Detection time, time to recover, requests lost, without and with client retries |

`make bench` first records the machine, as `make hardware` does, then runs them all in order and stops at the first failure; each benchmark that needs a cluster starts from a fresh one. The density step sizes, worker counts and durations above are the commands' defaults.

*Hardware class.* One laptop: an Apple M3 Pro (arm64), with one Lima VM (Apple Virtualization.framework, Ubuntu 24.04 arm64, 4 vCPU, 6 GiB). gVisor is `runsc` release-20261102.0 on the systrap platform; Go 1.27.4.

*How to read the tables, once they exist:*

- One machine. Every node is a process in the same VM, sharing its 4 vCPUs with the other nodes and with the load generator, so a number for "one node" is a number for one node with neighbours.
- Node capacities are declared, not measured; the density benchmark declares far more than the VM has on purpose, so that the VM, not the flag, is what gives out.
- systrap only; no other gVisor platform was tried.
- The start latencies are for the named primed set (numpy and pandas included) and say nothing about other Python code or packages.
- Observability was off (D25).
- One run each: no repeats and no confidence intervals, so small differences between rows are not evidence.
- The local failure-domain test runs the spread policy (D35) and says nothing about networks, regions or providers.
- `schedsim` measures the policy alone; it says how fast and how well the policy places, not how fast a placement is end to end.

**D26. Results are rendered by a program from committed JSON.**

- *Choice.* Each benchmark writes a JSON file under `results/` with its command, parameters, start time and every series. `make results` ([cmd/mkresults](../cmd/mkresults/main.go)) renders `docs/results.md` and the README's block between `<!-- results:begin -->` and `<!-- results:end -->` from those files and `results/hardware.json`. Each table row names its command and each table its hardware. No number is typed by hand.
- *Why.* Every number can be traced to a command and a file, and re-rendering after a new run cannot leave a stale number behind.
- *Alternative.* Copying numbers into the documents by hand, which goes stale and cannot be checked.
- *What would change it.* Results from more than one machine, which would need a machine column and a file per machine.

## 12. Not built

Nothing in this repository is multi-cloud, and nothing has run on any cloud or on more than one machine. Real multi-cloud placement of sandboxes would add at least the following:

- *Identity federation between providers.* Nodes in one provider must prove who they are to a control plane in another, without long-lived shared secrets: workload identity from each provider, federated into one trust domain.
- *mTLS across networks, and certificate rotation.* Today every hop is plaintext on loopback (D7). Across networks every hop needs mutual TLS, a certificate authority, short-lived certificates and rotation that does not drop running Exec streams.
- *A control plane that survives the loss of a provider.* The scheduler is one process with soft state (D18). It would need replicas in more than one provider, leader election, and a way to rebuild its state from nodes in whichever providers are left.
- *Image distribution per CPU family.* A checkpoint image restores only on a CPU with the features present at checkpoint time (D17), and providers offer different CPU families. Images would be built per (runtime version, rootfs, CPU family), stored in each provider, and the placement policy would have to know which nodes can restore which image.
- *Egress policy across providers.* The nftables allowlist is per node (D12). Across providers, egress addresses, NAT and firewall rules differ per provider, and the allowlist would need a single source that each provider's network enforces.
- *Cost-aware scoring.* `BinPack` treats every node as equally priced. Across providers, prices, data transfer charges and spot capacity differ; a scorer would weigh them, next to latency to the client and data locality.

Why the local failure-domain test says nothing about these: it kills processes in one VM. Both groups share one kernel, one disk, one clock, one network stack and one laptop. There is no network partition between them, no added latency, no separate identity or certificate, no separate image store and no separate bill. It shows that the scheduler detects dead nodes and restarts their sandboxes on the others; it does not show that any of the above would work.

## 13. Known limits and what to do next

### Not verified yet

- The name of `runsc`'s control socket in the runsc root is taken from gVisor's source and has not been confirmed on this release. If it is wrong, removing the root at shutdown fails safe: an entry it does not recognise is left in place, and the error names it.

### Isolation and the sandbox

- Restored sandboxes share the string hash seed, address layout and stack canaries of the interpreter (D36).
- A process that leaves its process group can outlive an Exec timeout until the TTL or a Destroy; a background process that holds stdout or stderr open delays the end of the Exec stream by up to 2 s.
- A program over its memory limit usually ends the sandbox (D32).
- A claimed pool sandbox shows the pool's default memory in `/proc/meminfo` (D16).
- The rootfs `FROM` tag is not pinned to a digest. `make rootfs` swaps the tree with two renames and removes the old one at once; it refuses while a `gss-*` systemd unit is active, but it does not see agents started outside systemd (the integration tests start their own), and `FORCE=1` skips the check.
- Old checkpoint images for keys no longer in use are never removed.
- If `runsc --version` fails when an agent starts, the image key stays empty and the node starts sandboxes cold until it restarts.
- The zygote treats a timeout or environment that is a falsy non-number (an empty string, for example) as absent; `pyfork` never sends one.

### Network

- Allowlisted hostnames are pinned at create time; IPv4 and TCP only; no DNS (D12); at most 64 allowlist entries.
- A drop policy on another forward-hook chain, such as the one Docker sets, drops all sandbox egress. The agent only warns about it at start; a chain added later, or one whose rules drop the traffic, is not seen.
- Egress sandboxes never come from the warm pool (D16).
- The veth name carries 16 bits of the node prefix; a collision between two nodes is detected and refuses the second node, so it fails closed.
- The rule that drops traffic between two sandbox veths is covered by the text of the generated rules and a counter, not by a live test with crafted packets; the source-address check is tested with an ordinary connect from a second address.
- The network policy's Setup and Teardown for the same sandbox could interleave if called at once; the manager never does that, because it serialises the lifecycle of each sandbox.

### Node agent

- Liveness is checked with signal 0 to the sandbox's pid, which counts a reused pid as alive; a pidfd would not.
- The liveness loop retries broken entries every pass, with no backoff. A broken pool sandbox counts against CPU and memory like other broken entries.
- A second `Close` returns nil even if entries are left, and stopping the background loops is not bounded by `Close`'s context.
- The node keeps no state across restarts (D13).

### Scheduler

- One process, no high availability, and it is in the data path of every Exec (D8, D18).
- A restart loses requests in flight and the tombstones of recently destroyed IDs; an adopted sandbox's spec shows the TTL that was left at adoption.
- One failure threshold (D19); a node that was only briefly unreachable loses its sandboxes (section 8.3).
- `DestroySandbox` skips the direct node call for a `LOST` sandbox; in a narrow window that can skip one destroy that the next heartbeat then performs.
- The placement record keeps nodes considered, nodes feasible and the top three; per-filter rejection counts appear only when no node fits, so `gssctl explain` cannot say why other nodes were filtered out of a successful placement.
- Health counts only node Create results.
- Tombstones are kept for 60 min 10 s, so the table holds every sandbox destroyed in that time (they are not listed, but `GetSandbox` returns them).
- When a node reports a sandbox at a different incarnation from the scheduler's, that report keeps the scheduler's copy from counting as missing; once the node has destroyed the stale copy the count starts, so detection is delayed, not lost. Allocation releases are clamped at zero, which would hide a double release.

### Performance

- Under heavy load the restore path's p95 grows (see the throughput and density tables in [docs/results.md](results.md)), because the nodes share the VM's 4 vCPUs with each other and with the load generator.

### Observability and tooling

- Each metric instrument is looked up on every call instead of once.
- A placement that fails with an empty rejection map is labelled `no_nodes`.
- The observability container publishes its OTLP ports on every interface of the VM.
- `schedsim`'s table has no column for the fill level reached; it is in the JSON only. The throughput rows print 0.0 when a run had no successful cycle (a true zero, but easy to misread).
- `bundle.Spec` does not validate its input; only `bundle.Write` does.

### Tests that are thin

- Placement: first-rejection attribution with a node that fails two filters; sampling with infeasible nodes inside the window and with wrap-around; the random policy's test checks only that it is reproducible.
- Node manager: no randomised concurrency stress test, no test of `Close` during a `Create`, and one destroy-during-create test uses a non-blocking select that could flake.
- Cluster state: the TTL test's "released exactly once" assertion cannot fail on a double release (the clamp hides it), and the concurrent invariant test checks only at the end.
- `schedsim`: the guard against an under-filled run has no test, and the table's column order and JSON keys are not checked.

### What to do next

1. Push the repository and let both CI jobs run; record which ones passed in the README.
2. Re-read D21 (weights) and D16 (pool size) against the measured start latencies.
3. mTLS between the scheduler and the nodes, with node certificates; then authentication for clients.
4. A suspect state before death (D19), and a node that fences itself when it cannot reach the scheduler.
5. Leader election over the same soft state, so a second scheduler can take over.
6. pidfd-based liveness; image garbage collection; a pinned base image digest.
7. A warm pool with network namespaces made ahead of time, so egress sandboxes can be claimed.
8. Measure gVisor's KVM platform on a host that exposes `/dev/kvm`.
9. Periodic checkpoints shipped off the node, for sessions that should survive node loss (D23).

## Index of calls

| Call | Short form | Where |
| --- | --- | --- |
| D1 | Repository name `gvisor-sandbox-scheduler` | [1](#names-and-prior-art) |
| D2 | One Lima VM defined in the repository | [1](#where-it-runs) |
| D3 | Nodes are processes in that VM, with declared capacity | [1](#where-it-runs) |
| D4 | systrap platform only | [3](#3-platform-systrap-kvm-or-ptrace) |
| D5 | No emulation in any benchmark | [1](#where-it-runs), [3](#3-platform-systrap-kvm-or-ptrace) |
| D6 | `sched`, `cluster`, `gateway` split | [7.1](#71-three-packages) |
| D7 | gRPC on both hops, plaintext | [7.2](#72-api-and-transport) |
| D8 | The scheduler proxies Exec | [7.2](#72-api-and-transport) |
| D9 | Drive the `runsc` CLI | [4.1](#41-driving-runsc) |
| D10 | Shared read-only rootfs, writable tmpfs at `/work` and `/tmp` | [4.2](#42-root-filesystem) |
| D11 | Zygote and `pyfork` with descriptor passing | [4.4](#44-the-primed-interpreter) |
| D12 | Egress through netns, veth and an nftables allowlist; hosts pinned, no DNS | [5](#5-network) |
| D13 | The node agent keeps no state across restarts | [4.8](#48-lifetime-and-agent-restarts), [8.5](#85-agent-restart) |
| D14 | Rootfs from a Dockerfile, built with rootless nerdctl | [4.2](#42-root-filesystem) |
| D15 | The primed set includes numpy and pandas | [4.4](#44-the-primed-interpreter) |
| D16 | The warm pool holds no-network sandboxes; limits rewritten at claim | [6.2](#62-the-warm-pool) |
| D17 | One checkpoint image per node class, not distributed | [6.1](#61-the-checkpoint-image) |
| D18 | Scheduler state in memory, rebuilt from heartbeats | [7.3](#73-state-in-memory-rebuilt-from-heartbeats), [8.6](#86-scheduler-restart) |
| D19 | One failure threshold: 3 s without a heartbeat | [7.4](#74-registration-heartbeats-and-one-failure-threshold) |
| D20 | Bin-packing is the default | [7.7](#77-bin-packing-or-spreading) |
| D21 | Equal weights for packing, warm hit and health; affinity half | [7.6](#76-scorers-and-weights) |
| D22 | Score all feasible nodes; sampling optional | [7.8](#78-sampling) |
| D23 | A rescheduled sandbox starts fresh | [8.1](#81-node-loss) |
| D24 | `grafana/otel-lgtm` in one container | [10](#10-observability) |
| D25 | Benchmarks run with observability off | [10](#10-observability), [11](#11-benchmarks) |
| D26 | Results rendered by a program from committed JSON | [11](#11-benchmarks) |
| D27 | One Makefile for macOS (through Lima) and Linux | [1](#where-it-runs) |
| D28 | The prior-art file names existing projects and products | [1](#names-and-prior-art) |
| D29 | Short prefix `gss`, not `sbx` | [1](#names-and-prior-art) |
| D30 | CPU limit checked from the host's `cpu.stat` | [4.7](#47-limits-and-what-happens-over-the-memory-limit) |
| D31 | Smallest sandbox 192 MiB | [4.7](#47-limits-and-what-happens-over-the-memory-limit) |
| D32 | Over-limit programs usually end the sandbox; both outcomes reported | [4.7](#47-limits-and-what-happens-over-the-memory-limit) |
| D33 | The agent, not the wrapper, recognises an OOM-killed sentry | [4.7](#47-limits-and-what-happens-over-the-memory-limit) |
| D34 | One exit event only when the exit was seen; otherwise a gRPC status | [4.5](#45-exec-paths-and-the-stream-contract) |
| D35 | The local failure-domain test uses the spread policy | [8.7](#87-the-local-failure-domain-test) |
| D36 | Image key, publish after fsync and a test restore, numpy reseeded per worker | [6.1](#61-the-checkpoint-image) |
| D37 | The Exec proxy ends on `LOST` in scheduler state; keepalive at 10 s | [8.2](#82-execs-in-flight) |
