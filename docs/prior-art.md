# Prior art

This file records the existing work that `gvisor-sandbox-scheduler` overlaps with: projects that use its original working name, sandboxes for agent-generated code, and the building blocks it is made from. The research was done on 2026-11-12 (GitHub data was read on 2026-11-11 at 19:30 UTC), and each project is described only by what its own repository or documentation pages said on that date. Star counts, versions and licenses are as of that date.

## The name

The working folder for this code is still called `agent-sandbox`. The repository has a different name because `agent-sandbox` is already taken in the same problem space:

- `kubernetes-sigs/agent-sandbox` is a Kubernetes SIG Apps project written in Go (Apache-2.0, 4,576 stars, Go module `sigs.k8s.io/agent-sandbox`). It covers warm pools and gVisor sandboxes for agent code.
- Google's GKE documentation uses "Agent Sandbox" as the name for running that project on GKE.
- The PyPI package `agent-sandbox` is the Python SDK of `agent-infra/sandbox`. The npm package `agent-sandbox` belongs to another project (version 1.1.2; the repository link in its metadata returned 404 when checked). `@agent-sandbox/cli` on npm is the `abx` CLI from the `Alibaba-Agent-Sandbox` GitHub organization, and there is a GitHub organization named `agent-sandbox`.
- A GitHub repository search for `agent-sandbox` in the name returned 2,266 repositories.

Candidates were checked with the GitHub repository search API (by name, sorted by stars) and by exact package name on npm and PyPI.

| Candidate | What was found | Result |
|---|---|---|
| `sandboxd` | 164 GitHub matches, several in the same domain: `tastyeffectco/sandboxd` (988 stars), `inclusionAI/sandboxd`, `swualabs/sandboxd-o`. The npm name is taken. `kubernetes-sigs/agent-sandbox` calls its portable runtime daemon "sandboxd", and macOS ships a system daemon at `/usr/libexec/sandboxd`. | Rejected |
| `sandbox-fleet` | `AgentX-lab/SandboxFleet` (34 stars, Go) is in the same domain. `kubernetes-sigs/agent-sandbox` uses "fleet" for its multi-cluster layer (`fleetctl`, `fleet-member`). | Rejected |
| `sandbox-scheduler` | `NolanFoster/sandbox-scheduler` (1 star, Go) does multi-cluster placement for `kubernetes-sigs/agent-sandbox`. The name is generic. | Rejected |
| `sandbox-orchestrator` | 46 matches, including `kuasar-sandbox/orchestrator` and `GALIAIS/CTF-Sandbox-Orchestrator` (226 stars). | Rejected, crowded |
| `runsc-fleet` | No matches. Free on npm and PyPI. | Usable, but does not say "scheduler" and shares "fleet" with the names above |
| `sandbox-placer` | No matches. Free on npm and PyPI. | Usable, but "placer" is an uncommon word and gVisor is not named |
| `boxsched` | 3 unrelated matches with 0 stars. Free on npm and PyPI. | Usable, but not descriptive |
| `runsc-scheduler` | No exact match. Free on npm and PyPI. `runsc` is less widely recognized than gVisor. | Fallback if a shorter name is wanted |
| `gvisor-sandbox-scheduler` | No matches, and none for `gvisor-scheduler` or `runsc-sandbox-scheduler` either. Free on npm and PyPI. | Picked |

`gvisor-sandbox-scheduler` was picked because it names the three things that describe the project (gVisor, sandboxes, a scheduler), had no existing use on GitHub, npm or PyPI, and cannot be mistaken for `kubernetes-sigs/agent-sandbox`. The project also covers warm pools, checkpoint/restore, heartbeats and failover; those are parts of the placement control loop, so "scheduler" still fits. The name is long, at 24 characters.

This project is independent of the gVisor project and is not affiliated with it; "gVisor" in the name only says which runtime the code drives.

## Projects that share the name

- `kubernetes-sigs/agent-sandbox` (Apache-2.0, 4,576 stars). A Kubernetes SIG Apps project that defines a `Sandbox` custom resource and controller for isolated, stateful, single-instance workloads, with AI agent runtimes named as a target use. Extensions add `SandboxTemplate`, `SandboxClaim` and `SandboxWarmPool`. Latest release v1.0.10 (2026-11-06). Its Python client is on PyPI as `k8s-agent-sandbox`.
- "Agent Sandbox" on GKE. The name Google's GKE documentation uses for running the project above on GKE with GKE Sandbox (gVisor). The how-to pages install the upstream controller from its GitHub release manifest. Launch stage: not confirmed.
- `agent-infra/sandbox`, "AIO Sandbox" (Apache-2.0, 6,327 stars). One Docker image that bundles a browser, shell, file API, VS Code Server, Jupyter and MCP servers behind one HTTP port. Its Python SDK holds the PyPI name `agent-sandbox`.
- `agent-sandbox/agent-sandbox` (Apache-2.0, Go, 232 stars). A self-hosted sandbox platform that puts a REST API and an MCP server in front of a Kubernetes cluster, installs no CRDs, and claims E2B protocol and SDK compatibility.
- `Alibaba-Agent-Sandbox/agent-sandbox` (2 stars) and <https://www.agent-sandbox.cn/en/>. An Apache-2.0 sandbox runtime and `abx` CLI for coding agents with several backends, including OpenSandbox, CubeSandbox and E2B. The README says it is in preview.
- Novita and PPIO. Novita's docs name its product "Novita Agent Runtime" and refer to the "Novita Agent Sandbox ecosystem". The npm package `ppio-sandbox-cli` is described as a CLI for PPIO agent sandbox templates.
- Smaller GitHub repositories: `disler/agent-sandbox-skill` (393 stars, an agent skill for managing isolated execution environments), `mattolson/agent-sandbox` (214, a local development environment for coding agents), `archie-judd/agent-sandbox.nix` (209, Nix-based), `inoio/agents-sandbox` (85, a microVM per project), `Parassharmaa/agent-sandbox` (35, WASM), `vivek1504/agent-sandbox` (35, Firecracker), `thomaspeklak/agent-sandbox` (26, Podman), `1996fanrui/agents-sandbox` (24, a local sandbox for coding agents) and `scitix/Agent-Sandbox` (12).

## Agent code sandboxes

One row per project. The last column asks whether the project documents how it chooses a machine for each sandbox. "Not confirmed" means the pages that were opened did not state the point.

| Project | Isolation | Source available | Warm pool / snapshot | Placement across nodes published |
|---|---|---|---|---|
| `kubernetes-sigs/agent-sandbox` | None of its own; pods select gVisor or Kata through RuntimeClass | Yes, Apache-2.0 | Warm pool with claims; memory snapshots only through GKE Pod snapshots | No own scheduler; kube-scheduler places the pods |
| Agent Sandbox on GKE | GKE Sandbox (gVisor) | Controller yes; GKE Sandbox and Pod snapshots are GKE features | Warm pool as upstream; GKE Pod snapshots | No; standard GKE scheduling |
| `agent-infra/sandbox` (AIO Sandbox) | A single Docker container | Yes, Apache-2.0 | None described | None; one container image |
| `agent-sandbox/agent-sandbox` | Kubernetes pods; runtime not confirmed | Yes, Apache-2.0 | Pre-warmed pool, pause and resume, process snapshot | No; relies on Kubernetes |
| E2B (`e2b-dev/runtime`) | One Firecracker microVM per sandbox | Yes, Apache-2.0; the hosted cloud is commercial | No idle pool; each sandbox is a resumed snapshot; pause, resume, fork | Yes: best-of-K by load, node states |
| Modal Sandboxes | gVisor or a VM, selectable | Client SDK only | Filesystem snapshots; memory snapshots (alpha); no warm pool feature | No |
| Daytona | Linux namespaces per the docs; Docker runner in v0.190.0 | Archived; AGPL-3.0 up to v0.190.0 | Image templates called snapshots; warm pool service in v0.190.0 | In the v0.190.0 source: filter, sort by availability, random pick from the top 10 |
| Cloudflare Sandboxes | Firecracker microVM per container instance | SDK only | Filesystem snapshots, directory backups; the platform prepares capacity | No; done by the platform |
| Vercel Sandbox | Firecracker microVM | SDK and CLI only | Filesystem snapshots, fork from a snapshot; no warm pool feature | No; region level only |
| Fly.io Machines and Sprites | Firecracker microVMs | Not presented as open source | Machines: suspend and resume of whole VM state. Sprites: checkpoint and restore | No; you choose a region |
| microsandbox | MicroVMs; KVM on Linux | Yes, Apache-2.0 | Fork, full snapshot and restore of running state | None; single host |
| Arrakis | cloud-hypervisor microVMs on KVM | Yes, AGPL-3.0 | Snapshot and restore of a running sandbox; no warm pool | None; single host |
| OpenSandbox | runc (default), gVisor or Kata; Fast Sandbox uses Firecracker | Yes, Apache-2.0 | Pool of pre-warmed pods; Fast Sandbox checkpoints to an object store | Yes: predicates then scoring for pools; cache affinity then load in Fast Sandbox |
| Anthropic sandbox-runtime (`srt`) | OS primitives (Seatbelt, bubblewrap, Windows Filtering Platform); no VM | Yes, Apache-2.0 | None | None; wraps a local process |
| Docker Sandboxes (`sbx`) | MicroVM per sandbox (local) | Not confirmed | Not described | Local is single host; cloud not described |
| Agent Substrate | gVisor (`runsc`, default) or Kata on Cloud Hypervisor | Yes, Apache-2.0 | Warm worker pool; `runsc` checkpoint and restore; golden snapshots | Partly: own actor-to-worker scheduler; scoring rules not confirmed |
| Ray Sandboxes | gVisor (`runsc`, no Docker daemon) | Yes, part of Ray | None described | Placement by the Ray scheduler |
| Tencent Cloud CubeSandbox | KVM microVMs | Yes, Apache 2.0 | Resource pooling, snapshot cloning, auto pause and resume | Orchestrator described; algorithm not confirmed |
| `swualabs/sandboxd-o` | gVisor through the containerd `runsc` shim | Yes, MIT | Not mentioned in the README | Yes: scheduler loop, node scoring, labels, cordon |
| `AgentX-lab/SandboxFleet` | gVisor (default), runc or Kata | Public; no license file detected | Snapshot and fork of running sandboxes | Slot scheduler places sandboxes on slots in worker pods |
| `NolanFoster/sandbox-scheduler` | Placement layer for `kubernetes-sigs/agent-sandbox` | Yes, Apache-2.0 | Not confirmed | Filter, score, bind across clusters, per the repo description |
| `openkruise/agents` | On Kubernetes; runtime not confirmed | Yes, Apache-2.0 | Resource pooling, hibernation, checkpoint | Not confirmed |
| Azure Container Apps dynamic sessions | Hyper-V | No | Pools of prewarmed sessions | Not confirmed |
| Blaxel Sandboxes | Virtual machines; hypervisor not confirmed | Not confirmed | Standby snapshot of processes and in-memory filesystem | Not confirmed; region is user-selected |

Also seen, with fewer facts recorded: `inclusionAI/sandboxd` (a gRPC sandbox lifecycle service that runs sandboxes with gVisor, Kata Containers, Firecracker or runc), `kuasar-sandbox/orchestrator` (described as an E2B-compatible node service and multi-node control plane), `boxlite-ai/boxlite` (an embeddable microVM per box using KVM or Hypervisor.framework), `NVIDIA/OpenShell` (a policy-enforcing runtime for groups of autonomous agents; mechanisms not confirmed) and Amazon Bedrock AgentCore Code Interpreter (a containerized environment; isolation technology not confirmed). Not examined: Runloop, Northflank, CodeSandbox SDK, Deno Sandboxes, Together Code Interpreter and Google Vertex AI code execution.

No project was found that combines all of the following: `runsc` driven directly from OCI bundles, no Kubernetes, and a published filter and score scheduler with warm pools, checkpoint/restore, heartbeats and group failover. The closest published pieces are these.

Agent Substrate (Apache-2.0, Go, 6,450 stars, version 1.0.0 released 2026-11-05) runs on Kubernetes and maps a large set of stateful "actors" onto a smaller pool of ready "workers". Idle actors are snapshotted out of memory and restored when a request arrives; the README claims resume in under 500 ms. For gVisor, the worker process calls `runsc` to checkpoint and restore the sandboxed process tree, and root filesystem overlays and durable directories are saved as separate bundles. Snapshots go to object storage through a plugin, and the node keeps a local copy so that a resume on the same node avoids a download. kube-scheduler places the worker pods; Substrate's own control plane then assigns actors to workers, with a state store in PostgreSQL and a scheduler component that selects a ready worker for each resumption. The scoring rules of that scheduler are not confirmed.

Ray Sandboxes (`ray.experimental.sandbox`, alpha) runs model-generated code under `runsc` on Ray worker nodes with no Docker daemon. Images are pulled through the registry HTTP API, flattened and cached as EROFS images. Root filesystems are read-only by default with a copy-on-write overlay for writable sandboxes, CPU and memory limits come from cgroups, rootless mode is the default, and the default network mode is `none`. Each sandbox is a Ray actor, so the Ray scheduler places it using normal actor options, and a custom resource can pin sandboxes to nodes that have `runsc`. No checkpoint or warm pool is described.

`swualabs/sandboxd-o` (MIT, Go, 5 stars) is an orchestrator built as a replacement for a Kubernetes-based provisioner. It uses gVisor through the containerd `runsc` shim. `sbxlet` is the per-node agent. `sbxorch` runs a scheduler loop that assigns pending sandboxes to worker nodes, a reconcile loop, and a node resource sync loop whose data is used to score nodes. `sbxctl` has `cordon` and `uncordon`, and a sandbox can carry a node selector that matches node labels. Its README does not mention checkpoint/restore or warm pools.

E2B's backend (`e2b-dev/runtime`, Apache-2.0, Go, 1,835 stars) runs one Firecracker microVM per sandbox, each in its own cgroup and network namespace with a per-sandbox nftables egress firewall. It keeps no pool of idle VMs: templates are pre-booted VMs stored in object storage, and a sandbox is a resumed snapshot whose memory pages are loaded lazily with `userfaultfd`. Its architecture document describes placement. The API keeps a live map of orchestrator nodes, samples K ready nodes, scores each by the higher of CPU load and hugepage-pool load, picks the lowest, and retries with jittered backoff when a node is exhausted. Nodes in the `Draining`, `ShuttingDown` or `NotReady` states get no new sandboxes, and resume prefers the node that still has the sandbox cached.

OpenSandbox (Apache-2.0, 17,197 stars) has a server that runs on Docker locally or on Kubernetes, with runc, gVisor or Kata Containers as the runtime. Its Kubernetes operator keeps a `Pool` of pre-warmed pods. When a `BatchSandbox` names no pool, the controller can choose one with predicate plugins (capacity, image, resource, node selector) followed by a scoring plugin (least-allocated by default). Its Fast Sandbox backend runs Firecracker microVMs inside pre-warmed pods, takes the Kubernetes scheduler out of the create path, and ranks candidates by image-cache affinity, then normalized load, then a stable-hash tiebreak. Pause writes a checkpoint to an S3-style store, and resume can happen on a different host pod.

`kubernetes-sigs/agent-sandbox` is the closest in name and purpose but sits at a different layer. It is a controller that creates pods, leaves isolation to the RuntimeClass and leaves node placement to kube-scheduler. Its performance document reports about 300 claims per second per cluster when draining an already-warm pool, and about 70 to 85 sandboxes per second per pool when the pool must refill at the same time. A proposal (KEP-1434) describes multi-cluster placement policies as an example plus a Python library, not as part of the core controller.

## Building blocks

### gVisor and `runsc`

gVisor is an application kernel written in Go that runs in user space and implements Linux system calls for the sandboxed application; its docs describe it as a Cloud Native Computing Foundation sandbox project originally created by Google (Apache-2.0, 19,966 stars). `runsc` is its OCI runtime and can be used with Docker, with Kubernetes, or directly. This repository uses the direct path: the node agent prepares an OCI bundle (a `rootfs` directory and a `config.json`) and runs `runsc` on it, with no containerd or Kubernetes in between.

### gVisor checkpoint/restore

`runsc checkpoint --image-path=<path> <id>` saves a container's state to a directory, and `runsc create` followed by `runsc restore --image-path=<path> <id>` loads it into a new container. Background restore is on by default, so the application starts once kernel state is loaded while memory and file data load asynchronously, and a restore on another machine requires the CPU features that were present at checkpoint time. This repository's third start path is a restore from such an image, taken from a sandbox whose Python interpreter has already done its imports; all of it happens inside one VM, so restore on a different machine is not exercised.

### Firecracker

Firecracker is a virtual machine monitor that uses KVM to run microVMs with a minimal device model, and it supports full and diff snapshots (Apache-2.0, 37,971 stars). E2B, Vercel Sandbox, Fly Machines, Cloudflare Containers and OpenSandbox Fast Sandbox use it, according to their own docs. This repository does not use Firecracker; it is listed because several of the sandboxes above are built on it.

### Kata Containers

Kata Containers is an OCI and CRI compatible runtime that runs each pod in a lightweight VM, with Cloud Hypervisor, Firecracker, QEMU, Dragonball and StratoVirt listed as supported hypervisors (Apache-2.0, 9,120 stars). It is selected per pod through Kubernetes RuntimeClass, and it is the VM-based alternative to gVisor in `kubernetes-sigs/agent-sandbox`, Agent Substrate, OpenSandbox and GKE Sandbox. This repository does not use it.

### Kubernetes RuntimeClass

RuntimeClass is a cluster resource (`node.k8s.io/v1`, stable since Kubernetes v1.20) that maps a name to a CRI runtime handler; a pod selects it with `spec.runtimeClassName`, and an optional `scheduling` field keeps such pods on nodes that have the runtime. It is how `kubernetes-sigs/agent-sandbox`, GKE Sandbox and OpenSandbox select gVisor or Kata. This repository calls `runsc` directly, so it does not use RuntimeClass.

### kube-scheduler scheduling framework

The scheduling framework is the plugin architecture compiled into kube-scheduler, stable since Kubernetes v1.19. Filter plugins remove nodes that cannot run the pod, Score plugins rank the nodes that remain, and the scheduler combines the scores according to configured plugin weights. This repository's scheduler is modelled on those two stages, with three filters (node ready, node group constraint, capacity) and four weighted scores (bin-packing, warm-pool hit, group affinity, node health); it is a separate Go program with a gRPC interface, not a kube-scheduler plugin.

### GKE Sandbox and GKE Pod snapshots

GKE Sandbox is a GKE feature that isolates pods running untrusted code with gVisor or with microVMs (Kata Containers with Cloud Hypervisor), requested through a RuntimeClass. GKE Pod snapshots save the whole pod state, including process memory and file system changes, to Cloud Storage and restore new pods from it; they require GKE Sandbox with gVisor, and the target node must match on machine series, CPU vendor, CPU features and GKE Sandbox kernel version. This repository does a local version of the same thing with `runsc checkpoint` and `runsc restore`, and uses no GKE or other cloud service.

## What this repository does differently, or more simply

This is a small reference implementation with no users. The points below are differences in scope and design, not claims that it works better.

- **No Kubernetes and no containerd.** The node agent calls `runsc` itself with OCI bundles. `kubernetes-sigs/agent-sandbox`, Agent Substrate, the OpenSandbox operator, `agent-sandbox/agent-sandbox` and `openkruise/agents` run on Kubernetes. `swualabs/sandboxd-o` also avoids Kubernetes but goes through the containerd shim. Ray Sandboxes drives `runsc` directly too, inside a Ray cluster. The Kubernetes projects do more as a result: `kubernetes-sigs/agent-sandbox` gives each sandbox a stable hostname, optional persistent storage and an HTTP router.
- **One scheduler at one level.** The scheduler here places each sandbox on a node. In `kubernetes-sigs/agent-sandbox`, kube-scheduler places the pods. In Agent Substrate, kube-scheduler places worker pods and a second scheduler assigns actors to workers. In Ray Sandboxes, the Ray scheduler places each sandbox as an actor. kube-scheduler is a plugin framework with many more extension points than the three filters and four scores here.
- **A fixed filter-then-score policy.** The filters are node ready, node group constraint and capacity; the scores are bin-packing, warm-pool hit, group affinity and node health. E2B's runtime samples K ready nodes and picks the least loaded. Daytona's last public source filters runners, sorts them by an availability score and picks at random from the top 10. OpenSandbox uses predicate plugins and a scoring plugin to choose a pool, and Fast Sandbox ranks by image-cache affinity and then load. E2B's preference for the node that still has a sandbox cached, and Fast Sandbox's image-cache affinity, resemble the warm-pool score here.
- **Three start paths measured on the same machine.** Cold boot, claim from a per-node warm pool, and restore from a `runsc checkpoint` image are timed side by side. Other projects publish figures for their own mechanism: `kubernetes-sigs/agent-sandbox` reports warm pool claim rates, Agent Substrate claims resume in under 500 ms, E2B's docs give about 1 second to resume, and Fly gives a few hundred ms for Machine resume. The measurements here come from one laptop VM and are not comparable with any of those.
- **A checkpoint of a pre-imported Python interpreter.** The checkpoint image is taken from a sandbox whose Python interpreter has already done its imports, with a small fork server inside the sandbox. Agent Substrate's golden snapshots, the `kubernetes-sigs/agent-sandbox` example that restores every warm pool member from one golden GKE Pod snapshot, and microsandbox's "Warm Workers" how-to are similar. Agent Substrate goes further: it stores snapshots in object storage, saves root filesystem overlays separately, and also supports microVMs.
- **An egress allowlist enforced outside the sandbox.** A sandbox has no network by default. With an allowlist, a network namespace, a veth pair and nftables rules enforce it from outside; allowlisted hostnames are resolved once and pinned in `/etc/hosts`, and the sandbox gets no DNS. E2B also uses a network namespace and a per-sandbox nftables egress firewall. Anthropic `srt` and Docker Sandboxes send traffic through a filtering proxy, and Docker's proxy also injects credentials. Ray Sandboxes defaults to no network and offers three other modes. Pinning addresses is simpler than a proxy, and it does not follow a hostname whose address changes after the sandbox starts.
- **Failover restarts, it does not migrate.** A node that misses heartbeats is drained and its sandboxes are started again on other nodes as fresh sandboxes; in-sandbox state is not preserved. Daytona's last public source marks a runner unresponsive after 60 seconds without a health report. OpenSandbox Fast Sandbox has a per-sandbox failure policy and recovery timeout, and a paused sandbox can resume on a different host pod from a checkpoint in object storage, which keeps state that this repository loses.
- **Laptop scale.** Everything runs in one Lima VM on one laptop. The "nodes" are node-agent processes in that VM, two labelled node groups stand in for failure domains in a local failure test, and a simulation runs the policy against 1,000 synthetic nodes. `kubernetes-sigs/agent-sandbox` publishes per-cluster throughput and CubeSandbox supports multi-node clusters. Nothing here has been run on more than one machine.

## What the others do that this repository does not

- **Run on more than one machine.** Fly.io and Vercel place by region, Cloudflare selects the nearest location with a pre-fetched image, CubeSandbox supports multi-node clusters, `kubernetes-sigs/agent-sandbox` has a multi-cluster proposal (KEP-1434), and `NolanFoster/sandbox-scheduler` describes placement across multiple clusters and providers. Here the nodes are processes in one VM, and no cloud or multi-cloud support is built.
- **Hardware virtualization.** E2B, Vercel Sandbox, Fly.io, Cloudflare Containers and OpenSandbox Fast Sandbox use Firecracker. Arrakis uses cloud-hypervisor. Agent Substrate and GKE Sandbox offer Kata Containers on Cloud Hypervisor. Azure dynamic sessions use Hyper-V. This repository uses gVisor only.
- **Keep or move the state of a running sandbox.** E2B pauses to object storage (about 4 seconds per GiB of RAM) and resumes in about 1 second. OpenSandbox Fast Sandbox can resume on a different host pod. Agent Substrate stores snapshots in object storage. CubeSandbox lists cross-node pause and resume as a preview. GKE Pod snapshots restore whole pods on matching nodes. This repository does not move a running sandbox's state to another node.
- **Snapshot, fork or roll back a sandbox that is in use.** E2B, Vercel Sandbox (`Sandbox.fork`), microsandbox (`msb fork`), CubeSandbox and `AgentX-lab/SandboxFleet` can start new sandboxes from a live one or from its snapshot, and Arrakis snapshots a running sandbox so that an agent can backtrack. The sandbox lifecycle here is create, exec and destroy; the checkpoint image is prepared ahead of time from one template sandbox.
- **Persistent storage and long-lived sandboxes.** `kubernetes-sigs/agent-sandbox` gives a sandbox a stable hostname and optional persistent storage, with suspend and resume based on a persistent volume. Sprites have a persistent filesystem. Vercel's persistent sandboxes snapshot when a session stops and resume later. Modal, Cloudflare and Vercel keep filesystem snapshots.
- **Kubernetes integration.** Custom resources, controllers and RuntimeClass selection, as in `kubernetes-sigs/agent-sandbox`, Agent Substrate, OpenSandbox and `openkruise/agents`. Not built here.
- **Client SDKs and API compatibility.** E2B, Modal, Vercel, Cloudflare, Daytona and OpenSandbox ship SDKs. `agent-sandbox/agent-sandbox`, CubeSandbox and `openkruise/agents` state E2B compatibility. AIO Sandbox, Arrakis and `agent-sandbox/agent-sandbox` have MCP servers. The interface here is gRPC.
- **Browser and desktop environments.** AIO Sandbox bundles a browser, VS Code Server and Jupyter, and Arrakis sandboxes include a VNC server for computer use. Sandboxes here run commands and stream stdout and stderr.
- **Multi-tenant authentication, a highly available scheduler and production hardening.** None of these is built here. Several entries in the table are hosted services (Vercel Sandbox is generally available, for example); this repository is a reference implementation.

## Sources

All pages were opened on the research date. GitHub READMEs, docs files, star counts, licenses and push dates were read through the GitHub REST API; the URLs below are the corresponding repository pages.

**The name and projects that share it**

- `kubernetes-sigs/agent-sandbox`: <https://github.com/kubernetes-sigs/agent-sandbox>, <https://agent-sandbox.sigs.k8s.io/docs/sandbox/snapshots/>, <https://pypi.org/pypi/k8s-agent-sandbox/json>
- Agent Sandbox on GKE: <https://docs.cloud.google.com/kubernetes-engine/docs/how-to/agent-sandbox>, <https://docs.cloud.google.com/kubernetes-engine/docs/how-to/agent-sandbox-pod-snapshots>
- `agent-infra/sandbox`: <https://github.com/agent-infra/sandbox>, <https://pypi.org/pypi/agent-sandbox/json>
- `agent-sandbox/agent-sandbox`: <https://github.com/agent-sandbox/agent-sandbox>
- Alibaba Agent Sandbox: <https://github.com/Alibaba-Agent-Sandbox/agent-sandbox>, <https://www.agent-sandbox.cn/en/>
- Novita: <https://docs.novita.ai/guides/sandbox-agent-runtime-introduction>
- Name searches: the GitHub repository search API (`<name> in:name`), <https://registry.npmjs.org/agent-sandbox> and the npm search API, PyPI by exact package name
- Candidate list only: <https://github.com/arjan/awesome-agent-sandboxes>

**Agent code sandboxes**

- E2B: <https://github.com/e2b-dev/runtime>, <https://docs.e2b.dev/sandbox/persistence>, <https://github.com/e2b-dev/E2B> (metadata only)
- Modal Sandboxes: <https://modal.com/docs/guide/sandboxes>, <https://modal.com/docs/guide/sandbox-snapshots>
- Daytona: <https://github.com/daytonaio/daytona> (default branch README and the source at tag v0.190.0), <https://www.daytona.io/docs/architecture/>
- Cloudflare Sandboxes and Containers: <https://developers.cloudflare.com/sandbox/>, <https://developers.cloudflare.com/sandbox/concepts/architecture/>, <https://developers.cloudflare.com/sandbox/llms.txt>, <https://developers.cloudflare.com/containers/platform-details/architecture/>
- Vercel Sandbox: <https://vercel.com/docs/vercel-sandbox>, <https://vercel.com/docs/sandbox/concepts/snapshots>
- Fly.io Machines and Sprites: <https://docs.fly.io/machines/overview>, <https://docs.fly.io/reference/suspend-resume>, <https://docs.fly.io/reference/architecture>, <https://fly.io/sprites/checkpoint-restore/>
- microsandbox: <https://github.com/superradcompany/microsandbox>
- Arrakis: <https://github.com/abshkbh/arrakis>
- OpenSandbox: <https://github.com/opensandbox-group/OpenSandbox>, <https://github.com/opensandbox-group/fast-sandbox> (metadata only)
- Anthropic sandbox-runtime: <https://github.com/anthropics/sandbox-runtime>
- Docker Sandboxes: <https://docs.docker.com/ai/sandboxes/>, <https://docs.docker.com/ai/sandboxes/architecture/>, <https://github.com/docker/sbx-releases> (file listing only)
- Agent Substrate: <https://github.com/agent-substrate/substrate>, <https://opensource.googleblog.com/2026/11/sandboxing-untrusted-ai-agents-at-scale.html>, <https://docs.cloud.google.com/kubernetes-engine/ai-ml/about-agent-substrate>
- Ray Sandboxes: <https://docs.ray.io/en/latest/ray-core/sandboxes.html>
- Tencent Cloud CubeSandbox: <https://github.com/TencentCloud/CubeSandbox>
- Smaller projects: <https://github.com/swualabs/sandboxd-o>, <https://github.com/AgentX-lab/SandboxFleet>, <https://github.com/NolanFoster/sandbox-scheduler>, <https://github.com/inclusionAI/sandboxd>, <https://github.com/openkruise/agents>, <https://github.com/boxlite-ai/boxlite>
- Azure Container Apps dynamic sessions: <https://learn.microsoft.com/en-us/azure/container-apps/sessions>
- Amazon Bedrock AgentCore Code Interpreter: <https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/code-interpreter-tool.html>
- Blaxel Sandboxes: <https://docs.blaxel.ai/Sandboxes/Overview>

**Building blocks**

- gVisor and `runsc`: <https://gvisor.dev/docs/>, <https://gvisor.dev/docs/user_guide/quick_start/oci/>, <https://github.com/google/gvisor> (metadata only)
- gVisor checkpoint/restore: <https://gvisor.dev/docs/user_guide/checkpoint_restore/>
- Firecracker: <https://github.com/firecracker-microvm/firecracker>
- Kata Containers: <https://github.com/kata-containers/kata-containers>
- Kubernetes RuntimeClass: <https://kubernetes.io/docs/concepts/containers/runtime-class/>
- kube-scheduler scheduling framework: <https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/>
- GKE Sandbox and GKE Pod snapshots: <https://docs.cloud.google.com/kubernetes-engine/docs/concepts/sandbox-pods>, <https://docs.cloud.google.com/kubernetes-engine/docs/concepts/pod-snapshots>
