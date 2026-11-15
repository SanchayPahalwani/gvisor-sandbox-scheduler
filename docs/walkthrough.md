# Walkthrough: one request through the code

This follows one command,

```sh
bin/gssctl run -c 'print(1)'
```

from the client to a gVisor sandbox and back, in the order the code runs. There are 14 stops, about two minutes of reading each, then a shorter second pass: what happens when the node dies in the middle of a session. Links go to the file and line; function names are the ones in the code. The reasons behind each choice are in [design.md](design.md), cited here by call number (D1 to D37).

The setting is the local cluster that `make demo` starts: one scheduler on `127.0.0.1:7000` and four node agents, `a1` and `a2` in group `a`, `b1` and `b2` in group `b`, each declared with 4000m CPU, 2048 MiB and up to 16 sandboxes, each keeping two warm sandboxes. Nothing else is running.

The path, in one line: `gssctl` → scheduler (`gateway` → `sched` → `cluster`) → node agent (`Manager`) → `runsc` → `pyfork` → zygote → worker, and the output back the same way.

## 1. The client: `gssctl run`

[cmd/gssctl/run.go](../cmd/gssctl/run.go#L93), `(*cli).run`.

`run` parses two groups of flags. The spec flags ([flags.go, `specFlags.spec`](../cmd/gssctl/flags.go#L79)) build a `SandboxSpec`: `--cpu 500m`, `--mem 256Mi`, no `--allow`, no group, `--mode auto` and a TTL of 10 minutes, all defaults here. The command flags ([`cmdFlags.request`](../cmd/gssctl/flags.go#L192)) build an `ExecRequest`; `-c` puts the code in the `python_code` field ([line 225](../cmd/gssctl/flags.go#L225)), with the default 30 s timeout. `-- ARGV...` would fill `argv` instead and run a new process.

[`connect`](../cmd/gssctl/main.go#L255) dials the scheduler over plaintext gRPC (D7) with the `otelgrpc` client handler ([`dial`](../cmd/gssctl/main.go#L160)). `gssctl` starts no span of its own, so with telemetry on each of the three calls below is the root of its own trace: one `gssctl run` makes three traces.

Then three calls, in order: [`createSandbox`](../cmd/gssctl/run.go#L73), [`execStream`](../cmd/gssctl/run.go#L22) and [`destroySandbox`](../cmd/gssctl/run.go#L84). Two details are worth seeing now:

- The create runs on `context.WithoutCancel(ctx)` ([line 118](../cmd/gssctl/run.go#L118)). A Ctrl-C during the create does not abandon it, because a sandbox created after the client gave up would live until its TTL. For the same reason the destroy runs whatever happened before it, unless `--keep` was given ([line 134](../cmd/gssctl/run.go#L134)).
- `sent` is taken just before the create ([line 115](../cmd/gssctl/run.go#L115)); the final line on stderr reports `total` as the time from then until the exit event arrived.

## 2. The scheduler receives the create: `gateway.CreateSandbox`

[internal/gateway/gateway.go](../internal/gateway/gateway.go#L196), `(*Server).CreateSandbox`.

The scheduler is one process with three packages (D6): `gateway` talks, `sched` decides, `cluster` remembers. `CreateSandbox` opens the span `gateway.create`, clones the spec, and runs [`ApplyDefaults`](../api/v1/defaults.go#L27) and [`ValidateSpec`](../api/v1/defaults.go#L48): CPU at least 100m, memory at least 192 MiB (D31), TTL at most 60 min, egress entries of the form `host:port`, `ip:port` or `cidr:port`.

[`claimNewID`](../internal/gateway/gateway.go#L169) makes an ID of the form `sb-` plus 12 hex characters and holds it in `s.busy`, the set of IDs being placed, so a create and a reschedule can never place the same sandbox at once. [`state.Add`](../internal/cluster/state.go#L278) records the sandbox as `PENDING` at incarnation 0, with `expires_at` set from the TTL.

Then [`placeAndStart`](../internal/gateway/gateway.go#L228), the loop the rest of the create runs in. Each pass places, reserves and starts; a failure excludes the node and goes round again, up to three failed node creates. The rescheduler uses the same function with `fresh` set to false, which is why a create and a reschedule behave the same (step 4 of the second pass).

## 3. Choosing a node: `sched.Place`

[`(*Server).place`](../internal/gateway/gateway.go#L360) takes a snapshot of the cluster with [`State.View`](../internal/cluster/state.go#L243) (one `sched.NodeInfo` per node, sorted by ID) and builds a [`sched.Request`](../internal/sched/types.go#L26). `WantsWarm` is true here: no egress, and start mode `AUTO`. Then it calls [`Policy.Place`](../internal/sched/policy.go#L33), under the span `sched.place`.

`Place` is pure: no I/O, no clock, no goroutines. It scans the nodes from an offset (which only moves when sampling is on, D22) and, for each node:

1. Runs the [filters](../internal/sched/filters.go) in order: `Excluded`, `NodeReady`, `NodeGroup`, `Capacity`. The first one that says no is counted against the node, and the scan moves on. `Capacity` compares the request with what is free, `cap - alloc`, rather than adding, so a huge request cannot overflow.
2. Runs the [scorers](../internal/sched/scorers.go) and sums weight times score: `BinPack` (weight 1, the mean of CPU and memory utilisation after placing the request, times 100), `WarmPool` (1, 100 if the node has a warm sandbox), `GroupAffinity` (0.5, 0 here because there is no affinity key) and `NodeHealth` (1, 100 times the node's health). The weights are [`DefaultWeights`](../internal/sched/policy.go#L163) (D21).
3. Inserts the node into a top-three list ([`insert`](../internal/sched/policy.go#L119)); ties go to the lower node ID ([`better`](../internal/sched/policy.go#L145)).

On this idle cluster every node scores the same: `BinPack` 12.5 (500m of 4000m and 256 of 2048 MiB), `WarmPool` 100, `NodeHealth` 100, total 212.5. The tie goes to `a1`. The `Decision` carries the node ID, the number of nodes considered and feasible, and the top three with their per-plugin scores; [`placement`](../internal/gateway/gateway.go#L409) stores that on the sandbox, which is what `gssctl explain` prints, and `place` puts the same figures on the span.

If no node passed, `Place` would return a `NoFitError` that counts rejections per filter, and the create would end with `ResourceExhausted`.

## 4. Holding the room: `cluster.Reserve`

[internal/cluster/state.go](../internal/cluster/state.go#L309), `(*State).Reserve`, called at [gateway.go line 257](../internal/gateway/gateway.go#L257).

The snapshot from stop 3 is already stale: another create may have taken the room on `a1` in the meantime. `Reserve` checks again under the state's one mutex: the sandbox is still `PENDING` (or `LOST`), its TTL has not run out, the node is ready and has room. Then it charges the request to the node (`allocate`), moves the sandbox to `STARTING`, sets its node and group, bumps the incarnation to 1, stores the placement record, and returns the incarnation and the TTL that is left.

This is the scheduler's version of kube-scheduler's Reserve step: room is taken before the node is called, so concurrent requests do not all pile onto the node that looked best in the same snapshot. If `Reserve` fails, `placeAndStart` excludes the node and places again; that does not count as a failed attempt.

## 5. To the node: `NodeService.CreateSandbox` and `Manager.Create`

[`(*Server).start`](../internal/gateway/gateway.go#L419) copies the spec with the TTL replaced by what is left, and calls the node through [`grpcNodes.Create`](../internal/gateway/nodes.go#L85): one cached plaintext connection per node address, keepalive at 10 s (the floor grpc-go allows, D37), and a 60 s deadline for the call.

On the node, [`server.CreateSandbox`](../internal/nodeagent/server.go#L56) hands the request to [`(*Manager).Create`](../internal/nodeagent/manager.go#L211) and logs one line. `Create` opens `node.create`, validates the sandbox ID, applies the defaults and validates the spec again (a node does not trust its caller). Sandbox IDs are letters, digits and `-` only, because `runsc` matches container IDs by prefix. On the cold and restore paths this sandbox's `runsc` ID would be `sb-..._1_`, from [`runscID`](../internal/nodeagent/sandbox.go#L43); a warm claim keeps the pool sandbox's own `warm_<8 hex>_` ID. That is why sandbox IDs may not contain `_` (and `warm` is reserved): no sandbox's `runsc` ID can then be a prefix of a pool ID, or the other way round.

Then [`chooseMode`](../internal/nodeagent/manager.go#L309), the whole start-path policy in 30 lines. For `AUTO`: warm if the pool has a ready sandbox and there is no egress; else restore if the node has a checkpoint image; else cold. A forced mode the node cannot serve returns `FailedPrecondition` instead of measuring a different path. Node `a1` has two warm sandboxes, so the mode is warm, and the span gets `start.mode=warm`.

## 6. The warm claim

[`(*Manager).claim`](../internal/nodeagent/pool.go#L295) calls [`reserve`](../internal/nodeagent/manager.go#L391) with mode warm. `reserve` first admits the request against the node's own declared capacity ([`admitLocked`](../internal/nodeagent/manager.go#L466)), a second check after the scheduler's, then [`claimLocked`](../internal/nodeagent/pool.go#L243):

- takes the oldest pool sandbox;
- checks with signal 0 that its sandbox process is still alive (a dead one is dropped in the background and the next is tried);
- gives it the requested sandbox ID, incarnation, spec and expiry (TTL plus 10 s, the node's own backstop); it keeps its `runsc` ID `warm_<8 hex>_`, its bundle, its cgroup and its pid;
- puts it in the table as `starting`, so an Exec cannot reach it yet.

Back in `claim`, [`setClaimLimits`](../internal/nodeagent/pool.go#L322) writes `cpu.max` and `memory.max` in the sandbox's cgroup to the request (span `pool.claim`). Then the entry is `ready`. There is no `runsc` call on this path at all (D16). The deferred `kickRefill` wakes the refill loop, which starts a replacement now, before this request is even answered.

If the pool had emptied since `chooseMode`, the claim would report a miss, and `Create` would fall through to the restore path (stop 7), or to the cold path on a node without an image, because the mode was `AUTO`.

## 7. Where the pool sandbox came from: the restore path and `bundle.Write`

The sandbox just claimed was made earlier by the refill loop, [`fillOne`](../internal/nodeagent/pool.go#L127), on the restore path. A request that finds no warm sandbox takes the same path from `Create`, so this is the code either way.

[`startSandbox`](../internal/nodeagent/sandbox.go#L119) does, in order:

1. Network setup, only for egress (not here): a namespace, a veth pair and an nftables chain from [`netpolicy.Setup`](../internal/netpolicy/netpolicy.go#L344) (D12).
2. [`bundle.Write`](../internal/bundle/bundle.go#L143): a directory under `<state-dir>/bundles/<runsc-id>/` with a `hosts` file and a `config.json` built by [`bundle.Spec`](../internal/bundle/bundle.go#L49). Read that function once: the read-only root (D10), tmpfs at `/work` and `/tmp`, uid 1000 with no capabilities, `RLIMIT_NOFILE` 1024, the cgroup path `/gss/<node>/<runsc-id>`, the CPU quota of `cpu_millis * 100` per 100000, and the memory limit. The process section is the same for every sandbox, because `runsc restore` refuses one whose arguments differ from the checkpoint's.
3. `runsc create`, then `runsc start` (cold) or `runsc restore` (restore), stop 8.
4. The readiness probe, stop 8.

Which image to restore from comes from [`restoreImage`](../internal/nodeagent/image.go#L199). The image was built when the node first started, by [`EnsureImage`](../internal/nodeagent/image.go#L97): under a file lock shared by every agent in the VM, cold-start one sandbox, checkpoint it, restore one sandbox from the result to check it, write a `.complete` marker, fsync, rename. Its directory name is [`ImageKey`](../internal/nodeagent/image.go#L56), a hash of the `runsc` version, the rootfs build ID, the primed set, the bundle's process section and mounts, and the global `runsc` flags (D36).

## 8. `runsc` and readiness

The manager calls `runsc` through the `Runtime` interface, wrapped so that each call is a span named `runsc.<method>` ([trace.go](../internal/nodeagent/trace.go)). The real implementation is [internal/runsc/cli.go](../internal/runsc/cli.go) (D9). Every command for a sandbox starts with the same flags, built by [`args`](../internal/runsc/cli.go#L74): `--root /run/gss/<node>`, `--network=none` (or `sandbox` for egress) and `--overlay2=none`. [`Create`](../internal/runsc/cli.go#L130) runs `runsc create --bundle <dir> <id>` with the container's own output going to `container.log` in the bundle; [`Restore`](../internal/runsc/cli.go#L160) runs `runsc restore --detach --image-path <image> --bundle <dir> <id>`; [`Start`](../internal/runsc/cli.go#L153) runs `runsc start <id>` on the cold path.

On the cold path PID 1 is [`zygote.py`'s `main`](../images/python/zygote.py#L345): it imports every module in `GSS_PRIME` (numpy and pandas included, D15), runs `gc.collect()` and `gc.freeze()`, binds `/tmp/gss-zygote.sock` and starts accepting. On the restore path all of that is already in the image: the restored zygote is listening the moment the sandbox runs.

Readiness is the same on every path ([`waitReady`](../internal/nodeagent/sandbox.go#L165), span `sandbox.ready`): a `runsc exec <id> /gss/pyfork` with [`readyProbe`](../internal/nodeagent/sandbox.go#L107) on stdin, a header and the four bytes `pass`. `pyfork` retries its connect for up to 30 s, so on a cold start this one call returns exactly when the zygote starts listening. The bound is 30 s, or 5 s after a restore. Then [`recordPID`](../internal/nodeagent/sandbox.go#L186) asks `runsc state` for the sandbox process's pid, which the liveness loop and the claim check later signal.

## 9. Commit: `READY`

Back in [`Manager.Create`](../internal/nodeagent/manager.go#L244), the warm branch records `gss_start_duration_seconds{mode="warm"}` and answers `started_as: WARM` with `ready_latency`, the time since the node received the call.

In the scheduler, [`placeAndStart`](../internal/gateway/gateway.go#L292) raises the node's health ([`RecordResult`](../internal/cluster/state.go#L225)) and calls [`State.Commit`](../internal/cluster/state.go#L364). `Commit` moves the sandbox to `READY` only if it is still `STARTING` at incarnation 1. If a destroy, the TTL or a node death had got there first, `Commit` would return false, and the gateway would destroy this copy on the node ([`destroyOnNode`](../internal/gateway/gateway.go#L433)), so nothing is left running that the scheduler does not know about. A warm commit also lowers the node's cached warm count, so the next placement does not count on the same warm sandbox before `a1`'s next heartbeat.

`Commit` closes the state's `changed` channel, which wakes anything waiting in `WaitReady`. The gateway logs `sandbox placed`, and `CreateSandbox` returns the `Sandbox` to `gssctl`: ID, node `a1`, group `a`, state `READY`, `started_as`, incarnation 1 and the node's `start_latency`.

## 10. The Exec proxy: `gateway.Exec`

[internal/gateway/gateway.go](../internal/gateway/gateway.go#L457), `(*Server).Exec`.

`gssctl`'s [`execStream`](../cmd/gssctl/run.go#L22) sends the `ExecRequest` with the sandbox ID and a deadline of the timeout plus 30 s. The scheduler proxies it (D8): clients talk to one address, and nodes need not be reachable from clients.

[`waitReady`](../internal/gateway/gateway.go#L535) returns at once here; for a sandbox that is `STARTING` or `LOST` it would wait in [`State.WaitReady`](../internal/cluster/state.go#L464) for up to 15 s. Then the gateway starts [`watch`](../internal/gateway/gateway.go#L560) in a goroutine and opens the node's Exec stream on a context the watcher can cancel. Every 250 ms the watcher compares the state's copy of the sandbox with the one this Exec started on ([`watchEnd`](../internal/gateway/gateway.go#L581)), and if it is no longer `READY` on `a1` at incarnation 1, cancels the stream with `Unavailable` and the state's reason: lost, moved, destroyed or expired (D34, D37). The second pass shows why.

The relay loop forwards whatever the node sends, as it arrives, and remembers whether an exit event went by. Nothing follows an exit because the node sends nothing after its exit; the gateway inherits that rule rather than enforcing it. Once an exit has gone by, how the node's stream ends does not matter: the result is in (D34). Without an exit event, the gateway maps the node's status: `Unavailable` keeps the node's message after `sandbox <id> lost on node <node>:`, and `NotFound`, which means the node never ran the Exec, is explained from the state ([`notOnNode`](../internal/gateway/gateway.go#L513)): it stays `NotFound` when the state has the sandbox destroyed or no longer knows it (the destroy started before the Exec reached the node), and becomes `Unavailable` when the sandbox was lost, moved, or is on a node agent that restarted.

## 11. On the node: `Manager.Exec`

[internal/nodeagent/exec.go](../internal/nodeagent/exec.go#L83), `(*Manager).Exec`, reached through [`server.Exec`](../internal/nodeagent/server.go#L73).

[`planExec`](../internal/nodeagent/exec.go#L154) turns the request into what `runsc exec` takes. For `python_code`: the arguments are `/gss/pyfork`, and stdin is one JSON header line, `{"code_len":8,"timeout_s":30,"env":{}}`, then the eight bytes `print(1)`, then the request's stdin (empty). The agent's own kill timer is set to the timeout plus 2 s: the zygote enforces the timeout inside the sandbox, and this timer is the backstop.

[`registerExec`](../internal/nodeagent/exec.go#L245) finds the entry and adds this Exec to its set, so that a destroy can end it without waiting for it; an entry still `starting` would give `Unavailable`, an unknown one `NotFound`.

[`execIn`](../internal/nodeagent/exec.go#L292) is the core:

- It creates a pid file for `--internal-pid-file` and reads the cgroup's `oom_kill` counter.
- It runs `Runtime.Exec` in a goroutine, with stdout and stderr as two [`streamWriter`](../internal/nodeagent/exec.go#L568)s. Each `Write` cuts its bytes into events of at most 32 KiB and hands them over an unbuffered channel, so output is held one chunk at a time and each stream keeps its order.
- One loop is the only caller of `send`. It forwards events, notices a cancelled context, and ends when the runtime call returns.
- The kill timer is a `time.AfterFunc`, outside the loop, so a client that stops reading cannot hold off a timeout. A stop kills the process inside the sandbox by its in-sandbox pid ([`killExec`](../internal/nodeagent/exec.go#L490)) and then ends the `runsc exec` client.

When the runtime call returns, [`exitStatus`](../internal/nodeagent/exec.go#L398) turns the outcome into an `ExitStatus` or an error: a timeout is exit 137 with `timed_out`; a 137 with a raised `oom_kill` is `oom_killed`; a runtime error with the sandbox gone removes the sandbox and returns `Unavailable` (D32, D33).

## 12. Inside the sandbox: `pyfork` and the zygote

`runsc exec` starts [`/gss/pyfork`](../cmd/pyfork/main.go#L37) inside the sandbox.

[`forward`](../cmd/pyfork/main.go#L88) reads the request with [`readRequest`](../cmd/pyfork/main.go#L128): the header line one byte at a time, then exactly `code_len` bytes, so anything after the code stays on fd 0 for the program. [`dial`](../cmd/pyfork/main.go#L185) connects to the zygote's socket, retrying every 5 ms. Then the step that makes the design work ([line 105](../cmd/pyfork/main.go#L105)): one `sendmsg` with a 4-byte length and `SCM_RIGHTS` carrying `pyfork`'s own descriptors 0, 1 and 2, then the JSON request. `pyfork` then blocks on a 4-byte exit code.

In the zygote, the accept loop forks a supervisor per connection. [`supervise`](../images/python/zygote.py#L240) receives the three descriptors and the request, forks the worker, closes its copies of the descriptors, and waits: for the worker to exit, for the deadline (30 s here), or for the connection to close, which means the Exec was cancelled. A deadline or a closed connection makes it kill the worker's process group, and the worker itself in case it left the group ([`kill_worker`](../images/python/zygote.py#L230)).

The worker calls [`reseed_numpy`](../images/python/zygote.py#L325) (restored sandboxes share interpreter memory, D36), then [`run_worker`](../images/python/zygote.py#L121): `dup2` of the passed descriptors onto 0, 1 and 2, a fresh `__main__` module, `sys.argv = ["-c"]`, the working directory on `sys.path`. [`run_code`](../images/python/zygote.py#L163) compiles and runs `print(1)` in a fork of an interpreter that has already imported pandas, and [`shut_down`](../images/python/zygote.py#L197) does what interpreter exit would do: threads, `atexit`, a collection pass, flushed streams. Then `os._exit(0)`.

## 13. The way back: output and the exit code

The bytes `1\n` never pass through the zygote or `pyfork`. The worker writes them to fd 1, which is `pyfork`'s stdout, which is the stdout pipe of the `runsc exec` process on the host. There the runsc wrapper copies them into the stdout `streamWriter`, which hands one `ExecEvent{stdout: "1\n"}` to the loop in `execIn`, which sends it on the node's gRPC stream. The gateway's relay loop receives it and sends it on the client's stream, and `execStream` writes it to `gssctl`'s stdout. Inside the sandbox nothing relays or frames the output (D11); outside it, the bytes are copied into one gRPC message per hop.

The exit code goes the long way round: the worker exits 0; the supervisor reaps it and writes 0 on the socket ([`send_code`](../images/python/zygote.py#L338)); `pyfork` reads it and exits 0; `runsc exec` exits 0; [`CLI.Exec`](../internal/runsc/cli.go#L207) returns `(0, nil)` (it would check `runsc state` first on an exit 128, which is also `runsc`'s own failure code). `exitStatus` makes `ExitStatus{code: 0}`, the span gets the exit code, `gss_exec_duration_seconds{kind="python"}` is recorded, and the exit event is sent. The stream then ends with OK, and the gateway, having seen the exit, ends its own.

`gssctl` keeps the first exit event and stops reading at the end of the stream. If the stream had ended without one, it would have printed the status and exited 1, never a made-up exit code (D34).

## 14. Destroy, and the pool refills

[`destroySandbox`](../cmd/gssctl/run.go#L84) calls [`gateway.DestroySandbox`](../internal/gateway/gateway.go#L614). The scheduler marks the sandbox `DESTROYED` first ([`State.Destroy`](../internal/cluster/state.go#L421)), which releases its allocation on `a1` and keeps its ID as a tombstone for 10 minutes, so a report of it within those 10 minutes is answered with a destroy instead of being adopted. A report later than that would be adopted (a known limit, [design.md section 13](design.md#scheduler)). Then it asks the node, bounded by 5 s. If that call fails, nothing is lost: the node's next heartbeat lists the ID, the tombstone matches, and the answer tells the node to destroy it.

On the node, [`Manager.Destroy`](../internal/nodeagent/manager.go#L514) with incarnation 0 (whatever is live) goes to [`destroyEntry`](../internal/nodeagent/sandbox.go#L205): take the entry's lifecycle lock (which would wait for a create of the same sandbox still in flight), mark it destroying (new Execs now get `NotFound`, running ones are ended with `Unavailable`), and run [`teardown`](../internal/nodeagent/sandbox.go#L258): `runsc delete --force`, kill anything left in the cgroup, network teardown if there was one, remove the bundle and the pid files, remove the cgroup. Each step is forgotten once it succeeded; if one fails the entry stays as broken, still counted against the node, and the liveness loop tries again.

Meanwhile the refill loop ([`refillLoop`](../internal/nodeagent/pool.go#L81)), woken by the claim in stop 6, has been restoring a replacement pool sandbox. Once it is ready, the next heartbeat reports two warm sandboxes again.

`gssctl` prints its summary line on stderr and exits with the program's code, 0:

```
sandbox sb-<id> node=a1 group=a start=warm ready=<ms> total=<ms>
```

## Second pass: the node dies mid-session

Now the same sandbox, created with `gssctl create` and used by repeated `gssctl exec` calls, so it outlives one command. It is on `a1`, at incarnation 1, and an Exec is running in it when `a1` dies: `scripts/cluster.sh kill-node a1` sends SIGKILL to the agent and writes `1` to `cgroup.kill` of `/gss/a1`, which kills every sandbox of the node with it ([`kill_nodes`](../scripts/cluster.sh#L465), D3).

**1. Nothing is said.** The agent is gone, so it sends no more heartbeats. What happens to the running Exec depends on the connection. Here the kernel closes the dead agent's sockets, so the node stream fails at once, and the gateway ends the Exec with `Unavailable: sandbox <id> lost on node a1: <transport error>`. On a real network a dead or cut-off machine often closes nothing, and the stream would hang; keepalive alone would take 10 s or more to notice. Step 3 covers that case.

**2. Detection.** [`Server.Run`](../internal/gateway/reschedule.go#L19) calls [`tick`](../internal/gateway/reschedule.go#L70) every 250 ms, which calls [`State.Tick`](../internal/cluster/state.go#L493). Once `a1`'s last heartbeat is more than 3 s old, `Tick` marks it `DEAD` and moves every `STARTING` or `READY` sandbox on it to `LOST`, releasing their allocations. Detection takes at most the timeout plus one tick (D19). `gss_node_deaths_total` goes up.

**3. A hanging Exec ends.** If the node stream is still open, the Exec's watcher ([`watch`](../internal/gateway/gateway.go#L560)) sees the sandbox `LOST` within the next 250 ms and cancels it with `Unavailable: sandbox <id> lost on node a1: marked LOST by the scheduler` (D37). Either way the client gets a status and no exit event (D34). Output streamed before the death is still valid; the exit is simply unknown.

**4. Rescheduling.** `tick` hands the lost IDs to [`enqueue`](../internal/gateway/reschedule.go#L109), which skips IDs already being placed and starts at most 8 reschedules at once ([`dispatchLocked`](../internal/gateway/reschedule.go#L125)). [`reschedule`](../internal/gateway/reschedule.go#L139) opens `gateway.reschedule` and calls the same `placeAndStart` as a create, with `fresh` false. `a1` now fails `NodeReady`; `Reserve` accepts a `LOST` sandbox, bumps it to incarnation 2 on, say, `a2`, and passes the TTL that is left (the TTL counts from the original create and is not reset). The node creates a fresh sandbox on whichever start path it has: under the `runsc` ID `sb-..._2_` on the cold or restore path, or under a pool sandbox's `warm_<8 hex>_` ID if it claims one. If no node could take it, it would stay `LOST` and [`NeedsPlacement`](../internal/cluster/state.go#L544) would bring it back every second until its TTL ran out.

**5. The next Exec.** A `gssctl exec` sent while the sandbox is `LOST` or `STARTING` waits in the gateway's `waitReady` for up to 15 s, then runs on incarnation 2. It runs, but in a fresh sandbox: files written to `/work` and any interpreter state are gone (D23). `gssctl explain <id>` shows the incarnation; a client that cares compares it with the one it saw before.

**6. If `a1` comes back.** A restarted agent deletes everything under its runsc root and starts as an empty node (D13). It registers again, and its empty heartbeats change nothing. If instead `a1` had only been cut off (a partition, not a crash), its next heartbeat would make it ready again with its allocations reset, and would still list the sandbox at incarnation 1. [`State.Heartbeat`](../internal/cluster/heartbeat.go#L20) sees that the scheduler has the sandbox on `a2` at incarnation 2 ([`reportedHere`](../internal/cluster/heartbeat.go#L129) is false) and puts the ID in the answer's destroy list. The node's [heartbeat loop](../internal/nodeagent/heartbeat.go#L141) destroys only the incarnation it reported, 1, so the instruction cannot touch a newer copy if one ever landed there.

That is the whole failure story: one threshold, a fresh sandbox under the same ID, and reconciliation on every heartbeat. [design.md section 8](design.md#8-failure-handling) gives the reasons and the alternatives, and the local failure-domain test (`make bench-grouploss`) measures it for a whole group of nodes.
