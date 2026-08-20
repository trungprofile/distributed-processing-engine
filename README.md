# distributed-processing-engine

Exactly-once distributed record processing in Go — a Redis Streams engine with lease-based recovery, and a Kubernetes operator that scales it on backlog and drains workers before it ever removes one.

[![Go](https://img.shields.io/badge/go-1.24%2B-00ADD8)](https://go.dev/dl/)
[![CI](https://github.com/trungprofile/distributed-processing-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/trungprofile/distributed-processing-engine/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

## What it does

Processes high-volume record streams across a cluster of machines, and guarantees every record is handled exactly once — even when a machine dies holding work in progress.

The engine gets this from three pieces: a Redis Streams consumer group that will always redeliver an unfinished record, an idempotency key that makes a redelivered record a no-op instead of a second effect, and a lease sweep that hands a dead node's work to a live one without any leader or membership protocol.

On top of that sits a Kubernetes operator. You declare a `ProcessingJob` — which stream, how many workers at most, how much GPU each one needs — and it runs the workers as a StatefulSet, grows the pool when the backlog grows, and shrinks it only after draining a worker's in-flight records first. Workers get stable names so a restarted pod rejoins as the same consumer and picks up its own unfinished work rather than waiting for someone else to reclaim it.

## Results

| Metric | Result | Source |
| --- | --- | --- |
| Throughput (1 node → 8 nodes) | 1.2K → 8.9K records/sec | Docker Compose, `make bench` |
| Scaling efficiency | 92% | Docker Compose, `make bench` |
| Data loss under node failure | 0 records | `make test` |
| Recovery time after node loss | < 1.8s | `make test` |
| GPU fragmentation, bin-pack vs spread (**simulated**) | 4-GPU block free vs 1; 6 of 12 nodes used vs 12 | [`bench/results/placement.json`](bench/results/placement.json) |

Engine setup: 256-byte records, 8 worker replicas at concurrency 64 (1 vCPU / 256 MB each) on an 8-vCPU / 16 GB Linux host, Redis 7.2 with `appendonly yes` / `appendfsync everysec` / `maxmemory-policy noeviction`, `min-idle` lease 5s.

Placement setup: `cmd/placementsim` over [`bench/fixtures/gpu-inference.yaml`](bench/fixtures/gpu-inference.yaml) — 12 nodes (8 × 4 GPUs, 4 CPU-only), 40 pods. Both strategies scheduled every pod at identical 75% GPU utilization; what differs is fragmentation. Reproduce with `make bench-placement`.

### Status

**Verified end to end:** the engine, unchanged and still covered by the kill-a-node zero-loss integration test that runs in CI. The placement simulation, which runs in pure Go with no cluster and whose committed output is what the table above quotes.

**Implemented and unit-tested, but not yet run against a live cluster:** the operator itself. The reconcile loop, the lag autoscaler and the drain-before-scale-in protocol are complete code with tests against a fake API server — no stubs, no `TODO` — but no reconcile has executed on a real cluster. The Helm chart passes `helm lint` and renders valid manifests across its value permutations; the example `ProcessingJob` manifests validate against the generated CRD schema; the e2e suite compiles under its build tag and is genuinely written. None of those three has been installed or executed. The `e2e` workflow is deliberately `workflow_dispatch`-only for the same reason.

**Not measured:** reclaim latency after pod eviction, scale-up latency, and throughput under the operator. Two separate attempts have now failed for the same reason — neither the environment this was built in nor the workstation it was later restored on had a container runtime, so no kind cluster could be created and `make kind-up`, `make e2e` and the operator benchmarks have still never executed. `bench/results/` therefore holds one file, `placement.json`, and the table above gains no Kubernetes rows. The harnesses are committed and ready to run; [`bench/README.md`](bench/README.md) records exactly what each one needs. No Kubernetes number was estimated or inferred from the Docker Compose figures.

**Re-verified on restore, Go toolchain only:** `go build ./...`, `go vet ./...` and `gofmt -l` are clean; `go vet -tags e2e ./test/e2e/...` compiles the e2e suite; `make unit` passes under `-race` across the controller, placement, stream and worker packages. `make test` was *not* re-run, because its Redis service needs Docker — the four engine figures in the table above are unchanged from the earlier Docker Compose runs and were not re-measured.

## How it works

```
                                  ┌──────────────────────────────────────┐
                                  │  ProcessingJob  (dpe.trungprofile.dev)│
                                  │  stream, concurrency, minIdle,        │
                                  │  autoscale{min,max,targetLagPerWorker}│
                                  │  resources{nvidia.com/gpu}, placement │
                                  └───────────────────┬──────────────────┘
                                                      │ watch
                                  ┌───────────────────▼──────────────────┐
                                  │            dpe-operator              │
                                  │  reconcile every 5s:                 │
                                  │   • own StatefulSet + headless Svc   │
                                  │   • desired = ceil(lag / target)     │
                                  │   • drain victim before scale-in     │
                                  │   • write status + conditions        │
                                  └────┬────────────────────────┬────────┘
                                       │ GetClusterStats (gRPC) │ scale / roll
                                       ▼                        │
  producer ──XADD──▶ Redis Stream  ┌─────────────┐              │
                     dpe:records   │ coordinator │◀─DrainNode───┘
                          │        └──────┬──────┘
                    XREADGROUP            │ PUBLISH dpe:control:drain
       (group: dpe-workers, one consumer per pod)
                          │               │
        ┌─────────────────┼───────────────┼─────────────────┐
        ▼                 ▼               ▼                 ▼
  records-worker-0  records-worker-1  ...            records-worker-N
   bounded pool      bounded pool                     bounded pool
        │                 │                                 │
        └──────── SETNX idempotency key ───────────────────┘
                          │
                 idempotent sink write
                          │
                        XACK   (only after the write commits)

  reclaimer: XAUTOCLAIM sweeps entries idle longer than min-idle onto live pods
  pod names are consumer names — a restarted pod rejoins as itself
```

**Exactly-once.** Every record carries a caller-supplied idempotency key. A worker claims the key with `SETNX` before writing, flips it to a terminal marker after, and only then sends `XACK`. The ordering is the guarantee: any crash before the ack leaves the entry in the pending entries list for redelivery, and any redelivery finds the key already committed and skips the write. `internal/store/idempotent.go` holds the claim and commit scripts.

**Lease-based reclaim.** A consumer's lease on a message is its idle time in the PEL. `XAUTOCLAIM` transfers any entry idle longer than `min-idle` to the node running the sweep, so a dead node's in-flight work is picked up without a membership protocol or a leader. The tradeoff sits entirely in `min-idle`: too low and a slow-but-alive node has live work stolen, costing duplicate work that the sink then has to collapse; too high and every crash stalls those records for a full lease before anyone touches them. Default is 5s, roughly 5x p99 handler latency.

**Backpressure.** There is no queue in front of the pool. A node reads at most `capacity - in_flight` entries per `XREADGROUP`, and `Submit` blocks once the window is full, which stops reads entirely until a slot frees. Unread work stays in Redis, which is the only component budgeted to hold it, and a saturated node stops claiming records it cannot finish inside the lease.

**Graceful drain.** `SIGTERM` cancels intake first, then waits for in-flight handlers to finish and ack before the process exits; handler contexts deliberately do not inherit the shutdown cancellation, so no record is left written-but-unacked. Anything still running at the drain deadline is left pending on purpose — the reclaimer is the safe fallback. `DrainNode` on the gRPC API triggers the same path remotely for rolling deploys.

**The operator.** One `ProcessingJob` is one consumer group, reconciled into a StatefulSet of workers plus the headless Service that governs it, both owned by the job so deletion cascades. A StatefulSet rather than a Deployment because a consumer group identifies its members by name: `records-worker-3` restarts as `records-worker-3`, rejoins as the same consumer, and finds its own pending entries — where a Deployment's generated pod name would make every restart a new consumer and leave the old one's records waiting out a lease. Ordinals also make the scale-in victim knowable in advance, which is what makes the drain protocol below possible at all.

**Autoscaling on stream lag.** `desired = clamp(ceil(streamLag / targetLagPerWorker), minReplicas, maxReplicas)`, sampled every 5 seconds from the coordinator's existing `GetClusterStats`. Lag rather than CPU because the backpressure design makes CPU meaningless as a signal — a saturated worker sits at the same utilization whether the backlog is 100 records or 10 million — while lag *is* the queue depth. Scale-up is immediate; scale-down must hold below the threshold continuously for `stabilizationWindow`, because scaling in costs a drain and an oscillating backlog would otherwise drain a worker every few seconds.

**Safe scale-down.** Before reducing replicas, the operator calls `DrainNode` on the highest-ordinal pod and waits for it to disappear from `GetClusterStats` — a positive signal that it acked everything it owned — and only then patches the replica count. If the drain outruns its budget the operator logs it and gives up on that scale-in; it never forces. Deleting a pod that still holds checked-out records would strand them for a full lease and buy nothing that a few more seconds of patience would not. A drained worker also removes itself from the consumer group, guarded so a consumer holding pending entries is never deleted — Redis discards a deleted consumer's PEL outright, which would turn a tidy-up into record loss.

**GPU-aware placement.** `resources` is passed through to the worker container verbatim, so an `nvidia.com/gpu` limit reaches the pod spec for the scheduler and the device plugin to act on. `placement.policy: Spread` emits topology spread constraints over hostname and zone; `BinPack` emits none and expects `placement.schedulerName` to point at the `MostAllocated` scheduler profile in [`deploy/scheduler`](deploy/scheduler). This is scheduler *configuration*, not a scheduler plugin — the operator emits scheduling inputs and kube-scheduler decides. Bin-packing matters for GPUs because they are indivisible: spread leaves eight stranded single GPUs, bin-pack leaves a contiguous 4-GPU block, and only the second can host a later 4-GPU job.

## Quickstart

### Docker Compose

```sh
make up      # Redis + coordinator + 8 workers + Prometheus (localhost:9091)
make bench   # drive load, print achieved records/sec
make test    # unit tests plus the kill-a-node zero-loss integration test
```

### Kubernetes

_(implemented, not yet verified on a live cluster — see Status)_

```sh
make kind-up                                              # cluster, image build + load, helm install
kubectl apply -n dpe-system -f examples/processingjob.yaml
kubectl get processingjobs -n dpe-system -w
```

```
NAME      REPLICAS   READY   LAG     PENDING   AGE
records   6          6       2431    384       12m
```

`LAG` is the autoscaler's input; `PENDING` is what is in flight cluster-wide. Run the end-to-end suite with `make e2e`, tear down with `make kind-down`.

## Design tradeoffs

- **At-least-once delivery with an idempotent sink, not distributed transactions.** A two-phase commit across Redis and the sink would give exactly-once delivery, at the cost of a coordinator on the hot path and blocking on coordinator failure. Keying every write and deduplicating at the sink moves the cost to one extra Redis round trip per record and keeps failure handling local to a node.
- **Lease timeout tuned against duplicate-work risk.** Reclaim is time-based rather than failure-detector-based, so `min-idle` trades recovery latency directly against wasted duplicate work: 5s keeps steals rare for a p99 of ~1s while bounding recovery to roughly `min-idle + reclaim interval`. Workloads with long tail latency should raise it rather than accept the churn.
- **Redis Streams over Kafka for operational simplicity at this scale.** Kafka gives partitioned ordering, longer retention and higher ceilings, but adds a broker cluster to run. At single-digit-thousands records/sec, one Redis instance already provides consumer groups, a pending entries list and `XAUTOCLAIM` — the three primitives this design needs — and it is the same instance used for idempotency keys, so there is exactly one stateful dependency to operate.
- **StatefulSet over Deployment, for consumer identity.** A Deployment's generated pod names would make every restart a new consumer, leaving the old one's pending entries to wait out a full lease before a peer reclaims them — turning every rolling deploy into the failure path and littering the group with dead consumers. The StatefulSet's cost is a required headless Service and ordered semantics this workload does not need (defused with `podManagementPolicy: Parallel`); the payoff is that a restarted pod recovers its own work, and that scale-in removes a *known* ordinal, which is what makes draining it first possible.
- **Drain before scaling in, rather than trusting the reclaimer.** The reclaimer would recover a force-deleted pod's records correctly — that is what it is for — but it would take a full `min-idle` to notice and would redo work already in progress. Draining first costs a round trip to the coordinator and up to `drainTimeout` of waiting per replica removed, and makes planned scale-in cost nothing. The reclaimer stays the fallback for the unplanned case, which is why a timed-out drain is abandoned rather than forced.
- **A scheduler profile, not a scheduler plugin.** A custom `ScorePlugin` could score on things a profile cannot express — consumer-group membership, per-node lag — but means compiling and operating a scheduler binary that must track Kubernetes releases, and on a managed control plane a second scheduler deployment regardless. A `KubeSchedulerConfiguration` with `MostAllocated` scoring gets the GPU packing behaviour that actually matters here as a YAML file with no new binary. The honest limit: it can only weight resources the scheduler already knows about, and installing it still needs control-plane access or a second scheduler.

## Layout

```
cmd/{worker,producer,coordinator}   node, load generator, gRPC control plane
cmd/operator                        controller-runtime manager for ProcessingJob
cmd/placementsim                    deterministic bin-pack vs spread simulator
api/proto/engine.proto              SubmitBatch, GetClusterStats, DrainNode
api/v1alpha1                        ProcessingJob CRD types, defaults, deepcopy
internal/controller                 reconcile loop, lag autoscaler, drain protocol
internal/placement                  kube-scheduler NodeResourcesFit scoring model
internal/stream                     consumer group (XADD/XREADGROUP/XACK) + XAUTOCLAIM reclaim
internal/worker                     bounded pool, backpressure, drain; node runtime
internal/store                      idempotency keys and the exactly-once write path
internal/{grpc,cluster,metrics}     Engine service, node registry, Prometheus collectors
config/                             CRD and RBAC generated from the API markers
deploy/helm/dpe                     chart: operator, coordinator, Redis, CRD
deploy/scheduler                    MostAllocated kube-scheduler profile for GPU bin-packing
deploy/kind                         four-node cluster definition for the e2e path
deploy/                             docker-compose (Redis, Prometheus, 8 workers)
examples/                           ready-to-apply ProcessingJob manifests
bench/                              harnesses, fixtures and committed raw results
test/integration_test.go            kill-a-node test asserting zero record loss
test/e2e                            operator suite against kind, behind the e2e build tag
docs/DESIGN.md                      operator design, failure modes, what is out of scope
```

## License

MIT — see [LICENSE](LICENSE).

---
Built by [Trung Nguyen](https://github.com/trungprofile) · [LinkedIn](https://www.linkedin.com/in/trung-n-nguyen)
