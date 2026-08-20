# Design: the ProcessingJob operator

This document covers the Kubernetes layer added on top of the processing
engine: what the reconcile loop does, why replicas follow stream lag rather
than CPU, how a worker is drained before its pod is removed, which failure
modes were considered, and what was deliberately left out.

The engine underneath — at-least-once delivery through a Redis Streams
consumer group, made exactly-once by an idempotency key claimed before the sink
write and committed before the ack — is unchanged and documented in the
top-level [README](../README.md).

---

## 1. What a ProcessingJob is

One `ProcessingJob` is one consumer group. It says which stream to drain, how
wide the worker pool may grow, and where its pods may land:

```yaml
apiVersion: dpe.trungprofile.dev/v1alpha1
kind: ProcessingJob
metadata:
  name: records
spec:
  stream: "dpe:records"
  group: "dpe-workers"
  redisAddr: "dpe-redis:6379"
  image: "dpe:0.1.0"
  concurrency: 64
  minIdle: 3s
  drainTimeout: 30s
  autoscale:
    minReplicas: 1
    maxReplicas: 16
    targetLagPerWorker: 500
    stabilizationWindow: 60s
  placement:
    policy: Spread
```

The operator owns exactly two objects per job: a headless `Service` and a
`StatefulSet` of workers, both with owner references back to the job so
deleting the job cascades.

---

## 2. StatefulSet, not Deployment

This is the most consequential decision in the operator, and it is forced by
the engine's design rather than chosen for taste.

A Redis Streams consumer group identifies its members **by name**. A worker
joins as a consumer, and every entry it reads is recorded in the group's
pending entries list (PEL) **against that consumer name** until it is acked.
The name is not decoration — it is the key that says who owns which in-flight
records.

The worker takes its node ID from `os.Hostname()`, which under Kubernetes is
the pod name. So:

- **A `StatefulSet` gives `records-worker-0 … records-worker-N`.** A pod that
  restarts comes back with the same name, rejoins as the same consumer, and
  finds its own pending entries waiting. Recovery is a local matter.
- **A `Deployment` gives `records-worker-7d9f8b6c4-x8k2p`.** Every restart is a
  brand-new consumer. The old one lingers in the group holding pending entries
  that nobody will ever ack, and those records wait out a full `min-idle` lease
  before a peer's `XAUTOCLAIM` sweep recovers them. A month of rolling deploys
  leaves hundreds of dead consumers in `XINFO CONSUMERS`.

The engine survives either — the reclaimer is exactly the mechanism for
orphaned entries — but a Deployment turns *every* pod restart into the failure
path. A StatefulSet keeps the failure path for actual failures.

Two consequences fall out of the same property:

- **Ordinals make scale-in deterministic.** A StatefulSet always removes the
  highest ordinal, so the operator knows which worker a scale-in will take
  *before* it happens. That is what makes drain-then-scale possible at all; a
  Deployment's scale-in victim is chosen by the controller after the fact.
- **`podManagementPolicy: Parallel`.** Consumer-group members are independent —
  pod 7 does not need pod 6 to be ready before it can read. Parallel management
  makes a scale-up from 2 to 12 arrive in one pod-start latency rather than ten
  in series. The ordered-startup guarantee a StatefulSet normally provides buys
  nothing here; the stable identity is the only part that matters.

The one cost is the headless `Service` a StatefulSet requires. It is cheap, and
it gives per-pod DNS for scraping metrics off individual workers.

---

## 3. The reconcile loop

Every reconcile, and every 5 seconds thereafter:

```
 ┌─ Reconcile(job) ────────────────────────────────────────────────────┐
 │                                                                      │
 │  1. Get the ProcessingJob        not found → drop timers, return     │
 │  2. Resolve defaults             spec.Defaulted()                    │
 │  3. Ensure the headless Service  create or repair                    │
 │  4. Ensure the StatefulSet       create at minReplicas, or roll the   │
 │                                  pod template if its spec-hash moved  │
 │  5. GetClusterStats  ──gRPC──▶   coordinator                          │
 │        unreachable → set Degraded, hold replicas, requeue             │
 │  6. Converge replicas            §4 autoscale, §5 drain-then-scale    │
 │  7. Update status subresource    lag, pending, totals, conditions     │
 │  8. Requeue after 5s                                                  │
 └──────────────────────────────────────────────────────────────────────┘
```

Plus a watch on the owned `StatefulSet`, so a pod becoming ready wakes the loop
immediately rather than waiting out the interval.

### Reading state through the coordinator, not Redis

Step 5 dials the coordinator's existing `GetClusterStats` RPC rather than
issuing `XINFO GROUPS` from the controller. The controller could read Redis
directly and skip a hop — but the consumer-group depth, the node registry and
the drain broadcast already have exactly one owner, and giving the operator its
own copy of that logic would give two components licence to disagree about how
much work is outstanding. Reusing the control plane also means the operator
sees precisely what an engineer running `make stats` sees, which matters when
someone is trying to work out why the autoscaler did what it did.

The cost is a dependency: if the coordinator is down, the operator is blind.
That is handled explicitly (§6).

### Not rewriting what has not changed

A naive controller rebuilds the desired `StatefulSet`, compares it to the live
one, finds a difference (the API server defaults dozens of pod-spec fields it
never set) and writes — every single reconcile, forever.

Instead the operator stores a SHA-256 of the pod template it asked for in a
`dpe.trungprofile.dev/spec-hash` annotation and compares hashes. The hash
deliberately **excludes** replica count, so a scale event does not read as a
template change and roll every pod. `TestSteadyStateReconcileIsAWriteFreeNoOp`
asserts that a settled job's `resourceVersion` does not move.

---

## 4. Autoscaling on stream lag

```
desired = clamp(ceil(streamLag / targetLagPerWorker), minReplicas, maxReplicas)
```

### Why lag and not CPU

Lag is the only signal proportional to unfinished work.

The engine has no queue in front of its pool. A worker reads at most
`capacity - in_flight` entries per `XREADGROUP`, and `Submit` blocks once the
window is full — which stops reads entirely. That backpressure is deliberate
and it is exactly what makes CPU useless as a scaling signal: **a saturated
worker looks identical at 100 records of backlog and at 10 million.** It is
pinned at its concurrency limit either way. A CPU-driven autoscaler cannot tell
a cluster that is keeping up from one that is falling ten hours behind.

Consumer-group lag can. It counts entries never delivered to any consumer, so
it *is* the queue depth, and dividing by a per-worker absorption rate gives a
replica count with a physical meaning: how many consumers it takes to work this
backlog off in the time budget `targetLagPerWorker` implies.

Two details in that formula:

- **Lag excludes the pending entries list.** Entries already checked out are
  someone's in-flight work; adding a worker does not make them finish sooner.
  Counting them would inflate desired replicas by exactly the amount of work
  already being handled.
- **The division rounds up.** A backlog of one record still gets a worker.

`targetLagPerWorker` is the tuning surface, and it is a latency budget in
disguise: at a per-worker throughput of *R* records/sec, a target of *L* means
the job tolerates roughly *L/R* seconds of backlog before it adds capacity. 500
against a worker doing ~1.1K records/sec is a sub-second budget.

### Asymmetric response

Scale-up is immediate. Backlog is already costing latency, and an extra
consumer costs nothing but a pod.

Scale-down waits for `stabilizationWindow` — a lower desired count must hold
*continuously*, and any reconcile wanting the current count or more resets the
clock. The asymmetry is not caution for its own sake: scaling in costs a drain,
and a backlog oscillating around the threshold would otherwise drain a worker
every five seconds forever. Each step down earns its own window, so scaling
from 12 to 4 is eight damped steps rather than one cliff.

Those timers live in memory rather than in status, because they are debounce
state, not facts about the cluster. An operator restart clears them, which
costs at most one extra window before the first scale-in — strictly the safe
direction.

---

## 5. The drain protocol

A pod holding checked-out records must never simply disappear. If it does,
those entries sit in the PEL owned by a consumer that no longer exists, and
they wait out a full `min-idle` lease before a peer reclaims them. That is a
correctness-preserving outcome — the engine is built for it — but it is a
*recovery* path, and using it for a planned scale-in is choosing to lose time
on purpose.

So scaling in is a protocol, not a patch:

```
  operator                coordinator            worker-N            Redis
     │                         │                     │                  │
     │  desired < current, window elapsed             │                  │
     │────DrainNode(worker-N)─▶│                     │                  │
     │                         │──PUBLISH drain──────▶│                  │
     │                         │                     │ stop reading     │
     │                         │                     │ finish in-flight │
     │                         │                     │───XACK──────────▶│
     │                         │                     │  (PEL empties)   │
     │                         │                     │──DELCONSUMER────▶│
     │                         │                     │ deregister       │
     │                         │                     ✗ exits            │
     │────GetClusterStats─────▶│                     │                  │
     │◀───worker-N absent──────│                     │                  │
     │                                                                  │
     │  patch StatefulSet replicas = current - 1                        │
     ▼                                                                  │
```

Each step is load-bearing:

- **The victim is known in advance.** A StatefulSet removes the highest
  ordinal, so the operator can name `records-worker-N-1` before touching
  anything. (§2)
- **The drain request is the existing `DrainNode` RPC** — the same one an
  engineer uses by hand — which publishes on the registry's drain channel. It
  is idempotent: re-broadcasting to a node that already left is a no-op.
- **"Drained" means gone from the registry,** not "we waited a bit". The worker
  deregisters only after its pool is empty, so its absence from
  `GetClusterStats` is a positive statement that it acked everything it owned.
- **Only then is `spec.replicas` patched.** The kubelet then sends SIGTERM to a
  worker that is already idle, and `terminationGracePeriodSeconds`
  (`drainTimeout + 5s`) guarantees it is not killed mid-drain even so.
- **One replica at a time.** Removing several at once takes a large slice of
  the group's capacity out of service simultaneously.

### When a drain does not finish

The operator **does not force.** If the victim is still registered after
`drainTimeout`, the attempt is logged and abandoned, the replica count stays
where it is, and the next reconcile decides afresh from current backlog.

Forcing would mean deleting a pod that is still holding records — trading a
slow scale-in for guaranteed duplicate work and a lease-length stall. A stuck
drain is a scale-in that did not happen, which is a cost the system can carry
indefinitely. `TestDrainTimeoutLeavesTheReplicaRunning` pins this.

If backlog recovers while a drain is in flight, the scale-in is abandoned
outright rather than completed and then undone: the StatefulSet restarts the
drained pod under the same name, and it rejoins as the same consumer.

### Consumer cleanup

A clean drain also issues `XGROUP DELCONSUMER`, because under Kubernetes a
scaled-in pod never comes back under that name and would otherwise accumulate
in the group forever.

This is guarded rather than unconditional, and the guard is the interesting
part: **Redis discards a deleted consumer's pending entries outright.** They
are neither acked nor reclaimable afterwards. Deleting a consumer that still
owns work would convert a tidy-up into silent record loss — precisely the
failure the whole engine exists to prevent. So `RemoveConsumer` checks the PEL
first and declines if anything is pending, and it is only ever called on the
clean-drain path, never after a timeout.

---

## 6. Failure modes considered

**The operator crashes mid-drain.** The drain is the *worker's* own SIGTERM
path, not something the operator steps through, so it continues regardless. The
new leader re-observes the node in the registry, re-broadcasts the idempotent
drain request and starts its own clock. Worst case: one extra broadcast and one
extra stabilization window.

**Redis is unavailable.** The operator sees `GetClusterStats` fail (the
coordinator cannot reach Redis either) and holds the replica count. Workers are
already blocked on their own reads and will resume when Redis returns; nothing
about scaling the pool helps a broker outage, and scaling *down* during one
would be actively harmful.

**The coordinator is unreachable.** The operator sets `Degraded=true` with the
dial error, leaves replicas untouched and requeues. This is the deliberate
choice made in §3's tradeoff: without stats there is no lag signal, and acting
without one means guessing. Workers read Redis directly and are entirely
unaffected — a control-plane outage costs autoscaling, not throughput.
`TestCoordinatorUnreachableHoldsTheReplicaCount` pins it.

**A pod is stuck terminating.** The operator does not delete pods, so it cannot
make this worse. The StatefulSet controller will not create a replacement until
the old pod's name is free; meanwhile that worker's entries stay in the PEL and
peers reclaim them after `min-idle`. The job reports `Progressing=true` with
`readyReplicas < replicas` until the kubelet resolves it.

**Two operators reconcile the same job.** Prevented by leader election, which
is on by default. Without it, two operators could pick different scale-in
victims and drain two workers for one unit of scale-in.

**A worker is slow rather than dead.** The engine's existing tradeoff, unchanged
by the operator: `min-idle` bounds how long a live-but-slow worker keeps its
lease before a peer steals the entry. The steal costs duplicate work that the
idempotent sink collapses — never a lost or doubly-applied record.

**Someone edits the StatefulSet by hand.** The next reconcile finds a
spec-hash mismatch and rolls the pod template back. Replica count is
deliberately *not* reasserted on that path, because doing so would undo a drain
in progress.

---

## 7. GPU-aware placement

**The operator does not make placement decisions.** It emits scheduling inputs;
kube-scheduler decides. Being precise about that boundary matters, because the
two halves are frequently conflated:

1. **`resources` is passed through verbatim.** `nvidia.com/gpu` is an extended
   resource, meaningless to the operator — the scheduler filters on it and the
   node's device plugin satisfies it. The operator's entire job is to make sure
   it reaches the pod spec unmodified, and it deep-copies rather than aliasing
   so a mutation can never corrupt the informer cache.

2. **`placement.policy` selects scheduling intent.** `Spread` emits topology
   spread constraints over hostname and zone; `BinPack` emits none, because
   anti-affinity would fight the packing. The constraints use `ScheduleAnyway`
   rather than `DoNotSchedule`: a group that cannot spread should still run. An
   unschedulable worker costs throughput immediately, while an unevenly spread
   one costs only a larger reclaim if that node is lost — and the reclaim path
   is the part of this system that is already proven.

3. **`placement.schedulerName` routes pods to a scheduler profile.**
   `deploy/scheduler/binpack-profile.yaml` is a `KubeSchedulerConfiguration`
   setting `NodeResourcesFit` to `MostAllocated` scoring with `nvidia.com/gpu`
   weighted ten times cpu and memory. **`policy: BinPack` alone changes
   nothing about scoring** — without the profile, the default scheduler's
   `LeastAllocated` still spreads.

### Why bin-pack GPUs at all

The default `LeastAllocated` prefers the emptiest node. For GPUs that is the
wrong default, because GPUs are indivisible and cannot be oversubscribed.
Spreading eight single-GPU pods over eight 4-GPU nodes leaves every node
fragmented, and a later pod wanting four GPUs on one node will not schedule
even though the cluster has 24 free.

`cmd/placementsim` measures this by replaying the scheduler's own filter and
scoring arithmetic — the real `mostRequestedScore` / `leastRequestedScore`
functions, integer truncation and all — over a fixed inventory. On the
committed fixture (12 nodes, 32 GPUs, 40 pods; see
[`bench/results/placement.json`](../bench/results/placement.json)):

| Strategy | Nodes used | GPU utilization | Nodes with all GPUs free | Largest free GPU block |
| --- | --- | --- | --- | --- |
| MostAllocated (bin-pack) | 6 / 12 | 75.0% | 2 | 4 |
| LeastAllocated (spread) | 12 / 12 | 75.0% | 0 | 1 |

**Utilization is identical.** That is the point: every pod scheduled under both
strategies, so bin-packing did not fit more work in. What changed is
fragmentation — bin-pack leaves a contiguous 4-GPU block, spread leaves eight
stranded single GPUs. A subsequent 4-GPU job schedules in the first cluster and
does not schedule in the second.

The simulation surfaced a second, less obvious effect. With GPUs weighted
heavily, an *untouched* GPU node scores 100 on the GPU term under
`LeastAllocated` — so spreading pulls **CPU-only** pods onto GPU nodes,
consuming cpu and memory that GPU work will later need. Bin-pack inverts it: an
empty GPU term scores 0, so CPU-only pods land on CPU-only nodes.
`TestZeroGPUPodsIgnoreGPUScoring` pins both directions.

Every figure above is **simulated**. It models the scheduler's scoring exactly
and models nothing else — no affinity, taints, preemption, priority, or the
other plugins that score alongside `NodeResourcesFit`. The one deliberate
divergence is tie-breaking: upstream picks randomly among equal top scorers,
which cannot produce a reproducible result, so ties break on node name here.

---

## 8. Deliberately out of scope

**A custom scheduler plugin.** Writing a `ScorePlugin` in Go would allow
scoring on things a profile cannot express — consumer-group membership, say, or
current lag per node. It would also mean compiling and operating a scheduler
binary, and on a managed control plane, running a second scheduler regardless.
The configuration route gets the GPU packing behaviour that actually matters
here with a YAML file and no new binary to keep in step with Kubernetes
releases. If a future requirement genuinely needs stream state in the scoring
decision, the plugin becomes worth its cost; nothing here does.

**Multi-cluster.** One `ProcessingJob` is one consumer group against one Redis.
Spanning clusters means either a shared Redis across cluster boundaries or
cross-cluster coordination, and both are much larger problems than the one this
solves.

**A `ScaledObject`-style external metrics adapter.** Exposing lag through the
custom metrics API would let a stock HPA drive the replica count. That is the
more conventional shape, and it is genuinely tempting — but an HPA scales by
patching `spec.replicas`, which is precisely the operation that must not happen
without a drain first (§5). Two writers of that field is the bug the drain
protocol exists to prevent. Owning the replica count is the price of owning the
scale-in safety.

**Webhooks.** Defaulting and validation come from the CRD's OpenAPI schema,
generated from the kubebuilder markers on the types. A mutating or validating
webhook would add a certificate to manage and a failure mode where the API
server cannot admit a `ProcessingJob` because the operator is down. `Defaulted()`
resolves the same values in-process, so the controller behaves identically
whether an object arrived through the API server or was built in a test.

**Redis Cluster mode, Kafka, multi-tenancy, authentication, a web UI.**
Unchanged from the engine's original scope.

---

## 9. Status and printer columns

`kubectl get processingjobs` is the operator's primary interface, so the status
subresource is designed to make one line answer "is this working?":

```
NAME      REPLICAS   READY   LAG     PENDING   AGE
records   6          6       2431    384       12m
```

- `REPLICAS` / `READY` — convergence at a glance.
- `LAG` — the autoscaler's input. Rising lag with `REPLICAS` at `maxReplicas`
  means the ceiling is the constraint.
- `PENDING` — records in flight cluster-wide. Non-zero with zero lag means the
  cluster is finishing up; persistently non-zero with zero lag means entries
  are stuck and being reclaimed in a loop.

Three conditions carry the detail: `Ready` (every desired replica ready),
`Progressing` (converging, including during a drain) and `Degraded` (the
operator cannot observe the cluster). They are set in exactly one place so
their meanings cannot drift apart.

---

Built by [Trung Nguyen](https://github.com/trungprofile) · [LinkedIn](https://www.linkedin.com/in/trung-n-nguyen)
