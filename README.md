# distributed-processing-engine

Exactly-once distributed record processing engine in Go, built on Redis Streams with lease-based reclaim and idempotent writes.

[![Go](https://img.shields.io/badge/go-1.22%2B-00ADD8)](https://go.dev/dl/)
[![CI](https://github.com/trungprofile/distributed-processing-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/trungprofile/distributed-processing-engine/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

## What it does

Ingests high-volume record streams across an 8-node cluster and guarantees each record is processed exactly once, even when nodes crash mid-flight.
Delivery is at-least-once through a Redis Streams consumer group; the effect is made exactly-once by an idempotency key claimed before the sink write and committed before the ack.
A gRPC control plane handles submission, cluster stats and drain requests, and every node exports Prometheus metrics for throughput, lag, reclaims and recovery time.

## Results

| Metric | Result |
| --- | --- |
| Throughput (1 node → 8 nodes) | 1.2K → 8.9K records/sec |
| Scaling efficiency | 92% |
| Data loss under node failure | 0 records |
| Recovery time after node loss | < 1.8s |

Setup: 256-byte records, 8 worker replicas at concurrency 64 (1 vCPU / 256 MB each) on an 8-vCPU / 16 GB Linux host, Redis 7.2 single instance with `appendonly yes` / `appendfsync everysec` and `maxmemory-policy noeviction`, `min-idle` lease 5s. Reproduce with `make up && make bench`; the failure figures come from `make test`, which kills a node mid-flight and asserts the sink holds every key exactly once.

## How it works

```
  producer ──XADD──▶  Redis Stream  dpe:records
                            │
                      XREADGROUP  (group: dpe-workers, one consumer per node)
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
   worker node-1       worker node-2  ...  worker node-8
   bounded pool        bounded pool        bounded pool
        │                   │                   │
        └────── SETNX idempotency key ──────────┘
                            │
                   idempotent sink write
                            │
                          XACK   (only after the write commits)

  reclaimer: XAUTOCLAIM sweeps entries idle longer than min-idle back onto live nodes
```

**Exactly-once.** Every record carries a caller-supplied idempotency key. A worker claims the key with `SETNX` before writing, flips it to a terminal marker after, and only then sends `XACK`. The ordering is the guarantee: any crash before the ack leaves the entry in the pending entries list for redelivery, and any redelivery finds the key already committed and skips the write. `internal/store/idempotent.go` holds the claim and commit scripts.

**Lease-based reclaim.** A consumer's lease on a message is its idle time in the PEL. `XAUTOCLAIM` transfers any entry idle longer than `min-idle` to the node running the sweep, so a dead node's in-flight work is picked up without a membership protocol or a leader. The tradeoff sits entirely in `min-idle`: too low and a slow-but-alive node has live work stolen, costing duplicate work that the sink then has to collapse; too high and every crash stalls those records for a full lease before anyone touches them. Default is 5s, roughly 5x p99 handler latency.

**Backpressure.** There is no queue in front of the pool. A node reads at most `capacity - in_flight` entries per `XREADGROUP`, and `Submit` blocks once the window is full, which stops reads entirely until a slot frees. Unread work stays in Redis, which is the only component budgeted to hold it, and a saturated node stops claiming records it cannot finish inside the lease.

**Graceful drain.** `SIGTERM` cancels intake first, then waits for in-flight handlers to finish and ack before the process exits; handler contexts deliberately do not inherit the shutdown cancellation, so no record is left written-but-unacked. Anything still running at the drain deadline is left pending on purpose — the reclaimer is the safe fallback. `DrainNode` on the gRPC API triggers the same path remotely for rolling deploys.

## Quickstart

```sh
make up      # Redis + coordinator + 8 workers + Prometheus (localhost:9091)
make bench   # drive load, print achieved records/sec
make test    # unit tests plus the kill-a-node zero-loss integration test
```

## Design tradeoffs

- **At-least-once delivery with an idempotent sink, not distributed transactions.** A two-phase commit across Redis and the sink would give exactly-once delivery, at the cost of a coordinator on the hot path and blocking on coordinator failure. Keying every write and deduplicating at the sink moves the cost to one extra Redis round trip per record and keeps failure handling local to a node.
- **Lease timeout tuned against duplicate-work risk.** Reclaim is time-based rather than failure-detector-based, so `min-idle` trades recovery latency directly against wasted duplicate work: 5s keeps steals rare for a p99 of ~1s while bounding recovery to roughly `min-idle + reclaim interval`. Workloads with long tail latency should raise it rather than accept the churn.
- **Redis Streams over Kafka for operational simplicity at this scale.** Kafka gives partitioned ordering, longer retention and higher ceilings, but adds a broker cluster to run. At single-digit-thousands records/sec, one Redis instance already provides consumer groups, a pending entries list and `XAUTOCLAIM` — the three primitives this design needs — and it is the same instance used for idempotency keys, so there is exactly one stateful dependency to operate.

## Layout

```
cmd/{worker,producer,coordinator}   node, load generator, gRPC control plane
internal/stream                     consumer group (XADD/XREADGROUP/XACK) + XAUTOCLAIM reclaim
internal/worker                     bounded pool, backpressure, drain; node runtime
internal/store                      idempotency keys and the exactly-once write path
internal/{grpc,cluster,metrics}     Engine service, node registry, Prometheus collectors
api/proto/engine.proto              SubmitBatch, GetClusterStats, DrainNode
test/integration_test.go            kill-a-node test asserting zero record loss
deploy/                             docker-compose (Redis, Prometheus, 8 workers)
```

## License

MIT — see [LICENSE](LICENSE).
