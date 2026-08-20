# bench

Benchmark harnesses and their raw output. Every number quoted in the top-level
README traces back to a file in [`results/`](results); anything without a file
here is marked `not measured` there rather than estimated.

## What ran

| Metric | Harness | Result | Environment |
| --- | --- | --- | --- |
| Placement, bin-pack vs spread (**simulated**) | `make bench-placement` | [`results/placement.json`](results/placement.json) | none — pure Go, no cluster |

### Placement simulation

```sh
make bench-placement
# or: go run ./cmd/placementsim -fixture=bench/fixtures/gpu-inference.yaml -out=bench/results/placement.json
```

`cmd/placementsim` replays kube-scheduler's `NodeResourcesFit` filter and both
of its scoring strategies over the inventory in
[`fixtures/gpu-inference.yaml`](fixtures/gpu-inference.yaml): 12 nodes (8 with
4 GPUs each, 4 CPU-only) and 40 pods (24 single-GPU inference workers, 16
CPU-only stream workers), with the same score weights as
`deploy/scheduler/binpack-profile.yaml` (gpu 10, cpu 1, memory 1).

Reproducing it needs nothing but a Go toolchain. The run is deterministic —
fixed expansion order, node names sorted, ties broken by name — so the same
fixture always produces byte-identical output, and `TestDeterminism` in
`internal/placement` asserts that.

**This is a simulation, not a measurement of a cluster.** It models the
scheduler's scoring arithmetic exactly, and models nothing else: no affinity,
taints, preemption, priority, or the other plugins that score alongside
`NodeResourcesFit`. The one deliberate divergence from upstream is tie-breaking
— the real scheduler picks randomly among equal top scorers, which cannot
produce a reproducible result. Read the output as "what the scoring strategy
prefers", not as "what a cluster did".

## What did not run, and why

No cluster-dependent benchmark was recorded. **The environment this work was
done in has no Docker daemon**, so kind could not create a cluster, no image
could be built or loaded, and the Helm chart could not be installed. The
harnesses below are written and syntax-checked but have never been executed;
their result files do not exist and their rows are absent from the README's
results table.

| Metric | Harness | What is needed |
| --- | --- | --- |
| Reclaim latency after pod eviction | `bench/operator-latency.sh reclaim` | kind cluster, `make kind-up`, kubectl + jq + grpcurl |
| Scale-up latency | `bench/operator-latency.sh scaleup` | as above |
| Scale-down safety (zero loss) | `make e2e` → `TestSafeScaleDown` | as above |
| Throughput under the operator | `bench/throughput.sh` | as above, plus a cluster wide enough for 8 workers |

### Reproducing the cluster-dependent runs

```sh
make kind-up                                    # cluster, image build + load, helm install
kubectl apply -n dpe-system -f examples/processingjob.yaml
kubectl port-forward -n dpe-system svc/dpe-coordinator 9090:9090 &

bench/operator-latency.sh reclaim               # 20 runs -> results/reclaim.json
bench/operator-latency.sh scaleup               # 10 runs -> results/scaleup.json
WORKERS=8 bench/throughput.sh                   # -> results/throughput.json
make e2e                                        # includes the scale-down zero-loss assertion
make kind-down
```

Record these alongside any results, because none of the figures mean anything
without them:

- **Hardware** — cores, RAM, and whether the kind nodes share one host.
- **kind and Kubernetes versions** — `kind version`, `kubectl version`.
- **Node count** — `deploy/kind/cluster.yaml` ships 1 control plane + 3 workers.
- **Record size** — 256 bytes to match the Docker Compose baseline.
- **Concurrency** — the `ProcessingJob`'s `spec.concurrency`, 64 in the example.
- **`min-idle`** — `spec.minIdle`. This one dominates reclaim latency: recovery
  is bounded below by the lease and cannot be faster than it. The example uses
  3s; the Compose baseline used 5s, so the two are not directly comparable
  without saying so.

A reclaim measurement taken with a different `min-idle` than the one it is
compared against measures the configuration, not the system.

### On statistics

`operator-latency.sh` reports p50 and p95 by nearest rank over 20 and 10 runs
respectively. At those sample sizes a p95 is one or two observations, not a
tight bound — quote the p50 as the typical case and the max as the observed
worst case, and raise `RUNS_RECLAIM` before making any stronger claim.

## Baselines from the existing engine

The Docker Compose figures in the top-level README (1.2K → 8.9K records/sec
over 8 workers, 92% scaling efficiency, zero loss under node kill, <1.8s
recovery) predate this work and were measured with `make up && make bench` on
the setup described there. They are **not** Kubernetes results and must never
be presented as such; `bench/throughput.sh` exists precisely so that the
Kubernetes number can be measured rather than inferred from them.
