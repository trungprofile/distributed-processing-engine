# bench/fixtures

Inputs for `cmd/placementsim`: a node inventory and a pod workload in YAML,
using Kubernetes quantity syntax so `"500m"` and `16Gi` mean what they mean in
a pod spec.

A fixture is committed alongside its results so any number in
`bench/results/placement.json` can be traced back to the exact cluster shape
that produced it. Change a fixture and the simulator's output changes with it —
rerun `make bench-placement` in the same commit.
