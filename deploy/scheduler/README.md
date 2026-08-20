# deploy/scheduler

A `KubeSchedulerConfiguration` profile named `dpe-binpack` that scores nodes
with `NodeResourcesFit` in `MostAllocated` mode, weighting `nvidia.com/gpu`
ten times higher than cpu or memory.

This is configuration, not code: no scheduler plugin is written or compiled
here. A `ProcessingJob` opts in by setting `placement.schedulerName:
dpe-binpack`, and `binpack-profile.yaml` documents how to install the profile
on a kind cluster and what changes on a managed control plane. The measured
effect of the two scoring strategies on a fixed node inventory is in
[`bench/results/placement.json`](../../bench/results/placement.json),
produced by [`cmd/placementsim`](../../cmd/placementsim).
