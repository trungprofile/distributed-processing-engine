# examples

Ready-to-apply `ProcessingJob` manifests. `processingjob.yaml` is the one the
README's Kubernetes quickstart applies, and it matches the services the Helm
chart installs under its default release name.

`processingjob-gpu.yaml` shows the two placement knobs together: a
`nvidia.com/gpu` limit passed through to the worker pods, and the bin-packing
scheduler profile from [`deploy/scheduler`](../deploy/scheduler) selected by
name.
