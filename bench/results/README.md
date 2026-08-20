# bench/results

Raw output from benchmark runs. A number appears in the top-level README only
if it can be traced to a file here.

| File | Status |
| --- | --- |
| `placement.json` | present — written by `make bench-placement`; **simulated**, no cluster involved |
| `reclaim.json` | not present — needs a kind cluster, see [`../README.md`](../README.md) |
| `scaleup.json` | not present — same |
| `scaledown.json` | not present — same |
| `throughput.json` | not present — same |

Each file carries a `measured` or `simulated` flag in its schema, so a reader
can tell what kind of claim it supports without leaving the file.
