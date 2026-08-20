# docs

Long-form design notes that would crowd the top-level README.

[`DESIGN.md`](DESIGN.md) covers the Kubernetes operator: the reconcile loop
step by step, why replica count follows consumer-group lag rather than CPU, the
drain protocol that runs before any scale-in, the failure modes considered, and
what was left out of scope on purpose.
