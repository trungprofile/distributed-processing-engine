# dpe Helm chart

Installs the ProcessingJob CRD, the operator, the gRPC coordinator and an
optional single-instance Redis. The worker StatefulSets are not part of the
chart — the operator creates one per `ProcessingJob`.

```sh
helm install dpe deploy/helm/dpe --namespace dpe-system --create-namespace
kubectl apply -n dpe-system -f examples/processingjob.yaml
kubectl get processingjobs -n dpe-system -w
```

## What it renders

| Object | Purpose |
| --- | --- |
| `CustomResourceDefinition` | `processingjobs.dpe.trungprofile.dev`, installed from `crds/` |
| `Deployment` (operator) | reconciler, leader election on, metrics `:8080`, probes `:8081` |
| `Deployment` + `Service` (coordinator) | `SubmitBatch`, `GetClusterStats`, `DrainNode` on gRPC `:9090` |
| `StatefulSet` + `Service` (redis) | stream, idempotency keys and node registry; `redis.enabled=false` to use your own |
| `ClusterRole`/`ClusterRoleBinding` | the verbs the reconcile loop issues, no wildcards |
| `Role`/`RoleBinding` | leases for leader election, namespaced |

## Values worth knowing

Every field in [`values.yaml`](values.yaml) is commented; these are the ones
that change behaviour rather than sizing.

- `redis.enabled` / `redis.externalAddr` — bring your own Redis. The render
  fails rather than installing a broken stack if you disable the bundled one
  without supplying an address.
- `redis.maxmemoryPolicy` — `noeviction` by design. An eviction policy lets
  Redis drop a stream entry under memory pressure, which loses a record the
  cluster still believes it owns.
- `operator.leaderElection` — keep it on with more than one replica. Two
  operators could otherwise pick different scale-in victims and drain two
  workers for one unit of scale-in.
- `operator.watchNamespace` — narrows the informer cache to one namespace.
- `coordinator.stream` / `coordinator.group` — must match the `ProcessingJob`
  spec, or the operator autoscales on a different consumer group's backlog.

## CRD upgrades

The CRD lives in `crds/`, which Helm installs but never upgrades or deletes.
After changing `api/v1alpha1`, run `make manifests` and apply the regenerated
CRD explicitly:

```sh
kubectl apply -f config/crd/bases/dpe.trungprofile.dev_processingjobs.yaml
```
