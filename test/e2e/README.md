# test/e2e

End-to-end tests for the operator, run against a real cluster (kind) with the
Helm chart already installed. They are behind the `e2e` build tag, so
`go test ./...` never picks them up and the default CI job stays fast.

```sh
make kind-up          # cluster, image build and load, helm install
make e2e              # go test -tags e2e ./test/e2e/...
make kind-down
```

Each test drives the same path a user would: apply a `ProcessingJob`, submit
load through the coordinator's gRPC API, and assert on cluster state and on the
sink. `DPE_E2E_*` environment variables (see `harness.go`) point the suite at
the namespace, image and Redis instance the chart installed.
