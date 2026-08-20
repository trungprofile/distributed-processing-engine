# config

Manifests generated from the kubebuilder markers in `api/v1alpha1` by
`controller-gen`: the ProcessingJob CustomResourceDefinition under `crd/bases`
and the operator's ClusterRole under `rbac`.

They are the source of truth for what the Helm chart in `deploy/helm/dpe`
ships — regenerate with `make manifests` after changing the API types or the
`+kubebuilder:rbac` markers on the controller, and copy the CRD into the
chart's `crds/` directory in the same commit.
