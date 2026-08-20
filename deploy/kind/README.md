# deploy/kind

Cluster definition for the local Kubernetes path: `make kind-up` creates it,
builds the image, loads it into the nodes and installs the Helm chart;
`make e2e` runs the suite in `test/e2e` against it; `make kind-down` deletes it.

Three worker nodes, because a single-node cluster cannot distinguish a spread
placement from a bin-packed one and gives the pod-eviction test nowhere to
reschedule.
