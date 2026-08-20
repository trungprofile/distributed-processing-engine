COMPOSE      ?= docker compose -f deploy/docker-compose.yml
REDIS_ADDR   ?= localhost:6379
BENCH_RATE   ?= 10000
BENCH_TIME   ?= 60s
BENCH_SIZE   ?= 256
GO           ?= go

# Kubernetes path. KIND_IMAGE is built locally and loaded into the cluster, so
# it never needs a registry; the chart's IfNotPresent pull policy keeps it.
KIND_CLUSTER  ?= dpe
KIND_IMAGE    ?= dpe:e2e
K8S_NAMESPACE ?= dpe-system
HELM_RELEASE  ?= dpe
CONTROLLER_GEN ?= controller-gen
PLACEMENT_FIXTURE ?= bench/fixtures/gpu-inference.yaml

.PHONY: help up down bench test unit build vet fmt proto stats drain logs clean \
        manifests kind-up kind-down operator-install operator-run e2e \
        bench-operator bench-placement

help: ## List targets
	@grep -hE '^[a-z0-9-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-17s %s\n", $$1, $$2}'

up: ## Start Redis, the coordinator, 8 workers and Prometheus
	$(COMPOSE) up -d --build
	@echo "coordinator gRPC :9090   prometheus http://localhost:9091"

down: ## Stop the cluster and remove volumes
	$(COMPOSE) down -v --remove-orphans

bench: ## Drive load through the running cluster and print the achieved rate
	$(COMPOSE) --profile bench run --rm producer \
		-redis=redis:6379 -rate=$(BENCH_RATE) -duration=$(BENCH_TIME) -size=$(BENCH_SIZE)
	@echo "throughput and lag: http://localhost:9091/graph?g0.expr=sum(rate(dpe_records_processed_total[30s]))"

test: ## Run every test, including the kill-a-node integration test
	$(COMPOSE) up -d redis
	@until docker exec $$($(COMPOSE) ps -q redis) redis-cli ping >/dev/null 2>&1; do sleep 0.5; done
	REDIS_ADDR=$(REDIS_ADDR) $(GO) test ./... -race -timeout 300s

unit: ## Run only the tests that need no Redis
	$(GO) test ./internal/... -race -timeout 120s

build: ## Build all four binaries into ./bin
	$(GO) build -trimpath -o bin/ ./cmd/...

vet: ## go vet
	$(GO) vet ./...

fmt: ## gofmt the tree
	gofmt -w $(shell find . -name '*.go' -not -name '*.pb.go')

proto: ## Regenerate gRPC stubs (requires buf, protoc-gen-go, protoc-gen-go-grpc)
	buf generate

manifests: ## Regenerate the CRD, deepcopy and RBAC from the API markers
	$(CONTROLLER_GEN) object:headerFile=/dev/null paths=./api/v1alpha1/...
	$(CONTROLLER_GEN) crd paths=./api/v1alpha1/... output:crd:artifacts:config=config/crd/bases
	$(CONTROLLER_GEN) rbac:roleName=dpe-operator paths=./internal/controller/... output:rbac:artifacts:config=config/rbac
	cp config/crd/bases/dpe.trungprofile.dev_processingjobs.yaml deploy/helm/dpe/crds/

kind-up: ## Create the kind cluster, load the image and install the Helm chart
	kind create cluster --config deploy/kind/cluster.yaml --name $(KIND_CLUSTER)
	docker build -t $(KIND_IMAGE) .
	kind load docker-image $(KIND_IMAGE) --name $(KIND_CLUSTER)
	helm upgrade --install $(HELM_RELEASE) deploy/helm/dpe \
		--namespace $(K8S_NAMESPACE) --create-namespace \
		--set image.repository=$(word 1,$(subst :, ,$(KIND_IMAGE))) \
		--set image.tag=$(word 2,$(subst :, ,$(KIND_IMAGE))) \
		--set redis.persistence.enabled=false \
		--wait --timeout 5m
	@echo "installed. kubectl apply -n $(K8S_NAMESPACE) -f examples/processingjob.yaml"

kind-down: ## Delete the kind cluster
	kind delete cluster --name $(KIND_CLUSTER)

operator-install: ## Apply the CRD and the operator's RBAC to the current cluster
	kubectl apply -f config/crd/bases/dpe.trungprofile.dev_processingjobs.yaml
	kubectl apply -f config/rbac/role.yaml

operator-run: ## Run the operator locally against the current kubeconfig
	$(GO) run ./cmd/operator -leader-elect=false -zap-log-level=debug

e2e: ## Run the end-to-end suite against a cluster prepared by kind-up
	$(GO) test -tags e2e ./test/e2e/... -v -timeout 30m -count=1

bench-placement: ## Simulate bin-pack vs spread placement and write bench/results
	$(GO) run ./cmd/placementsim -fixture=$(PLACEMENT_FIXTURE) -out=bench/results/placement.json

bench-operator: ## Measure reclaim and scale-up latency on a running kind cluster
	bench/operator-latency.sh

stats: ## Print cluster stats via the coordinator (requires grpcurl)
	grpcurl -plaintext localhost:9090 engine.v1.Engine/GetClusterStats

drain: ## Ask one node to drain: make drain NODE=<node-id>
	grpcurl -plaintext -d '{"node_id":"$(NODE)"}' localhost:9090 engine.v1.Engine/DrainNode

logs: ## Tail worker logs
	$(COMPOSE) logs -f worker

clean: ## Remove build output
	rm -rf bin
