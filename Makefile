COMPOSE      ?= docker compose -f deploy/docker-compose.yml
REDIS_ADDR   ?= localhost:6379
BENCH_RATE   ?= 10000
BENCH_TIME   ?= 60s
BENCH_SIZE   ?= 256
GO           ?= go

.PHONY: help up down bench test unit build vet fmt proto stats drain logs clean

help: ## List targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-10s %s\n", $$1, $$2}'

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

build: ## Build all three binaries into ./bin
	$(GO) build -trimpath -o bin/ ./cmd/...

vet: ## go vet
	$(GO) vet ./...

fmt: ## gofmt the tree
	gofmt -w $(shell find . -name '*.go' -not -name '*.pb.go')

proto: ## Regenerate gRPC stubs (requires buf, protoc-gen-go, protoc-gen-go-grpc)
	buf generate

stats: ## Print cluster stats via the coordinator (requires grpcurl)
	grpcurl -plaintext localhost:9090 engine.v1.Engine/GetClusterStats

drain: ## Ask one node to drain: make drain NODE=<node-id>
	grpcurl -plaintext -d '{"node_id":"$(NODE)"}' localhost:9090 engine.v1.Engine/DrainNode

logs: ## Tail worker logs
	$(COMPOSE) logs -f worker

clean: ## Remove build output
	rm -rf bin
