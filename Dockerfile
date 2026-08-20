# Build once, ship four static binaries; the entrypoint selects which one.
FROM golang:1.24-alpine AS build

WORKDIR /src

# Dependencies first so the module cache survives source edits.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker      ./cmd/worker && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/producer    ./cmd/producer && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/coordinator ./cmd/coordinator && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/operator    ./cmd/operator

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/worker      /usr/local/bin/worker
COPY --from=build /out/producer    /usr/local/bin/producer
COPY --from=build /out/coordinator /usr/local/bin/coordinator
# The operator Deployment overrides `command` to select this binary; the
# image's ENTRYPOINT stays the worker, which is what the StatefulSet runs.
COPY --from=build /out/operator    /usr/local/bin/operator

USER nonroot:nonroot
# coordinator gRPC, worker metrics, operator metrics and probes.
EXPOSE 9090 9100 8080 8081

# Workers must receive SIGTERM directly for the graceful drain to run, so the
# binary is PID 1 with no shell wrapper in between.
ENTRYPOINT ["/usr/local/bin/worker"]
