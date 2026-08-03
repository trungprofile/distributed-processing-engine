# Build once, ship three static binaries; the entrypoint selects which one.
FROM golang:1.22-alpine AS build

WORKDIR /src

# Dependencies first so the module cache survives source edits.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker      ./cmd/worker && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/producer    ./cmd/producer && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/coordinator ./cmd/coordinator

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/worker      /usr/local/bin/worker
COPY --from=build /out/producer    /usr/local/bin/producer
COPY --from=build /out/coordinator /usr/local/bin/coordinator

USER nonroot:nonroot
EXPOSE 9090 9100

# Workers must receive SIGTERM directly for the graceful drain to run, so the
# binary is PID 1 with no shell wrapper in between.
ENTRYPOINT ["/usr/local/bin/worker"]
