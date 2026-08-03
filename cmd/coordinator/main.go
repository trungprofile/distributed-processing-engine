// Command coordinator runs the gRPC control plane: batch submission, cluster
// stats and drain requests. It holds no record state — Redis does — so it can
// be restarted at any time without affecting in-flight work.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/trungprofile/distributed-processing-engine/internal/cluster"
	grpcserver "github.com/trungprofile/distributed-processing-engine/internal/grpc"
	"github.com/trungprofile/distributed-processing-engine/internal/metrics"
	"github.com/trungprofile/distributed-processing-engine/internal/stream"
)

func main() {
	var (
		listen      = flag.String("listen", ":9090", "gRPC listen address")
		redisAddr   = flag.String("redis", envOr("REDIS_ADDR", "localhost:6379"), "Redis address")
		streamKey   = flag.String("stream", "dpe:records", "Redis stream key")
		group       = flag.String("group", "dpe-workers", "consumer group name")
		prefix      = flag.String("prefix", "dpe", "Redis key namespace")
		metricsAddr = flag.String("metrics-addr", ":9101", "address for the Prometheus /metrics endpoint")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr, PoolSize: 32})
	defer func() { _ = rdb.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("redis unreachable", "addr", *redisAddr, "error", err)
		os.Exit(1)
	}

	m := metrics.New("coordinator")
	pub := stream.NewConsumer(rdb, stream.Config{
		Stream:   *streamKey,
		Group:    *group,
		Consumer: "coordinator",
	}, m)
	if err := pub.EnsureGroup(ctx); err != nil {
		log.Error("ensure group", "error", err)
		os.Exit(1)
	}

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", m.Handler())
		srv := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server stopped", "error", err)
		}
	}()

	srv := grpcserver.New(pub, cluster.NewRegistry(rdb, *prefix))
	log.Info("coordinator listening", "grpc", *listen, "stream", *streamKey, "group", *group)

	if err := srv.Serve(ctx, *listen); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("coordinator stopped", "error", err)
		os.Exit(1)
	}
	log.Info("coordinator stopped cleanly")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
