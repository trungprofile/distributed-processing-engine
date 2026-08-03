// Command worker runs one processing node: it joins the consumer group,
// executes records with a bounded pool, reclaims work from dead peers, and
// drains gracefully on SIGTERM.
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

	"github.com/trungprofile/distributed-processing-engine/internal/worker"
)

func main() {
	var (
		nodeID      = flag.String("node", defaultNodeID(), "unique node ID; also the consumer name in the group")
		concurrency = flag.Int("concurrency", 64, "bounded in-flight window (max records processed at once)")
		redisAddr   = flag.String("redis", envOr("REDIS_ADDR", "localhost:6379"), "Redis address")
		streamKey   = flag.String("stream", "dpe:records", "Redis stream key")
		group       = flag.String("group", "dpe-workers", "consumer group name")
		batch       = flag.Int64("batch", 128, "max entries per XREADGROUP")
		minIdle     = flag.Duration("min-idle", 5*time.Second, "lease length: idle time after which another node may reclaim a message")
		drainAfter  = flag.Duration("drain-timeout", 30*time.Second, "max time to finish in-flight work on SIGTERM")
		metricsAddr = flag.String("metrics-addr", ":9100", "address for the Prometheus /metrics endpoint")
		maxDeliver  = flag.Int64("max-deliveries", 8, "dead-letter a record after this many deliveries; 0 disables")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	rdb := redis.NewClient(&redis.Options{
		Addr:         *redisAddr,
		PoolSize:     *concurrency + 8,
		MinIdleConns: 4,
		ReadTimeout:  30 * time.Second,
	})
	defer func() { _ = rdb.Close() }()

	// SIGTERM cancels this context, which stops intake and starts the drain.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("redis unreachable", "addr", *redisAddr, "error", err)
		os.Exit(1)
	}

	node, err := worker.NewNode(rdb, worker.NodeConfig{
		NodeID:        *nodeID,
		Concurrency:   *concurrency,
		Stream:        *streamKey,
		Group:         *group,
		Batch:         *batch,
		MinIdle:       *minIdle,
		MaxDeliveries: *maxDeliver,
		DrainTimeout:  *drainAfter,
	}, nil, log)
	if err != nil {
		log.Error("node setup failed", "error", err)
		os.Exit(1)
	}

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", node.Metrics().Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		srv := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server stopped", "error", err)
		}
	}()

	log.Info("worker starting",
		"node", *nodeID, "concurrency", *concurrency,
		"stream", *streamKey, "group", *group, "min_idle", minIdle.String())

	if err := node.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("worker exited with pending work", "error", err)
		os.Exit(1)
	}
	log.Info("worker stopped cleanly", "processed", node.Processed(), "reclaimed", node.Reclaimed())
}

func defaultNodeID() string {
	if v := os.Getenv("NODE_ID"); v != "" {
		return v
	}
	host, err := os.Hostname()
	if err != nil {
		return "worker-unknown"
	}
	return host
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
