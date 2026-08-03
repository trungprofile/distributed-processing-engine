// Command producer is the load generator used by `make bench`. It appends
// records to the stream at a configurable rate and reports what it actually
// achieved, which is the input side of the throughput numbers in the README.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/trungprofile/distributed-processing-engine/internal/stream"
)

// tickHz is how finely the target rate is sliced. Higher values smooth the
// arrival pattern; lower values reduce pipeline overhead per record.
const tickHz = 20

func main() {
	var (
		rate      = flag.Int("rate", 5000, "target records per second")
		duration  = flag.Duration("duration", 30*time.Second, "how long to generate load; 0 runs until interrupted")
		size      = flag.Int("size", 256, "payload size in bytes")
		redisAddr = flag.String("redis", envOr("REDIS_ADDR", "localhost:6379"), "Redis address")
		streamKey = flag.String("stream", "dpe:records", "Redis stream key")
		maxLen    = flag.Int64("maxlen", 1_000_000, "approximate stream cap (XADD MAXLEN ~); 0 disables trimming")
		runID     = flag.String("run", "", "run ID prefixed to every idempotency key; random when empty")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if *rate <= 0 {
		log.Error("rate must be positive")
		os.Exit(1)
	}
	if *runID == "" {
		*runID = randomID()
	}

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr, PoolSize: 16})
	defer func() { _ = rdb.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("redis unreachable", "addr", *redisAddr, "error", err)
		os.Exit(1)
	}

	pub := stream.NewConsumer(rdb, stream.Config{
		Stream:   *streamKey,
		Group:    "dpe-workers",
		Consumer: "producer-" + *runID,
		MaxLen:   *maxLen,
	}, nil)
	if err := pub.EnsureGroup(ctx); err != nil {
		log.Error("ensure group", "error", err)
		os.Exit(1)
	}

	perTick := *rate / tickHz
	if perTick < 1 {
		perTick = 1
	}
	payload := make([]byte, *size)
	if _, err := rand.Read(payload); err != nil {
		log.Error("payload init", "error", err)
		os.Exit(1)
	}

	log.Info("producing", "rate", *rate, "payload_bytes", *size, "stream", *streamKey, "run", *runID)

	ticker := time.NewTicker(time.Second / tickHz)
	defer ticker.Stop()

	batch := make([]stream.Record, perTick)
	start := time.Now()
	var sent int64
	var behind int

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			for i := range batch {
				batch[i] = stream.Record{
					Key:     fmt.Sprintf("%s-%d", *runID, sent+int64(i)),
					Payload: payload,
				}
			}
			tickStart := time.Now()
			if err := pub.Publish(ctx, batch...); err != nil {
				if ctx.Err() != nil {
					break loop
				}
				log.Warn("publish failed", "error", err)
				continue
			}
			sent += int64(len(batch))
			// A tick that takes longer than its budget means Redis, not the
			// generator, is the bottleneck. Report it rather than silently
			// producing below target.
			if time.Since(tickStart) > time.Second/tickHz {
				behind++
			}
		}
	}

	elapsed := time.Since(start)
	achieved := float64(sent) / elapsed.Seconds()
	fmt.Printf("\nsent            %d records\n", sent)
	fmt.Printf("elapsed         %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("achieved rate   %.0f records/sec (target %d)\n", achieved, *rate)
	fmt.Printf("slow ticks      %d\n", behind)
	fmt.Printf("key range       %s-0 .. %s-%d\n", *runID, *runID, sent-1)
}

func randomID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("run%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
