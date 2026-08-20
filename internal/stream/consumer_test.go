package stream

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// dialRedis matches the convention in test/integration_test.go: skip rather
// than fail when no Redis is reachable, so `go test ./...` stays green on a
// laptop while CI runs these against its service container.
func dialRedis(t *testing.T) redis.UniversalClient {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("redis unavailable at %s (%v); run `make up` first", addr, err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func newTestConsumer(t *testing.T, rdb redis.UniversalClient, ns, name string) *Consumer {
	t.Helper()
	c := NewConsumer(rdb, Config{
		Stream:   ns + ":records",
		Group:    ns + ":group",
		Consumer: name,
		Batch:    32,
		Block:    100 * time.Millisecond,
	}, nil)
	if err := c.EnsureGroup(context.Background()); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	return c
}

func consumerNames(t *testing.T, rdb redis.UniversalClient, c *Consumer) []string {
	t.Helper()
	infos, err := rdb.XInfoConsumers(context.Background(), c.cfg.Stream, c.cfg.Group).Result()
	if err != nil {
		t.Fatalf("xinfo consumers: %v", err)
	}
	out := make([]string, 0, len(infos))
	for _, i := range infos {
		out = append(out, i.Name)
	}
	return out
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// TestRemoveConsumerDropsADrainedConsumer is the tidy-up path: a worker that
// acked everything it owned should not linger in XINFO CONSUMERS forever.
func TestRemoveConsumerDropsADrainedConsumer(t *testing.T) {
	rdb := dialRedis(t)
	ns := fmt.Sprintf("dpestream:%d", time.Now().UnixNano())
	t.Cleanup(func() { cleanupNS(t, rdb, ns) })

	ctx := context.Background()
	c := newTestConsumer(t, rdb, ns, "worker-0")

	if err := c.Publish(ctx, Record{Key: "a", Payload: []byte("a")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	recs, err := c.Read(ctx, 10)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("read %d records, want 1", len(recs))
	}
	if !contains(consumerNames(t, rdb, c), "worker-0") {
		t.Fatal("consumer did not register with the group after reading")
	}

	// A clean drain acks everything before removing the consumer.
	if err := c.Ack(ctx, recs[0].EntryID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	pending, err := c.RemoveConsumer(ctx)
	if err != nil {
		t.Fatalf("remove consumer: %v", err)
	}
	if pending != 0 {
		t.Fatalf("remove reported %d pending entries after a full ack, want 0", pending)
	}
	if names := consumerNames(t, rdb, c); contains(names, "worker-0") {
		t.Fatalf("drained consumer is still in the group: %v", names)
	}
}

// TestRemoveConsumerRefusesToDropPendingWork is the safety property. Redis
// discards a deleted consumer's pending entries outright, so removing a
// consumer that still owns work would lose records rather than tidy up.
func TestRemoveConsumerRefusesToDropPendingWork(t *testing.T) {
	rdb := dialRedis(t)
	ns := fmt.Sprintf("dpestream:%d", time.Now().UnixNano())
	t.Cleanup(func() { cleanupNS(t, rdb, ns) })

	ctx := context.Background()
	c := newTestConsumer(t, rdb, ns, "worker-0")

	for i := 0; i < 3; i++ {
		if err := c.Publish(ctx, Record{Key: fmt.Sprintf("k-%d", i)}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	recs, err := c.Read(ctx, 10)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("read %d records, want 3", len(recs))
	}

	// Ack only one: two entries are still checked out.
	if err := c.Ack(ctx, recs[0].EntryID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	pending, err := c.RemoveConsumer(ctx)
	if err != nil {
		t.Fatalf("remove consumer: %v", err)
	}
	if pending != 2 {
		t.Fatalf("remove reported %d pending entries, want 2", pending)
	}
	if names := consumerNames(t, rdb, c); !contains(names, "worker-0") {
		t.Fatalf("consumer holding pending work was deleted: %v", names)
	}

	// The entries must still be there to reclaim.
	st, err := c.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.Pending != 2 {
		t.Fatalf("group has %d pending entries, want the 2 that were never acked", st.Pending)
	}
}

// TestRemoveConsumerIsIdempotent covers the repeated-drain case: a second call
// on an already-removed consumer must not error.
func TestRemoveConsumerIsIdempotent(t *testing.T) {
	rdb := dialRedis(t)
	ns := fmt.Sprintf("dpestream:%d", time.Now().UnixNano())
	t.Cleanup(func() { cleanupNS(t, rdb, ns) })

	ctx := context.Background()
	c := newTestConsumer(t, rdb, ns, "worker-0")

	for i := 0; i < 2; i++ {
		if pending, err := c.RemoveConsumer(ctx); err != nil || pending != 0 {
			t.Fatalf("call %d: pending=%d err=%v, want 0 and no error", i, pending, err)
		}
	}
}

func cleanupNS(t *testing.T, rdb redis.UniversalClient, ns string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	iter := rdb.Scan(ctx, 0, ns+"*", 500).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if len(keys) > 0 {
		_ = rdb.Del(ctx, keys...).Err()
	}
}
