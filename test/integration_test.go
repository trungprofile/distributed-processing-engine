// Package test holds the end-to-end guarantee tests. They need a live Redis
// (REDIS_ADDR, default localhost:6379) and skip when one is not reachable, so
// `go test ./...` stays green on a laptop and CI runs them against the service
// container.
package test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/trungprofile/distributed-processing-engine/internal/store"
	"github.com/trungprofile/distributed-processing-engine/internal/stream"
	"github.com/trungprofile/distributed-processing-engine/internal/worker"
)

const (
	totalRecords = 4000
	nodeCount    = 4
	minIdle      = 750 * time.Millisecond
)

// TestNodeFailureLosesNoRecords is the guarantee the engine exists to provide.
//
// It starts a cluster, feeds it a known key set, kills a node while it is
// holding uncommitted work (hard stop: no drain, no acks, exactly what a
// SIGKILL or a lost EC2 instance looks like), and asserts that the surviving
// nodes reclaim the orphaned entries and that the sink ends up with every key
// exactly once.
func TestNodeFailureLosesNoRecords(t *testing.T) {
	rdb := dialRedis(t)
	ns := fmt.Sprintf("dpetest:%d", time.Now().UnixNano())
	t.Cleanup(func() { cleanup(t, rdb, ns) })

	sinkKey := ns + ":results"
	sink := store.NewRedisSink(rdb, sinkKey)

	pub := stream.NewConsumer(rdb, stream.Config{
		Stream:   ns + ":records",
		Group:    ns + ":group",
		Consumer: "test-producer",
	}, nil)
	if err := pub.EnsureGroup(context.Background()); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	// Start the cluster. Each node gets its own Redis client so killing one
	// can sever its connections the way losing a process would.
	ctxs := make([]context.CancelFunc, nodeCount)
	clients := make([]redis.UniversalClient, nodeCount)
	nodes := make([]*worker.Node, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeCtx, cancel := context.WithCancel(context.Background())
		ctxs[i] = cancel
		clients[i] = dialRedis(t)
		var sink store.Sink = store.NewRedisSink(clients[i], sinkKey)
		if i == 0 {
			// node-0 is the victim. A slow sink guarantees it is holding
			// uncommitted records at the moment it dies, which is the case
			// the exactly-once path has to survive.
			sink = slowSink{inner: sink, delay: 300 * time.Millisecond}
		}
		nodes[i] = newNode(t, clients[i], ns, fmt.Sprintf("node-%d", i), sink)
		go func(n *worker.Node, ctx context.Context) { _ = n.Run(ctx) }(nodes[i], nodeCtx)
	}
	t.Cleanup(func() {
		for _, cancel := range ctxs {
			cancel()
		}
	})

	// Feed the cluster a known key set.
	keys := make(map[string]bool, totalRecords)
	batch := make([]stream.Record, 0, 500)
	for i := 0; i < totalRecords; i++ {
		key := fmt.Sprintf("rec-%05d", i)
		keys[key] = true
		batch = append(batch, stream.Record{Key: key, Payload: []byte(key)})
		if len(batch) == cap(batch) {
			if err := pub.Publish(context.Background(), batch...); err != nil {
				t.Fatalf("publish: %v", err)
			}
			batch = batch[:0]
		}
	}
	if err := pub.Publish(context.Background(), batch...); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Wait until the cluster is genuinely mid-flight before killing a node,
	// otherwise the kill proves nothing.
	waitFor(t, 10*time.Second, func() bool {
		n, err := sink.Count(context.Background())
		return err == nil && n > totalRecords/10
	}, "cluster to start processing")

	// Kill node-0 the way an instance loss looks: no drain, no acks, and its
	// connections severed. Anything it had checked out is now orphaned in the
	// pending entries list with no consumer left to ack it.
	inFlightAtKill := nodes[0].Processed()
	killedAt := time.Now()
	_ = clients[0].Close() // sever its connections first, as a dead process would
	nodes[0].Kill()        // no drain: abandon whatever it holds
	ctxs[0]()              // stop its loops

	t.Logf("killed node-0 (had committed %d records); sink held %d of %d",
		inFlightAtKill, count(t, sink), totalRecords)

	// Survivors must reclaim the orphaned entries once the lease expires.
	deadline := 60 * time.Second
	waitFor(t, deadline, func() bool {
		return count(t, sink) == totalRecords
	}, "all records to reach the sink")

	recovery := time.Since(killedAt)
	t.Logf("recovered to full completeness %s after the kill", recovery.Round(time.Millisecond))

	// Zero loss: every key produced is present exactly once. A Redis hash
	// cannot hold a key twice, so completeness plus the duplicate counter is
	// the exactly-once assertion.
	stored, err := rdb.HKeys(context.Background(), sinkKey).Result()
	if err != nil {
		t.Fatalf("read sink: %v", err)
	}
	if len(stored) != totalRecords {
		t.Fatalf("record loss: sink holds %d of %d records", len(stored), totalRecords)
	}
	for _, k := range stored {
		if !keys[k] {
			t.Fatalf("sink holds unexpected key %q", k)
		}
		delete(keys, k)
	}
	if len(keys) != 0 {
		t.Fatalf("record loss: %d keys never reached the sink", len(keys))
	}

	// Nothing may be left in flight: every entry is acked or dead-lettered.
	waitFor(t, 15*time.Second, func() bool {
		st, err := pub.Stats(context.Background())
		return err == nil && st.Pending == 0
	}, "pending entries list to drain")

	var reclaimed int64
	for i := 1; i < nodeCount; i++ {
		reclaimed += nodes[i].Reclaimed()
	}
	if reclaimed == 0 {
		t.Error("expected survivors to reclaim the dead node's entries, got 0 reclaims")
	}
	t.Logf("survivors reclaimed %d entries from the dead node", reclaimed)
}

// TestDrainCommitsInFlightWork asserts the SIGTERM path: a drained node leaves
// nothing pending, which is what makes a rolling deploy free of duplicate work.
func TestDrainCommitsInFlightWork(t *testing.T) {
	rdb := dialRedis(t)
	ns := fmt.Sprintf("dpedrain:%d", time.Now().UnixNano())
	t.Cleanup(func() { cleanup(t, rdb, ns) })

	sink := store.NewRedisSink(rdb, ns+":results")
	pub := stream.NewConsumer(rdb, stream.Config{
		Stream: ns + ":records", Group: ns + ":group", Consumer: "test-producer",
	}, nil)
	if err := pub.EnsureGroup(context.Background()); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	const n = 500
	recs := make([]stream.Record, 0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("drain-%04d", i)
		recs = append(recs, stream.Record{Key: key, Payload: []byte(key)})
	}
	if err := pub.Publish(context.Background(), recs...); err != nil {
		t.Fatalf("publish: %v", err)
	}

	node := newNode(t, rdb, ns, "drain-node", sink)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- node.Run(ctx) }()

	waitFor(t, 10*time.Second, func() bool { return count(t, sink) > 0 }, "processing to start")
	cancel() // SIGTERM equivalent

	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("drain did not finish cleanly: %v", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("node did not drain within 45s")
	}

	st, err := pub.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.Pending != 0 {
		t.Errorf("drained node left %d entries pending; drain must ack everything it owns", st.Pending)
	}
}

// slowSink stretches the window in which a record is claimed but not yet
// committed, so a kill lands squarely inside it.
type slowSink struct {
	inner store.Sink
	delay time.Duration
}

func (s slowSink) Write(ctx context.Context, r stream.Record) error {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.inner.Write(ctx, r)
}

func newNode(t *testing.T, rdb redis.UniversalClient, ns, id string, sink store.Sink) *worker.Node {
	t.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	node, err := worker.NewNode(rdb, worker.NodeConfig{
		NodeID:        id,
		Concurrency:   16,
		Stream:        ns + ":records",
		Group:         ns + ":group",
		Prefix:        ns,
		Sink:          ns + ":results",
		Batch:         32,
		Block:         200 * time.Millisecond,
		MinIdle:       minIdle,
		ReclaimEvery:  250 * time.Millisecond,
		HeartbeatBeat: 500 * time.Millisecond,
		ClaimTTL:      minIdle,
		CommitTTL:     10 * time.Minute,
		DrainTimeout:  30 * time.Second,
	}, sink, log)
	if err != nil {
		t.Fatalf("new node %s: %v", id, err)
	}
	return node
}

func dialRedis(t *testing.T) redis.UniversalClient {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 64})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("redis unavailable at %s (%v); run `make up` first", addr, err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func count(t *testing.T, sink *store.RedisSink) int64 {
	t.Helper()
	n, err := sink.Count(context.Background())
	if err != nil {
		t.Fatalf("sink count: %v", err)
	}
	return n
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

func cleanup(t *testing.T, rdb redis.UniversalClient, ns string) {
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
