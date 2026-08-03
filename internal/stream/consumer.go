// Package stream wraps the Redis Streams consumer-group protocol used to move
// records between the producer and the worker pool.
//
// Delivery is at-least-once: an entry stays in the group's pending entries
// list (PEL) until XACK, so a node that dies mid-flight leaves its work
// visible to the reclaimer. Exactly-once is recovered at the sink by
// internal/store.
package stream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/trungprofile/distributed-processing-engine/internal/metrics"
)

const (
	fieldKey     = "k"
	fieldPayload = "p"
)

// Record is one unit of work as it travels through the engine.
type Record struct {
	// EntryID is the Redis stream ID (e.g. "1700000000000-0"). It is the
	// handle used for XACK and is assigned by Redis on XADD.
	EntryID string
	// Key is the caller-supplied idempotency key. Two records with the same
	// key are the same record and must be applied to the sink once.
	Key string
	// Payload is opaque to the engine.
	Payload []byte
	// Idle is how long the entry sat unacked before delivery. Non-zero only
	// for reclaimed records.
	Idle time.Duration
	// Redelivered is true when the record arrived via XAUTOCLAIM rather than
	// a fresh XREADGROUP.
	Redelivered bool
}

// Config describes one consumer's view of a stream.
type Config struct {
	Stream   string
	Group    string
	Consumer string
	// Batch is the maximum number of entries returned by one XREADGROUP.
	Batch int64
	// Block is how long XREADGROUP parks waiting for new entries.
	Block time.Duration
	// MaxLen caps the stream with XADD MAXLEN ~ so a stalled cluster cannot
	// exhaust Redis memory. Zero disables trimming.
	MaxLen int64
}

func (c *Config) withDefaults() {
	if c.Batch <= 0 {
		c.Batch = 128
	}
	if c.Block <= 0 {
		c.Block = 2 * time.Second
	}
}

// Consumer is a single member of a Redis Streams consumer group.
type Consumer struct {
	rdb redis.UniversalClient
	cfg Config
	m   *metrics.Metrics
}

// NewConsumer binds a consumer to a stream. EnsureGroup must be called once
// before reading.
func NewConsumer(rdb redis.UniversalClient, cfg Config, m *metrics.Metrics) *Consumer {
	cfg.withDefaults()
	return &Consumer{rdb: rdb, cfg: cfg, m: m}
}

// Name returns the consumer name registered with the group.
func (c *Consumer) Name() string { return c.cfg.Consumer }

// Stream returns the stream key this consumer is bound to.
func (c *Consumer) Stream() string { return c.cfg.Stream }

// EnsureGroup creates the stream and the consumer group if they do not exist.
// It is safe to call from every node concurrently on startup.
func (c *Consumer) EnsureGroup(ctx context.Context) error {
	err := c.rdb.XGroupCreateMkStream(ctx, c.cfg.Stream, c.cfg.Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create group %s: %w", c.cfg.Group, err)
	}
	return nil
}

// Publish appends records to the stream in one pipeline round trip. The
// producer and the coordinator's SubmitBatch RPC share this path.
func (c *Consumer) Publish(ctx context.Context, records ...Record) error {
	if len(records) == 0 {
		return nil
	}
	pipe := c.rdb.Pipeline()
	for _, r := range records {
		args := &redis.XAddArgs{
			Stream: c.cfg.Stream,
			Values: map[string]any{fieldKey: r.Key, fieldPayload: r.Payload},
		}
		if c.cfg.MaxLen > 0 {
			args.MaxLen = c.cfg.MaxLen
			args.Approx = true
		}
		pipe.XAdd(ctx, args)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("xadd batch of %d: %w", len(records), err)
	}
	if c.m != nil {
		c.m.RecordsPublished.Add(float64(len(records)))
	}
	return nil
}

// Read pulls the next batch of undelivered entries. It blocks for at most
// cfg.Block and returns an empty slice on timeout, which the caller should
// treat as "idle", not as an error.
func (c *Consumer) Read(ctx context.Context, count int64) ([]Record, error) {
	if count <= 0 || count > c.cfg.Batch {
		count = c.cfg.Batch
	}
	streams, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.cfg.Group,
		Consumer: c.cfg.Consumer,
		Streams:  []string{c.cfg.Stream, ">"},
		Count:    count,
		Block:    c.cfg.Block,
	}).Result()
	switch {
	case errors.Is(err, redis.Nil):
		return nil, nil // block timeout, nothing new
	case err != nil:
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("xreadgroup: %w", err)
	}

	var out []Record
	for _, s := range streams {
		for _, msg := range s.Messages {
			out = append(out, toRecord(msg, 0))
		}
	}
	return out, nil
}

// Ack removes entries from the pending entries list. It is called only after
// the sink write has committed.
func (c *Consumer) Ack(ctx context.Context, entryIDs ...string) error {
	if len(entryIDs) == 0 {
		return nil
	}
	if err := c.rdb.XAck(ctx, c.cfg.Stream, c.cfg.Group, entryIDs...).Err(); err != nil {
		return fmt.Errorf("xack %d entries: %w", len(entryIDs), err)
	}
	if c.m != nil {
		c.m.MessagesAcked.Add(float64(len(entryIDs)))
	}
	return nil
}

// Stats is the coordinator's view of a consumer group.
type Stats struct {
	Lag      int64
	Pending  int64
	Length   int64
	Consumer int64
}

// Stats reads group depth via XINFO GROUPS / XLEN. Used by GetClusterStats and
// by the lag gauge.
func (c *Consumer) Stats(ctx context.Context) (Stats, error) {
	var st Stats
	length, err := c.rdb.XLen(ctx, c.cfg.Stream).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return st, fmt.Errorf("xlen: %w", err)
	}
	st.Length = length

	groups, err := c.rdb.XInfoGroups(ctx, c.cfg.Stream).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return st, nil
		}
		return st, fmt.Errorf("xinfo groups: %w", err)
	}
	for _, g := range groups {
		if g.Name != c.cfg.Group {
			continue
		}
		st.Lag = g.Lag
		st.Pending = g.Pending
		st.Consumer = g.Consumers
	}
	if c.m != nil {
		c.m.StreamLag.Set(float64(st.Lag))
		c.m.PendingSize.Set(float64(st.Pending))
	}
	return st, nil
}

func toRecord(msg redis.XMessage, idle time.Duration) Record {
	r := Record{EntryID: msg.ID, Idle: idle, Redelivered: idle > 0}
	if v, ok := msg.Values[fieldKey].(string); ok {
		r.Key = v
	}
	if v, ok := msg.Values[fieldPayload].(string); ok {
		r.Payload = []byte(v)
	}
	return r
}
