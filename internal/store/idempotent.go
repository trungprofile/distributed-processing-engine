// Package store holds the exactly-once write path.
//
// The stream layer gives at-least-once delivery: a record can be handed to a
// second node after the first one dies, and both may run the handler. What
// makes the *effect* exactly-once is the idempotency key, claimed with SETNX
// before the sink write and flipped to a terminal marker in the same Redis
// round trip as the write itself.
//
// Ordering is fixed and matters:
//
//	claim (SETNX) -> sink write -> mark done -> XACK
//
// Acking last is what makes a crash safe: any failure before the ack leaves
// the entry in the pending list for the reclaimer, and any redelivery finds
// the key already marked and skips the write.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/trungprofile/distributed-processing-engine/internal/metrics"
	"github.com/trungprofile/distributed-processing-engine/internal/stream"
)

// Outcome reports what Apply did with a record.
type Outcome int

const (
	// Applied means this call ran the sink write and committed it.
	Applied Outcome = iota
	// Duplicate means the key was already committed by some node; the record
	// is safe to ack without touching the sink.
	Duplicate
	// Contended means another node holds an unexpired claim. The record is
	// left unacked on purpose so the reclaimer retries it later.
	Contended
)

func (o Outcome) String() string {
	switch o {
	case Applied:
		return "applied"
	case Duplicate:
		return "duplicate"
	default:
		return "contended"
	}
}

// ErrLeaseLost is returned when the claim expired while the handler was still
// running and another node took the key over. The sink write is not rolled
// back: sinks are keyed by idempotency key, so the competing write is the same
// write.
var ErrLeaseLost = errors.New("store: idempotency claim lost during write")

// Sink is the terminal destination for a processed record. Implementations
// must be keyed by Record.Key so a retried write is a no-op overwrite rather
// than a second effect.
type Sink interface {
	Write(ctx context.Context, r stream.Record) error
}

// Config tunes key lifetimes.
type Config struct {
	// Prefix namespaces idempotency keys.
	Prefix string
	// ClaimTTL bounds how long a crashed writer can block a key. Keep it close
	// to the reclaim MinIdle: shorter and a slow handler loses its claim,
	// longer and a redelivered record spins in Contended until it expires.
	ClaimTTL time.Duration
	// CommitTTL is how long a committed marker is remembered, i.e. the window
	// in which a duplicate can still be recognised. It must exceed the longest
	// plausible redelivery delay.
	CommitTTL time.Duration
}

func (c *Config) withDefaults() {
	if c.Prefix == "" {
		c.Prefix = "dpe:idem:"
	}
	if c.ClaimTTL <= 0 {
		c.ClaimTTL = 30 * time.Second
	}
	if c.CommitTTL <= 0 {
		c.CommitTTL = 24 * time.Hour
	}
}

// claimScript takes the key if it is free, and is a no-op otherwise. Returns
// "claimed", "duplicate" or "contended".
var claimScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if not cur then
  redis.call('SET', KEYS[1], 'claim:' .. ARGV[1], 'PX', ARGV[2])
  return 'claimed'
end
if cur == 'done' then return 'duplicate' end
if cur == 'claim:' .. ARGV[1] then return 'claimed' end
return 'contended'
`)

// commitScript flips the claim to the terminal marker, but only if this owner
// still holds it. Returns 1 committed, 0 already committed, -1 lease lost.
var commitScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if cur == 'done' then return 0 end
if cur ~= 'claim:' .. ARGV[1] then return -1 end
redis.call('SET', KEYS[1], 'done', 'PX', ARGV[2])
return 1
`)

// Store is the idempotent write path in front of a Sink.
type Store struct {
	rdb   redis.UniversalClient
	sink  Sink
	cfg   Config
	m     *metrics.Metrics
	owner string
}

// New builds a Store owned by nodeID. The owner token is what makes a claim
// stealable only after it expires, never while it is live.
func New(rdb redis.UniversalClient, sink Sink, cfg Config, nodeID string, m *metrics.Metrics) *Store {
	cfg.withDefaults()
	return &Store{rdb: rdb, sink: sink, cfg: cfg, m: m, owner: nodeID}
}

// Apply runs the record through the exactly-once path. The caller acks the
// stream entry if and only if the outcome is Applied or Duplicate.
func (s *Store) Apply(ctx context.Context, r stream.Record) (Outcome, error) {
	key := s.cfg.Prefix + r.Key

	res, err := claimScript.Run(ctx, s.rdb, []string{key},
		s.owner, s.cfg.ClaimTTL.Milliseconds()).Text()
	if err != nil {
		return Contended, fmt.Errorf("claim %s: %w", r.Key, err)
	}

	switch res {
	case "duplicate":
		if s.m != nil {
			s.m.RecordsDuplicate.Inc()
			s.m.RecordsProcessed.WithLabelValues("duplicate").Inc()
		}
		return Duplicate, nil
	case "contended":
		return Contended, nil
	}

	start := time.Now()
	if err := s.sink.Write(ctx, r); err != nil {
		// Release the claim so the redelivery is not stuck behind our own
		// expired lease; the entry is still unacked, so it will come back.
		s.release(ctx, key)
		if s.m != nil {
			s.m.RecordsFailed.WithLabelValues("sink").Inc()
		}
		return Contended, fmt.Errorf("sink write %s: %w", r.Key, err)
	}
	if s.m != nil {
		s.m.ObserveProcessing(time.Since(start))
	}

	committed, err := commitScript.Run(ctx, s.rdb, []string{key},
		s.owner, s.cfg.CommitTTL.Milliseconds()).Int()
	if err != nil {
		return Contended, fmt.Errorf("commit %s: %w", r.Key, err)
	}
	switch committed {
	case -1:
		return Contended, ErrLeaseLost
	case 0:
		if s.m != nil {
			s.m.RecordsDuplicate.Inc()
		}
		return Duplicate, nil
	}
	if s.m != nil {
		s.m.RecordsProcessed.WithLabelValues("applied").Inc()
	}
	return Applied, nil
}

// release drops a claim we own. Best effort: if it fails the TTL cleans up.
func (s *Store) release(ctx context.Context, key string) {
	const script = `if redis.call('GET', KEYS[1]) == 'claim:' .. ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0`
	_ = redis.NewScript(script).Run(ctx, s.rdb, []string{key}, s.owner).Err()
}

// RedisSink writes processed records into a Redis hash keyed by idempotency
// key. It stands in for a real sink (warehouse, object store) and keeps the
// integration test's "did every record land exactly once?" assertion cheap.
type RedisSink struct {
	rdb redis.UniversalClient
	key string
}

// NewRedisSink returns a sink writing to the given hash.
func NewRedisSink(rdb redis.UniversalClient, hashKey string) *RedisSink {
	if hashKey == "" {
		hashKey = "dpe:results"
	}
	return &RedisSink{rdb: rdb, key: hashKey}
}

// Write stores the record body under its idempotency key.
func (s *RedisSink) Write(ctx context.Context, r stream.Record) error {
	return s.rdb.HSet(ctx, s.key, r.Key, r.Payload).Err()
}

// Count returns the number of distinct records in the sink.
func (s *RedisSink) Count(ctx context.Context) (int64, error) {
	return s.rdb.HLen(ctx, s.key).Result()
}
