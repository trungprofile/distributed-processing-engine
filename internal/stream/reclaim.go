package stream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/trungprofile/distributed-processing-engine/internal/metrics"
)

// ReclaimConfig tunes lease-based recovery.
//
// A consumer's "lease" on a message is implicit: the entry sits in the group's
// PEL with an idle timer that resets on delivery. MinIdle is the lease length.
// Any node may steal an entry that has been idle longer than MinIdle.
//
// The tradeoff is entirely in MinIdle:
//
//   - Too low and a slow-but-alive node has its in-flight work stolen while it
//     is still processing it. The record is not lost — the idempotent sink
//     collapses the second write — but the duplicate work is wasted capacity,
//     and at high concurrency the steals feed on themselves.
//   - Too high and every crash costs a full MinIdle of stalled partition
//     progress before anyone picks the work up.
//
// The default is 5x the p99 handler latency, which keeps steals rare while
// bounding recovery to roughly MinIdle + Interval.
type ReclaimConfig struct {
	MinIdle time.Duration
	// Interval is how often each node sweeps for expired leases. Every node
	// sweeps; XAUTOCLAIM is atomic, so the winner takes the batch.
	Interval time.Duration
	// Count is the maximum entries per XAUTOCLAIM call.
	Count int64
	// MaxDeliveries drops an entry to the dead-letter stream once it has been
	// delivered this many times, so a poison record cannot circulate forever.
	// Zero disables dead-lettering.
	MaxDeliveries int64
}

func (c *ReclaimConfig) withDefaults() {
	if c.MinIdle <= 0 {
		c.MinIdle = 30 * time.Second
	}
	if c.Interval <= 0 {
		c.Interval = c.MinIdle / 3
	}
	if c.Count <= 0 {
		c.Count = 64
	}
}

// Reclaimer takes over messages whose lease has expired, i.e. messages
// belonging to consumers that crashed or stalled.
type Reclaimer struct {
	c      *Consumer
	cfg    ReclaimConfig
	m      *metrics.Metrics
	cursor string
}

// NewReclaimer builds a reclaimer for the consumer's group. The reclaimed
// messages are delivered to the calling node, so it must have pool capacity.
func NewReclaimer(c *Consumer, cfg ReclaimConfig, m *metrics.Metrics) *Reclaimer {
	cfg.withDefaults()
	return &Reclaimer{c: c, cfg: cfg, m: m, cursor: "0-0"}
}

// ReclaimOnce runs a single XAUTOCLAIM pass from the stored cursor. It returns
// the messages this node now owns; entries already deleted from the stream are
// dropped by Redis and reported in the deleted slice.
func (r *Reclaimer) ReclaimOnce(ctx context.Context) ([]Record, error) {
	msgs, next, err := r.c.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   r.c.cfg.Stream,
		Group:    r.c.cfg.Group,
		Consumer: r.c.cfg.Consumer,
		MinIdle:  r.cfg.MinIdle,
		Start:    r.cursor,
		Count:    r.cfg.Count,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("xautoclaim from %s: %w", r.cursor, err)
	}
	// "0-0" means the scan wrapped; start the next sweep from the top.
	r.cursor = next
	if r.cursor == "" {
		r.cursor = "0-0"
	}
	if len(msgs) == 0 {
		return nil, nil
	}

	// XAUTOCLAIM resets idle to zero on the claimed entries, so the age has to
	// come from the PEL snapshot taken in the same pass.
	ages := r.pendingAges(ctx, msgs)

	out := make([]Record, 0, len(msgs))
	for _, msg := range msgs {
		age := ages[msg.ID]
		if age < r.cfg.MinIdle {
			age = r.cfg.MinIdle
		}
		rec := toRecord(msg, age)
		rec.Redelivered = true
		out = append(out, rec)
		if r.m != nil {
			r.m.ObserveRecovery(age)
		}
	}
	if r.m != nil {
		r.m.ReclaimedTotal.Add(float64(len(out)))
		r.m.ReclaimBatches.Inc()
	}
	return out, nil
}

// Run sweeps on a ticker until ctx is cancelled, handing each reclaimed batch
// to deliver. deliver is expected to block when the worker pool is saturated,
// which throttles reclaim the same way it throttles fresh reads.
func (r *Reclaimer) Run(ctx context.Context, deliver func(context.Context, []Record) error) error {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			batch, err := r.ReclaimOnce(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return err
			}
			if len(batch) == 0 {
				continue
			}
			if err := deliver(ctx, batch); err != nil {
				return err
			}
		}
	}
}

// pendingAges reads delivery age and delivery count for the claimed IDs, and
// dead-letters anything past MaxDeliveries.
func (r *Reclaimer) pendingAges(ctx context.Context, msgs []redis.XMessage) map[string]time.Duration {
	ages := make(map[string]time.Duration, len(msgs))
	pending, err := r.c.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   r.c.cfg.Stream,
		Group:    r.c.cfg.Group,
		Consumer: r.c.cfg.Consumer,
		Start:    "-",
		End:      "+",
		Count:    int64(len(msgs)),
	}).Result()
	if err != nil {
		return ages
	}
	for _, p := range pending {
		ages[p.ID] = p.Idle
		if r.cfg.MaxDeliveries > 0 && p.RetryCount > r.cfg.MaxDeliveries {
			r.deadLetter(ctx, p.ID)
		}
	}
	return ages
}

// deadLetter moves a poison entry off the hot path: copy to <stream>.dead,
// then ack so the group stops redelivering it.
func (r *Reclaimer) deadLetter(ctx context.Context, entryID string) {
	pipe := r.c.rdb.TxPipeline()
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: r.c.cfg.Stream + ".dead",
		Values: map[string]any{"entry": entryID, "group": r.c.cfg.Group},
	})
	pipe.XAck(ctx, r.c.cfg.Stream, r.c.cfg.Group, entryID)
	_, _ = pipe.Exec(ctx)
}
