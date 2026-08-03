package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/trungprofile/distributed-processing-engine/internal/cluster"
	"github.com/trungprofile/distributed-processing-engine/internal/metrics"
	"github.com/trungprofile/distributed-processing-engine/internal/store"
	"github.com/trungprofile/distributed-processing-engine/internal/stream"
)

// NodeConfig is the full runtime configuration of one worker node.
type NodeConfig struct {
	NodeID      string
	Concurrency int

	Stream string
	Group  string
	Prefix string
	Sink   string

	Batch         int64
	Block         time.Duration
	MaxLen        int64
	MinIdle       time.Duration
	ReclaimEvery  time.Duration
	HeartbeatBeat time.Duration
	ClaimTTL      time.Duration
	CommitTTL     time.Duration
	MaxDeliveries int64
	DrainTimeout  time.Duration
}

func (c *NodeConfig) withDefaults() {
	if c.Concurrency <= 0 {
		c.Concurrency = 64
	}
	if c.Stream == "" {
		c.Stream = "dpe:records"
	}
	if c.Group == "" {
		c.Group = "dpe-workers"
	}
	if c.Prefix == "" {
		c.Prefix = "dpe"
	}
	if c.Sink == "" {
		c.Sink = "dpe:results"
	}
	if c.Batch <= 0 {
		c.Batch = 128
	}
	if c.Block <= 0 {
		c.Block = 2 * time.Second
	}
	if c.MinIdle <= 0 {
		c.MinIdle = 5 * time.Second
	}
	if c.ReclaimEvery <= 0 {
		c.ReclaimEvery = c.MinIdle / 3
	}
	if c.HeartbeatBeat <= 0 {
		c.HeartbeatBeat = 3 * time.Second
	}
	if c.ClaimTTL <= 0 {
		c.ClaimTTL = c.MinIdle
	}
	if c.CommitTTL <= 0 {
		c.CommitTTL = time.Hour
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = 30 * time.Second
	}
}

// Node is one worker process: a read loop, a reclaim loop, a heartbeat and the
// pool that executes records. cmd/worker is a thin wrapper over this type, and
// the integration test runs several Nodes in-process to simulate a cluster.
type Node struct {
	cfg      NodeConfig
	log      *slog.Logger
	m        *metrics.Metrics
	consumer *stream.Consumer
	reclaim  *stream.Reclaimer
	store    *store.Store
	registry *cluster.Registry
	pool     *Pool

	processed atomic.Int64
	reclaimed atomic.Int64

	drainOnce chan struct{}
	killed    chan struct{}
}

// NewNode builds a node against an existing Redis client. Passing a nil sink
// selects the default Redis hash sink.
func NewNode(rdb redis.UniversalClient, cfg NodeConfig, sink store.Sink, log *slog.Logger) (*Node, error) {
	cfg.withDefaults()
	if cfg.NodeID == "" {
		return nil, errors.New("worker: NodeID is required")
	}
	if log == nil {
		log = slog.Default()
	}
	if sink == nil {
		sink = store.NewRedisSink(rdb, cfg.Sink)
	}

	m := metrics.New(cfg.NodeID)
	consumer := stream.NewConsumer(rdb, stream.Config{
		Stream:   cfg.Stream,
		Group:    cfg.Group,
		Consumer: cfg.NodeID,
		Batch:    cfg.Batch,
		Block:    cfg.Block,
		MaxLen:   cfg.MaxLen,
	}, m)

	n := &Node{
		cfg:      cfg,
		log:      log.With("node", cfg.NodeID),
		m:        m,
		consumer: consumer,
		reclaim: stream.NewReclaimer(consumer, stream.ReclaimConfig{
			MinIdle:       cfg.MinIdle,
			Interval:      cfg.ReclaimEvery,
			Count:         cfg.Batch,
			MaxDeliveries: cfg.MaxDeliveries,
		}, m),
		store: store.New(rdb, sink, store.Config{
			Prefix:    cfg.Prefix + ":idem:",
			ClaimTTL:  cfg.ClaimTTL,
			CommitTTL: cfg.CommitTTL,
		}, cfg.NodeID, m),
		registry:  cluster.NewRegistry(rdb, cfg.Prefix),
		drainOnce: make(chan struct{}),
		killed:    make(chan struct{}),
	}
	n.pool = New(cfg.Concurrency, n.handle, m)
	n.pool.HandlerTimeout = cfg.ClaimTTL
	return n, nil
}

// Metrics exposes the node's registry for the /metrics endpoint.
func (n *Node) Metrics() *metrics.Metrics { return n.m }

// Processed is the number of records this node committed to the sink.
func (n *Node) Processed() int64 { return n.processed.Load() }

// Reclaimed is the number of records this node took over from dead peers.
func (n *Node) Reclaimed() int64 { return n.reclaimed.Load() }

// Run starts every loop and blocks until ctx is cancelled or a drain request
// arrives, then drains. Returning nil means the node stopped cleanly with no
// record left half-processed.
func (n *Node) Run(ctx context.Context) error {
	if err := n.consumer.EnsureGroup(ctx); err != nil {
		return err
	}

	// intakeCtx is cancelled first: it stops reading and reclaiming while the
	// pool is still allowed to finish what it holds.
	intakeCtx, stopIntake := context.WithCancel(ctx)
	defer stopIntake()

	g, gctx := errgroup.WithContext(intakeCtx)
	g.Go(func() error { return n.readLoop(gctx) })
	g.Go(func() error { return n.reclaimLoop(gctx) })
	g.Go(func() error { return n.heartbeatLoop(gctx) })
	g.Go(func() error {
		return n.registry.WatchDrain(gctx, n.cfg.NodeID, n.RequestDrain)
	})
	g.Go(func() error {
		select {
		case <-gctx.Done():
			return nil
		case <-n.drainOnce:
			n.log.Info("drain requested, stopping intake")
			stopIntake()
			return nil
		case <-n.killed:
			stopIntake()
			return nil
		}
	})

	err := g.Wait()
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		n.log.Error("node loop failed", "error", err)
	}
	select {
	case <-n.killed:
		// Crash path: no drain, no acks. Whatever this node held stays in the
		// pending list until a peer's reclaim sweep picks it up.
		return nil
	default:
	}
	return n.Drain(context.WithoutCancel(ctx))
}

// Kill stops the node without draining. It exists for failure testing: it
// leaves in-flight records unacked exactly as a SIGKILL or a lost instance
// would, so the reclaim path is what recovers them.
func (n *Node) Kill() {
	select {
	case <-n.killed:
	default:
		close(n.killed)
	}
}

// RequestDrain triggers a graceful drain. Safe to call more than once.
func (n *Node) RequestDrain() {
	select {
	case <-n.drainOnce:
	default:
		close(n.drainOnce)
	}
}

// Drain finishes in-flight work and deregisters the node. Records that cannot
// be finished within DrainTimeout are left unacked for the reclaimer, which is
// the safe direction: at worst they are processed twice and deduplicated.
func (n *Node) Drain(ctx context.Context) error {
	dctx, cancel := context.WithTimeout(ctx, n.cfg.DrainTimeout)
	defer cancel()

	err := n.pool.Drain(dctx)
	if err != nil {
		n.log.Warn("drain deadline exceeded, leaving work pending for reclaim",
			"in_flight", n.pool.InFlight())
	}
	_ = n.registry.Deregister(context.WithoutCancel(ctx), n.cfg.NodeID)
	n.log.Info("drained", "processed", n.processed.Load(), "reclaimed", n.reclaimed.Load())
	return err
}

// handle is the per-record path: idempotent write, then ack. A record that is
// not committed here stays in the pending list on purpose.
func (n *Node) handle(ctx context.Context, r stream.Record) error {
	outcome, err := n.store.Apply(ctx, r)
	if err != nil {
		n.log.Warn("apply failed, leaving entry pending", "key", r.Key, "error", err)
		return err
	}
	if outcome == store.Contended {
		// Another node holds a live claim on this key. Do not ack: if that
		// node dies the entry must remain reclaimable.
		return nil
	}
	if err := n.consumer.Ack(ctx, r.EntryID); err != nil {
		return fmt.Errorf("ack %s: %w", r.EntryID, err)
	}
	n.processed.Add(1)
	return nil
}

// readLoop keeps the in-flight window full and no fuller. It never reads more
// entries than there is capacity to run, so pending work stays in Redis rather
// than in this process's heap.
func (n *Node) readLoop(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		free := int64(n.pool.Capacity()) - n.pool.InFlight()
		if free <= 0 {
			// Saturated: wait for a slot instead of pulling more entries into
			// this node's ownership.
			if err := sleepCtx(ctx, 5*time.Millisecond); err != nil {
				return err
			}
			continue
		}
		if free > n.cfg.Batch {
			free = n.cfg.Batch
		}

		records, err := n.consumer.Read(ctx, free)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			n.log.Warn("read failed", "error", err)
			if err := sleepCtx(ctx, 250*time.Millisecond); err != nil {
				return err
			}
			continue
		}
		if _, err := n.pool.SubmitBatch(ctx, records); err != nil {
			if errors.Is(err, ErrDraining) || ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
	}
}

// reclaimLoop takes over work from consumers whose lease expired.
func (n *Node) reclaimLoop(ctx context.Context) error {
	return n.reclaim.Run(ctx, func(ctx context.Context, batch []stream.Record) error {
		n.reclaimed.Add(int64(len(batch)))
		n.log.Info("reclaimed messages from expired leases", "count", len(batch))
		_, err := n.pool.SubmitBatch(ctx, batch)
		if errors.Is(err, ErrDraining) {
			return nil
		}
		return err
	})
}

// heartbeatLoop publishes node state for GetClusterStats and refreshes the
// stream lag gauge.
func (n *Node) heartbeatLoop(ctx context.Context) error {
	t := time.NewTicker(n.cfg.HeartbeatBeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			state := cluster.NodeState{
				NodeID:    n.cfg.NodeID,
				Capacity:  int32(n.pool.Capacity()),
				InFlight:  n.pool.InFlight(),
				Processed: n.processed.Load(),
				Reclaimed: n.reclaimed.Load(),
				Draining:  n.pool.Draining(),
			}
			if err := n.registry.Heartbeat(ctx, state); err != nil && ctx.Err() == nil {
				n.log.Warn("heartbeat failed", "error", err)
			}
			if _, err := n.consumer.Stats(ctx); err != nil && ctx.Err() == nil {
				n.log.Warn("stats refresh failed", "error", err)
			}
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
