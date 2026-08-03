// Package worker contains the bounded-concurrency execution pool that sits
// between the stream reader and the sink.
//
// The pool is also the backpressure mechanism. There is no unbounded queue in
// front of it: Submit blocks once the in-flight window is full, the read loop
// blocks on Submit, and XREADGROUP is therefore not called again until a slot
// frees. Unread entries stay in Redis, which is the only place with the memory
// budget to hold them.
package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trungprofile/distributed-processing-engine/internal/metrics"
	"github.com/trungprofile/distributed-processing-engine/internal/stream"
)

// ErrDraining is returned by Submit once the node has begun a graceful drain.
var ErrDraining = errors.New("worker: pool is draining")

// Handler processes a single record. It owns the ack decision for that record
// so the pool never acks work it did not observe complete.
type Handler func(ctx context.Context, r stream.Record) error

// Pool runs handlers with a fixed in-flight window.
type Pool struct {
	// HandlerTimeout bounds a single record's processing time. Zero means no
	// bound beyond the drain deadline.
	HandlerTimeout time.Duration

	handler Handler
	slots   chan struct{}
	wg      sync.WaitGroup
	m       *metrics.Metrics

	inflight atomic.Int64
	draining atomic.Bool

	drainOnce sync.Once
	drained   chan struct{}
}

// New builds a pool with the given concurrency. size <= 0 is treated as 1.
func New(size int, h Handler, m *metrics.Metrics) *Pool {
	if size <= 0 {
		size = 1
	}
	p := &Pool{
		handler: h,
		slots:   make(chan struct{}, size),
		m:       m,
		drained: make(chan struct{}),
	}
	if m != nil {
		m.PoolSize.Set(float64(size))
	}
	return p
}

// Capacity is the configured in-flight window.
func (p *Pool) Capacity() int { return cap(p.slots) }

// InFlight is the number of records currently checked out.
func (p *Pool) InFlight() int64 { return p.inflight.Load() }

// Saturated reports whether the next Submit would block.
func (p *Pool) Saturated() bool { return len(p.slots) == cap(p.slots) }

// Draining reports whether a graceful shutdown is in progress.
func (p *Pool) Draining() bool { return p.draining.Load() }

// Submit blocks until a slot is free, then runs the record asynchronously.
// The block is deliberate: it is how saturation propagates back to the reader.
func (p *Pool) Submit(ctx context.Context, r stream.Record) error {
	if p.draining.Load() {
		return ErrDraining
	}
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	// Re-check after acquiring: a drain may have started while we waited.
	if p.draining.Load() {
		<-p.slots
		return ErrDraining
	}

	p.wg.Add(1)
	p.inflight.Add(1)
	if p.m != nil {
		p.m.InFlight.Set(float64(p.inflight.Load()))
	}

	// The handler deliberately does not inherit cancellation from ctx.
	// Cancelling the read loop at shutdown must not abandon a record halfway
	// between the sink write and the ack; Drain owns handler lifetime instead.
	runCtx := context.WithoutCancel(ctx)
	var cancel context.CancelFunc = func() {}
	if p.HandlerTimeout > 0 {
		runCtx, cancel = context.WithTimeout(runCtx, p.HandlerTimeout)
	}

	go func() {
		defer func() {
			cancel()
			p.inflight.Add(-1)
			if p.m != nil {
				p.m.InFlight.Set(float64(p.inflight.Load()))
			}
			<-p.slots
			p.wg.Done()
		}()
		if err := p.handler(runCtx, r); err != nil && p.m != nil {
			p.m.RecordsFailed.WithLabelValues("handler").Inc()
		}
	}()
	return nil
}

// SubmitBatch submits records in order, stopping at the first refusal. It
// returns the number accepted so the caller knows which entries are still
// unowned and must be left unacked.
func (p *Pool) SubmitBatch(ctx context.Context, records []stream.Record) (int, error) {
	for i, r := range records {
		if err := p.Submit(ctx, r); err != nil {
			return i, err
		}
	}
	return len(records), nil
}

// Drain stops accepting new work and waits for in-flight handlers to finish.
// On SIGTERM the node calls Drain before closing its Redis connection, so
// every record it owns is either committed and acked, or left pending for the
// reclaimer. Nothing is dropped in between.
func (p *Pool) Drain(ctx context.Context) error {
	p.draining.Store(true)
	if p.m != nil {
		p.m.Draining.Set(1)
	}
	p.drainOnce.Do(func() {
		go func() {
			p.wg.Wait()
			close(p.drained)
		}()
	})

	select {
	case <-p.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitIdle blocks until nothing is in flight or the deadline passes. Used by
// the drain RPC and by tests that need a quiescent cluster.
func (p *Pool) WaitIdle(ctx context.Context, poll time.Duration) error {
	if poll <= 0 {
		poll = 25 * time.Millisecond
	}
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		if p.inflight.Load() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
