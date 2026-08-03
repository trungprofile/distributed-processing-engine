package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/trungprofile/distributed-processing-engine/internal/stream"
)

// TestPoolBoundsConcurrency asserts the in-flight window is a hard bound: this
// is the property the whole backpressure story rests on.
func TestPoolBoundsConcurrency(t *testing.T) {
	const size = 4
	var cur, peak atomic.Int64
	release := make(chan struct{})

	p := New(size, func(context.Context, stream.Record) error {
		n := cur.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		<-release
		cur.Add(-1)
		return nil
	}, nil)

	ctx := context.Background()
	for i := 0; i < size; i++ {
		if err := p.Submit(ctx, stream.Record{Key: "k"}); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	// The next submit must block until a slot frees.
	blocked := make(chan error, 1)
	go func() { blocked <- p.Submit(ctx, stream.Record{Key: "overflow"}) }()
	select {
	case err := <-blocked:
		t.Fatalf("submit past capacity returned early (%v); the pool is not bounded", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("submit after free slot: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("submit never unblocked after slots freed")
	}

	if err := p.Drain(timeoutCtx(t, 5*time.Second)); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if peak.Load() > size {
		t.Errorf("peak concurrency %d exceeded window %d", peak.Load(), size)
	}
}

// TestDrainWaitsForInFlight covers the SIGTERM path: work already accepted
// must finish before Drain returns, and new work must be refused.
func TestDrainWaitsForInFlight(t *testing.T) {
	var finished atomic.Int64
	p := New(2, func(context.Context, stream.Record) error {
		time.Sleep(150 * time.Millisecond)
		finished.Add(1)
		return nil
	}, nil)

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := p.Submit(ctx, stream.Record{Key: "k"}); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}

	if err := p.Drain(timeoutCtx(t, 5*time.Second)); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := finished.Load(); got != 2 {
		t.Errorf("drain returned with %d of 2 handlers finished", got)
	}
	if err := p.Submit(ctx, stream.Record{Key: "late"}); err != ErrDraining {
		t.Errorf("submit after drain = %v, want ErrDraining", err)
	}
}

// TestHandlerSurvivesReaderCancellation guards the ordering guarantee: the
// read loop's context is cancelled at shutdown, but a record already accepted
// must still run to completion so it is never left written-but-unacked.
func TestHandlerSurvivesReaderCancellation(t *testing.T) {
	done := make(chan error, 1)
	p := New(1, func(ctx context.Context, _ stream.Record) error {
		time.Sleep(50 * time.Millisecond)
		done <- ctx.Err()
		return nil
	}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	if err := p.Submit(ctx, stream.Record{Key: "k"}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("handler context was cancelled with the reader: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler never ran")
	}
}

func timeoutCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
