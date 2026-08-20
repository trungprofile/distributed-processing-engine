package controller

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"

	dpev1alpha1 "github.com/trungprofile/distributed-processing-engine/api/v1alpha1"
)

// desiredReplicas is the autoscaling rule.
//
//	desired = clamp(ceil(streamLag / targetLagPerWorker), min, max)
//
// The signal is consumer-group lag rather than CPU because lag is the only
// quantity proportional to unfinished work. A worker running its bounded pool
// flat out looks identical at 100 records of backlog and at 10 million, so a
// CPU-driven autoscaler cannot tell a cluster that is keeping up from one that
// is falling behind — and the engine's whole backpressure design guarantees a
// busy worker stays busy rather than queuing.
//
// Lag counts entries never delivered to any consumer. Entries already checked
// out (the pending list) are excluded on purpose: they are someone's in-flight
// work, and adding a worker does not make them finish sooner.
func desiredReplicas(spec dpev1alpha1.ProcessingJobSpec, streamLag int64) int32 {
	as := spec.Autoscale
	if streamLag < 0 {
		streamLag = 0
	}

	// Integer ceiling division: one worker per targetLagPerWorker entries,
	// rounding any remainder up so a small backlog still gets a worker.
	want := (streamLag + as.TargetLagPerWorker - 1) / as.TargetLagPerWorker
	if want > int64(as.MaxReplicas) {
		want = int64(as.MaxReplicas)
	}
	if want < int64(as.MinReplicas) {
		want = int64(as.MinReplicas)
	}
	return int32(want)
}

// scaleDownTracker damps scale-in.
//
// Scale-up is immediate: backlog is already costing latency, and adding a
// consumer is free. Scale-down is not symmetric — it costs a drain, and a
// backlog oscillating around the threshold would drain a worker every few
// seconds. So a lower desired count must hold *continuously* for the
// stabilization window before it is acted on, and any reconcile that wants the
// current count or more resets the clock.
//
// The state is in memory rather than in status on purpose: it is a debounce
// timer, not a fact about the cluster. An operator restart resets the timers,
// which costs at most one extra stabilization window before the first
// scale-in — strictly the safe direction.
type scaleDownTracker struct {
	mu    sync.Mutex
	since map[types.NamespacedName]time.Time
}

func newScaleDownTracker() *scaleDownTracker {
	return &scaleDownTracker{since: make(map[types.NamespacedName]time.Time)}
}

// allow reports whether a scale-in from current to desired may proceed now.
//
// It returns the earliest time the scale-in becomes permissible, so the caller
// can requeue exactly then instead of polling. When desired >= current there
// is nothing to damp: the pending timer is cleared and the call is allowed.
func (t *scaleDownTracker) allow(key types.NamespacedName, current, desired int32, window time.Duration, now time.Time) (bool, time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if desired >= current {
		delete(t.since, key)
		return true, now
	}

	first, ok := t.since[key]
	if !ok {
		t.since[key] = now
		first = now
	}
	ready := first.Add(window)
	if now.Before(ready) {
		return false, ready
	}
	// The scale-in is happening; the next lower step has to earn its own
	// window rather than inheriting this one.
	delete(t.since, key)
	return true, now
}

// forget drops any pending timer for a job, so a deleted or rescaled job does
// not leave state behind in a long-lived operator process.
func (t *scaleDownTracker) forget(key types.NamespacedName) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.since, key)
}
