package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	dpev1alpha1 "github.com/trungprofile/distributed-processing-engine/api/v1alpha1"
)

func specWith(min, max int32, target int64) dpev1alpha1.ProcessingJobSpec {
	return dpev1alpha1.ProcessingJobSpec{
		RedisAddr: "redis:6379",
		Image:     "dpe:test",
		Autoscale: dpev1alpha1.AutoscaleSpec{
			MinReplicas:        min,
			MaxReplicas:        max,
			TargetLagPerWorker: target,
		},
	}.Defaulted()
}

func TestDesiredReplicas(t *testing.T) {
	spec := specWith(1, 16, 500)

	cases := []struct {
		name string
		lag  int64
		want int32
	}{
		{"empty backlog holds the floor", 0, 1},
		{"a single record still needs a worker", 1, 1},
		{"exactly one worker's worth", 500, 1},
		{"one over rounds up", 501, 2},
		{"mid range", 4200, 9},
		{"saturates at the ceiling", 1_000_000, 16},
		{"negative lag is treated as empty", -5, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := desiredReplicas(spec, tc.lag); got != tc.want {
				t.Fatalf("desiredReplicas(lag=%d) = %d, want %d", tc.lag, got, tc.want)
			}
		})
	}
}

// TestDesiredReplicasRespectsFloor covers a min above what the backlog alone
// would justify: a job that must always keep several consumers in the group.
func TestDesiredReplicasRespectsFloor(t *testing.T) {
	spec := specWith(4, 16, 500)
	if got := desiredReplicas(spec, 0); got != 4 {
		t.Fatalf("desiredReplicas(lag=0) = %d, want the floor of 4", got)
	}
	if got := desiredReplicas(spec, 5000); got != 10 {
		t.Fatalf("desiredReplicas(lag=5000) = %d, want 10", got)
	}
}

// TestDefaultedClampsInvertedBounds asserts a min above max cannot produce a
// desired count above the ceiling the user asked for.
func TestDefaultedClampsInvertedBounds(t *testing.T) {
	spec := specWith(20, 4, 500)
	if spec.Autoscale.MinReplicas != 4 {
		t.Fatalf("min = %d, want it clamped to max of 4", spec.Autoscale.MinReplicas)
	}
	if got := desiredReplicas(spec, 0); got != 4 {
		t.Fatalf("desiredReplicas(lag=0) = %d, want 4", got)
	}
}

func TestScaleDownTrackerHoldsForTheWindow(t *testing.T) {
	tr := newScaleDownTracker()
	key := types.NamespacedName{Namespace: "dpe", Name: "records"}
	window := 60 * time.Second
	t0 := time.Unix(1_700_000_000, 0)

	// First observation of a lower desired count only starts the clock.
	allowed, readyAt := tr.allow(key, 8, 4, window, t0)
	if allowed {
		t.Fatal("scale-in allowed on the first observation; the window must hold it")
	}
	if !readyAt.Equal(t0.Add(window)) {
		t.Fatalf("readyAt = %s, want %s", readyAt, t0.Add(window))
	}

	// Still inside the window.
	if allowed, _ := tr.allow(key, 8, 4, window, t0.Add(59*time.Second)); allowed {
		t.Fatal("scale-in allowed before the window closed")
	}

	// Window closed.
	if allowed, _ := tr.allow(key, 8, 4, window, t0.Add(window)); !allowed {
		t.Fatal("scale-in still blocked after the window closed")
	}
}

func TestScaleDownTrackerResetsOnDemandSpike(t *testing.T) {
	tr := newScaleDownTracker()
	key := types.NamespacedName{Namespace: "dpe", Name: "records"}
	window := 60 * time.Second
	t0 := time.Unix(1_700_000_000, 0)

	if allowed, _ := tr.allow(key, 8, 4, window, t0); allowed {
		t.Fatal("scale-in allowed on the first observation")
	}

	// Backlog rises again 30s in: scaling up is always allowed and must clear
	// the pending timer.
	if allowed, _ := tr.allow(key, 8, 12, window, t0.Add(30*time.Second)); !allowed {
		t.Fatal("scale-up was blocked; only scale-in is damped")
	}

	// The backlog drops again. The clock restarts from here, so the original
	// deadline must no longer apply.
	if allowed, _ := tr.allow(key, 8, 4, window, t0.Add(31*time.Second)); allowed {
		t.Fatal("scale-in allowed on a restarted window")
	}
	if allowed, _ := tr.allow(key, 8, 4, window, t0.Add(90*time.Second)); allowed {
		t.Fatal("scale-in used the stale deadline instead of the restarted one")
	}
	if allowed, _ := tr.allow(key, 8, 4, window, t0.Add(91*time.Second)); !allowed {
		t.Fatal("scale-in blocked past the restarted window")
	}
}

// TestScaleDownTrackerRearmsPerStep asserts each step down earns its own
// window: a permitted scale-in must not leave the gate open for the next one.
func TestScaleDownTrackerRearmsPerStep(t *testing.T) {
	tr := newScaleDownTracker()
	key := types.NamespacedName{Namespace: "dpe", Name: "records"}
	window := 30 * time.Second
	t0 := time.Unix(1_700_000_000, 0)

	_, _ = tr.allow(key, 8, 4, window, t0)
	if allowed, _ := tr.allow(key, 8, 4, window, t0.Add(window)); !allowed {
		t.Fatal("first step down was blocked past its window")
	}
	if allowed, _ := tr.allow(key, 7, 4, window, t0.Add(window)); allowed {
		t.Fatal("second step down reused the first step's window")
	}
}

// TestScaleDownTrackerForgetsDeletedJobs keeps a long-lived operator from
// accumulating timers for jobs that no longer exist.
func TestScaleDownTrackerForgetsDeletedJobs(t *testing.T) {
	tr := newScaleDownTracker()
	key := types.NamespacedName{Namespace: "dpe", Name: "records"}
	t0 := time.Unix(1_700_000_000, 0)

	_, _ = tr.allow(key, 8, 4, time.Minute, t0)
	tr.forget(key)

	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.since) != 0 {
		t.Fatalf("tracker still holds %d timers after forget", len(tr.since))
	}
}

// TestDefaultedFillsEveryOptionalField pins the controller's view of an
// undefaulted spec to the values the CRD schema declares.
func TestDefaultedFillsEveryOptionalField(t *testing.T) {
	spec := dpev1alpha1.ProcessingJobSpec{RedisAddr: "redis:6379", Image: "dpe:test"}.Defaulted()

	if spec.Stream != "dpe:records" || spec.Group != "dpe-workers" {
		t.Fatalf("stream/group = %q/%q", spec.Stream, spec.Group)
	}
	if spec.Concurrency != 64 {
		t.Fatalf("concurrency = %d, want 64", spec.Concurrency)
	}
	if spec.MinIdle.Duration != 3*time.Second {
		t.Fatalf("minIdle = %s, want 3s", spec.MinIdle.Duration)
	}
	if spec.DrainTimeout.Duration != 30*time.Second {
		t.Fatalf("drainTimeout = %s, want 30s", spec.DrainTimeout.Duration)
	}
	if spec.Autoscale.MaxReplicas != 16 || spec.Autoscale.TargetLagPerWorker != 500 {
		t.Fatalf("autoscale defaults = %+v", spec.Autoscale)
	}
	if spec.Autoscale.StabilizationWindow.Duration != 60*time.Second {
		t.Fatalf("stabilizationWindow = %s, want 60s", spec.Autoscale.StabilizationWindow.Duration)
	}
	if spec.Placement.Policy != dpev1alpha1.PlacementSpread {
		t.Fatalf("placement policy = %q, want Spread", spec.Placement.Policy)
	}
}

// TestDefaultedLeavesExplicitValues asserts defaulting never overwrites an
// operator's choice, including a deliberate floor of zero replicas.
func TestDefaultedLeavesExplicitValues(t *testing.T) {
	spec := dpev1alpha1.ProcessingJobSpec{
		RedisAddr:   "redis:6379",
		Image:       "dpe:test",
		Stream:      "orders",
		Concurrency: 8,
		MinIdle:     &metav1.Duration{Duration: 9 * time.Second},
		Autoscale:   dpev1alpha1.AutoscaleSpec{MinReplicas: 0, MaxReplicas: 3},
		Placement:   dpev1alpha1.PlacementSpec{Policy: dpev1alpha1.PlacementBinPack},
	}.Defaulted()

	if spec.Stream != "orders" || spec.Concurrency != 8 || spec.MinIdle.Duration != 9*time.Second {
		t.Fatalf("defaulting overwrote explicit values: %+v", spec)
	}
	if spec.Autoscale.MinReplicas != 0 {
		t.Fatalf("minReplicas = %d, want an explicit 0 preserved", spec.Autoscale.MinReplicas)
	}
	if spec.Placement.Policy != dpev1alpha1.PlacementBinPack {
		t.Fatalf("placement policy = %q, want BinPack preserved", spec.Placement.Policy)
	}
	if got := desiredReplicas(spec, 0); got != 0 {
		t.Fatalf("desiredReplicas(lag=0) = %d, want 0 for a job allowed to idle empty", got)
	}
}
