package controller

import (
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	enginepb "github.com/trungprofile/distributed-processing-engine/api/proto"
	dpev1alpha1 "github.com/trungprofile/distributed-processing-engine/api/v1alpha1"
)

// drainState is what one reconcile pass concluded about the scale-in victim.
type drainState int

const (
	// drainGone means the victim has left the registry: it either drained and
	// deregistered, or it was never there. Removing the replica is safe.
	drainGone drainState = iota
	// drainPending means the victim is still registered. The replica count
	// must not move yet.
	drainPending
	// drainExpired means the victim has been draining longer than the job's
	// drain timeout allows. The operator stops waiting but does not force:
	// see waitForDrain.
	drainExpired
)

// drainAttempt records which node the operator asked to drain and when.
type drainAttempt struct {
	node      string
	startedAt time.Time
}

// drainTracker remembers the in-flight drain for each job.
//
// Like the scale-down timer this lives in memory: it is a stopwatch on a
// request that has already been broadcast, not a fact about the cluster. If
// the operator restarts mid-drain the worker keeps draining regardless — the
// drain is the worker's own SIGTERM path, not something the operator steps
// through — and the new leader simply re-observes the node in the registry,
// re-broadcasts the (idempotent) request and starts its own clock.
type drainTracker struct {
	mu       sync.Mutex
	attempts map[types.NamespacedName]drainAttempt
}

func newDrainTracker() *drainTracker {
	return &drainTracker{attempts: make(map[types.NamespacedName]drainAttempt)}
}

// current returns the in-flight attempt for a job, if any.
func (t *drainTracker) current(key types.NamespacedName) (drainAttempt, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	a, ok := t.attempts[key]
	return a, ok
}

// start records a new attempt, replacing any previous one.
func (t *drainTracker) start(key types.NamespacedName, node string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.attempts[key] = drainAttempt{node: node, startedAt: now}
}

// clear forgets a job's attempt, whether it succeeded or timed out.
func (t *drainTracker) clear(key types.NamespacedName) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.attempts, key)
}

// victimNode is the node ID of the pod that a scale-in from `replicas` would
// remove. A StatefulSet always removes the highest ordinal, so the victim is
// deterministic — which is exactly why the drain protocol can name it up front
// instead of discovering it after the fact.
func victimNode(job *dpev1alpha1.ProcessingJob, replicas int32) string {
	return podName(job, replicas-1)
}

// beginDrain asks the coordinator to drain the scale-in victim.
//
// The request goes through the same DrainNode RPC an operator would use by
// hand, which publishes on the registry's drain channel. The worker stops
// reading, finishes and acks what it holds, deregisters, and disappears from
// GetClusterStats. Only then is its pod safe to remove.
//
// Calling this repeatedly is harmless: the worker's drain path is idempotent,
// and a node that already left the registry ignores the broadcast.
func (r *ProcessingJobReconciler) beginDrain(ctx context.Context, addr, nodeID string) error {
	log.FromContext(ctx).Info("requesting drain before scale-in", "node", nodeID)
	return r.Engine.DrainNode(ctx, addr, nodeID)
}

// waitForDrain classifies the victim's progress from the coordinator's view.
//
// The deliberate omission here is force. If the drain outlives its budget the
// operator logs it, leaves the replica in place, and requeues — it never
// deletes a pod that is still holding checked-out records. Doing so would
// strand that work in the pending entries list for a full lease, and the
// reclaimer would have to recover records that a few more seconds of patience
// would have committed. A stuck drain is a slow scale-in; a forced one is
// avoidable duplicate work.
func waitForDrain(stats *enginepb.ClusterStats, nodeID string, startedAt time.Time, budget time.Duration, now time.Time) drainState {
	for _, n := range stats.GetNodes() {
		if n.GetNodeId() != nodeID {
			continue
		}
		if now.Sub(startedAt) > budget {
			return drainExpired
		}
		return drainPending
	}
	return drainGone
}
