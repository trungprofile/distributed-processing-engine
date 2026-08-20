package controller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	enginepb "github.com/trungprofile/distributed-processing-engine/api/proto"
	dpev1alpha1 "github.com/trungprofile/distributed-processing-engine/api/v1alpha1"
)

// fakeEngine stands in for the coordinator's control plane. It records every
// drain request so a test can assert the operator asked before it acted.
type fakeEngine struct {
	mu       sync.Mutex
	stats    *enginepb.ClusterStats
	statsErr error
	drained  []string
	drainErr error
}

func (f *fakeEngine) ClusterStats(context.Context, string) (*enginepb.ClusterStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statsErr != nil {
		return nil, f.statsErr
	}
	return f.stats, nil
}

func (f *fakeEngine) DrainNode(_ context.Context, _, nodeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.drainErr != nil {
		return f.drainErr
	}
	f.drained = append(f.drained, nodeID)
	return nil
}

func (f *fakeEngine) Close() error { return nil }

// removeNode makes a node disappear from the registry, which is what a worker
// that finished draining and deregistered looks like to the operator.
func (f *fakeEngine) removeNode(nodeID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	kept := make([]*enginepb.NodeStats, 0, len(f.stats.Nodes))
	for _, n := range f.stats.Nodes {
		if n.NodeId != nodeID {
			kept = append(kept, n)
		}
	}
	f.stats.Nodes = kept
}

func (f *fakeEngine) setLag(lag int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stats.StreamLag = lag
}

func (f *fakeEngine) drainRequests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.drained...)
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := dpev1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add dpe scheme: %v", err)
	}
	return s
}

func testJob() *dpev1alpha1.ProcessingJob {
	return &dpev1alpha1.ProcessingJob{
		ObjectMeta: metav1.ObjectMeta{Name: "records", Namespace: "dpe", Generation: 1},
		Spec: dpev1alpha1.ProcessingJobSpec{
			RedisAddr:       "redis:6379",
			Image:           "dpe:test",
			CoordinatorAddr: "coordinator:9090",
			Autoscale: dpev1alpha1.AutoscaleSpec{
				MinReplicas:         1,
				MaxReplicas:         8,
				TargetLagPerWorker:  500,
				StabilizationWindow: &metav1.Duration{Duration: 0},
			},
		},
	}
}

// nodesFor builds a registry view with one entry per pod ordinal.
func nodesFor(job *dpev1alpha1.ProcessingJob, replicas int32) []*enginepb.NodeStats {
	out := make([]*enginepb.NodeStats, 0, replicas)
	for i := int32(0); i < replicas; i++ {
		out = append(out, &enginepb.NodeStats{NodeId: podName(job, i), Capacity: 64})
	}
	return out
}

// newHarness wires a reconciler over a fake API server with the job and its
// StatefulSet already present at the given replica count.
func newHarness(t *testing.T, job *dpev1alpha1.ProcessingJob, replicas int32, engine *fakeEngine) (*ProcessingJobReconciler, client.Client) {
	t.Helper()
	scheme := testScheme(t)

	spec := job.Spec.Defaulted()
	sts := desiredStatefulSet(job, spec, replicas)
	sts.Annotations = map[string]string{specHashAnnotation: specHash(sts)}
	sts.Status.ReadyReplicas = replicas

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(job, sts, desiredService(job)).
		WithStatusSubresource(&dpev1alpha1.ProcessingJob{}).
		Build()

	return &ProcessingJobReconciler{
		Client:    c,
		Scheme:    scheme,
		Engine:    engine,
		scaleDown: newScaleDownTracker(),
		drains:    newDrainTracker(),
	}, c
}

func replicasOnCluster(t *testing.T, c client.Client, job *dpev1alpha1.ProcessingJob) int32 {
	t.Helper()
	var sts appsv1.StatefulSet
	key := types.NamespacedName{Name: workloadName(job), Namespace: job.Namespace}
	if err := c.Get(context.Background(), key, &sts); err != nil {
		t.Fatalf("get statefulset: %v", err)
	}
	return replicasOf(&sts)
}

func reconcileOnce(t *testing.T, r *ProcessingJobReconciler, job *dpev1alpha1.ProcessingJob) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: job.Name, Namespace: job.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return res
}

// TestScaleInDrainsBeforeRemovingAReplica is the safety property of the
// scale-down path: the replica count must not drop until the victim has left
// the registry, because a pod removed while holding checked-out records
// strands them for a full lease.
func TestScaleInDrainsBeforeRemovingAReplica(t *testing.T) {
	job := testJob()
	engine := &fakeEngine{stats: &enginepb.ClusterStats{StreamLag: 0, Nodes: nodesFor(job, 4)}}
	r, c := newHarness(t, job, 4, engine)

	// Backlog is empty, so the desired count is the floor of 1. The first
	// reconcile may only request a drain.
	reconcileOnce(t, r, job)

	if got := replicasOnCluster(t, c, job); got != 4 {
		t.Fatalf("replicas = %d immediately after the drain request; want 4 until the node leaves", got)
	}
	requests := engine.drainRequests()
	if len(requests) != 1 || requests[0] != "records-worker-3" {
		t.Fatalf("drain requests = %v, want exactly [records-worker-3] (the highest ordinal)", requests)
	}

	// While the victim is still registered, nothing moves.
	reconcileOnce(t, r, job)
	if got := replicasOnCluster(t, c, job); got != 4 {
		t.Fatalf("replicas = %d while the victim is still draining; want 4", got)
	}

	// The victim finishes and deregisters.
	engine.removeNode("records-worker-3")
	reconcileOnce(t, r, job)
	if got := replicasOnCluster(t, c, job); got != 3 {
		t.Fatalf("replicas = %d after a clean drain; want 3", got)
	}
}

// TestScaleInStopsAtTheFloor asserts the operator never drains below
// minReplicas, however empty the backlog gets.
func TestScaleInStopsAtTheFloor(t *testing.T) {
	job := testJob()
	engine := &fakeEngine{stats: &enginepb.ClusterStats{StreamLag: 0, Nodes: nodesFor(job, 1)}}
	r, c := newHarness(t, job, 1, engine)

	reconcileOnce(t, r, job)
	if got := replicasOnCluster(t, c, job); got != 1 {
		t.Fatalf("replicas = %d at the floor; want 1", got)
	}
	if requests := engine.drainRequests(); len(requests) != 0 {
		t.Fatalf("drained %v at the floor; want no drain requests", requests)
	}
}

// TestDrainTimeoutLeavesTheReplicaRunning covers the case the design refuses
// to force: a worker that outlives its drain budget keeps its pod, and the
// reclaimer stays the fallback.
func TestDrainTimeoutLeavesTheReplicaRunning(t *testing.T) {
	job := testJob()
	job.Spec.DrainTimeout = &metav1.Duration{Duration: time.Millisecond}
	engine := &fakeEngine{stats: &enginepb.ClusterStats{StreamLag: 0, Nodes: nodesFor(job, 3)}}
	r, c := newHarness(t, job, 3, engine)

	reconcileOnce(t, r, job)
	time.Sleep(5 * time.Millisecond) // outlive the drain budget

	// The victim never leaves the registry; the operator must give up on the
	// scale-in rather than deleting the pod.
	reconcileOnce(t, r, job)
	if got := replicasOnCluster(t, c, job); got != 3 {
		t.Fatalf("replicas = %d after a drain timeout; want 3 — a stuck drain must never force a pod away", got)
	}
	if _, stillDraining := r.drains.current(types.NamespacedName{Name: job.Name, Namespace: job.Namespace}); stillDraining {
		t.Fatal("expired drain attempt was not cleared; the next reconcile must decide afresh")
	}
}

// TestScaleUpIsImmediate asserts backlog is acted on without a window.
func TestScaleUpIsImmediate(t *testing.T) {
	job := testJob()
	engine := &fakeEngine{stats: &enginepb.ClusterStats{StreamLag: 2600, Nodes: nodesFor(job, 2)}}
	r, c := newHarness(t, job, 2, engine)

	reconcileOnce(t, r, job)
	if got := replicasOnCluster(t, c, job); got != 6 {
		t.Fatalf("replicas = %d for a 2600-record backlog at 500/worker; want 6", got)
	}
	if requests := engine.drainRequests(); len(requests) != 0 {
		t.Fatalf("scale-up requested drains %v; scaling out must not drain anything", requests)
	}
}

// TestBacklogRecoveryAbandonsAnInFlightDrain asserts a scale-in in progress is
// abandoned when work arrives, rather than completing and then re-scaling up.
func TestBacklogRecoveryAbandonsAnInFlightDrain(t *testing.T) {
	job := testJob()
	engine := &fakeEngine{stats: &enginepb.ClusterStats{StreamLag: 0, Nodes: nodesFor(job, 4)}}
	r, c := newHarness(t, job, 4, engine)

	reconcileOnce(t, r, job) // requests the drain
	engine.setLag(3000)      // work arrives: desired is now 6

	reconcileOnce(t, r, job)
	if got := replicasOnCluster(t, c, job); got != 6 {
		t.Fatalf("replicas = %d after backlog recovered mid-drain; want 6", got)
	}
	if _, stillDraining := r.drains.current(types.NamespacedName{Name: job.Name, Namespace: job.Namespace}); stillDraining {
		t.Fatal("drain attempt survived the scale-up")
	}
}

// TestCoordinatorUnreachableHoldsTheReplicaCount asserts a control plane
// outage never moves the workload: the workers read Redis directly and are
// unaffected, so the safe action is to report Degraded and change nothing.
func TestCoordinatorUnreachableHoldsTheReplicaCount(t *testing.T) {
	job := testJob()
	engine := &fakeEngine{statsErr: errors.New("connection refused")}
	r, c := newHarness(t, job, 5, engine)

	reconcileOnce(t, r, job)
	if got := replicasOnCluster(t, c, job); got != 5 {
		t.Fatalf("replicas = %d with the coordinator down; want 5 unchanged", got)
	}

	var updated dpev1alpha1.ProcessingJob
	key := types.NamespacedName{Name: job.Name, Namespace: job.Namespace}
	if err := c.Get(context.Background(), key, &updated); err != nil {
		t.Fatalf("get job: %v", err)
	}
	if !hasCondition(updated.Status.Conditions, dpev1alpha1.ConditionDegraded, metav1.ConditionTrue) {
		t.Fatalf("Degraded condition not set; conditions = %+v", updated.Status.Conditions)
	}
}

// TestReconcileCreatesWorkloadAndStatus covers the first reconcile of a new
// job: the Service and StatefulSet appear, and status reflects the cluster.
func TestReconcileCreatesWorkloadAndStatus(t *testing.T) {
	job := testJob()
	scheme := testScheme(t)
	engine := &fakeEngine{stats: &enginepb.ClusterStats{
		StreamLag: 120,
		Pending:   7,
		Nodes: []*enginepb.NodeStats{
			{NodeId: "records-worker-0", Processed: 900, Reclaimed: 3},
		},
	}}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(job).
		WithStatusSubresource(&dpev1alpha1.ProcessingJob{}).
		Build()
	r := &ProcessingJobReconciler{
		Client: c, Scheme: scheme, Engine: engine,
		scaleDown: newScaleDownTracker(), drains: newDrainTracker(),
	}

	reconcileOnce(t, r, job)

	var sts appsv1.StatefulSet
	stsKey := types.NamespacedName{Name: workloadName(job), Namespace: job.Namespace}
	if err := c.Get(context.Background(), stsKey, &sts); err != nil {
		t.Fatalf("statefulset was not created: %v", err)
	}
	if len(sts.OwnerReferences) != 1 || sts.OwnerReferences[0].Kind != "ProcessingJob" {
		t.Fatalf("owner references = %+v, want one pointing at the ProcessingJob", sts.OwnerReferences)
	}

	var updated dpev1alpha1.ProcessingJob
	if err := c.Get(context.Background(), types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, &updated); err != nil {
		t.Fatalf("get job: %v", err)
	}
	if updated.Status.StreamLag != 120 || updated.Status.Pending != 7 {
		t.Fatalf("status lag/pending = %d/%d, want 120/7", updated.Status.StreamLag, updated.Status.Pending)
	}
	if updated.Status.ProcessedTotal != 900 || updated.Status.ReclaimedTotal != 3 {
		t.Fatalf("status totals = %d/%d, want 900/3", updated.Status.ProcessedTotal, updated.Status.ReclaimedTotal)
	}
	if updated.Status.ObservedGeneration != job.Generation {
		t.Fatalf("observedGeneration = %d, want %d", updated.Status.ObservedGeneration, job.Generation)
	}
}

// TestSteadyStateReconcileIsAWriteFreeNoOp guards against the classic
// controller bug: recomputing a defaulted object and updating it every pass.
func TestSteadyStateReconcileIsAWriteFreeNoOp(t *testing.T) {
	job := testJob()
	engine := &fakeEngine{stats: &enginepb.ClusterStats{StreamLag: 400, Nodes: nodesFor(job, 1)}}
	r, c := newHarness(t, job, 1, engine)

	reconcileOnce(t, r, job)

	var first appsv1.StatefulSet
	stsKey := types.NamespacedName{Name: workloadName(job), Namespace: job.Namespace}
	if err := c.Get(context.Background(), stsKey, &first); err != nil {
		t.Fatalf("get statefulset: %v", err)
	}

	reconcileOnce(t, r, job)
	reconcileOnce(t, r, job)

	var last appsv1.StatefulSet
	if err := c.Get(context.Background(), stsKey, &last); err != nil {
		t.Fatalf("get statefulset: %v", err)
	}
	if first.ResourceVersion != last.ResourceVersion {
		t.Fatalf("statefulset was rewritten on a no-op reconcile: %s -> %s",
			first.ResourceVersion, last.ResourceVersion)
	}
}

func hasCondition(conds []metav1.Condition, condType string, status metav1.ConditionStatus) bool {
	for _, c := range conds {
		if c.Type == condType {
			return c.Status == status
		}
	}
	return false
}
