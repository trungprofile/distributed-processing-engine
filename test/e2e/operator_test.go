//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	enginepb "github.com/trungprofile/distributed-processing-engine/api/proto"
	dpev1alpha1 "github.com/trungprofile/distributed-processing-engine/api/v1alpha1"
)

// TestReconcileCreatesReadyWorkers is the smoke test: applying a
// ProcessingJob produces a StatefulSet built from its spec, and the job
// reaches Ready once the workers are.
func TestReconcileCreatesReadyWorkers(t *testing.T) {
	h := newHarness(t)
	job := h.newJob(t, "reconcile", nil)

	h.waitForReady(t, job, 1, 3*time.Minute)

	sts := h.statefulSet(t, job)
	if sts.Spec.ServiceName != job.Name+"-workers" {
		t.Errorf("serviceName = %q, want the headless service", sts.Spec.ServiceName)
	}
	if len(sts.OwnerReferences) != 1 || sts.OwnerReferences[0].Name != job.Name {
		t.Errorf("owner references = %+v, want one pointing at the ProcessingJob", sts.OwnerReferences)
	}

	container := sts.Spec.Template.Spec.Containers[0]
	wantArgs := map[string]bool{
		"-stream=" + job.Spec.Stream:   false,
		"-group=" + job.Spec.Group:     false,
		"-concurrency=16":              false,
		"-min-idle=2s":                 false,
		"-drain-timeout=20s":           false,
		"-redis=" + job.Spec.RedisAddr: false,
	}
	for _, a := range container.Args {
		if _, ok := wantArgs[a]; ok {
			wantArgs[a] = true
		}
	}
	for arg, seen := range wantArgs {
		if !seen {
			t.Errorf("worker args missing %q; got %v", arg, container.Args)
		}
	}

	// terminationGracePeriodSeconds must exceed the drain budget, or the
	// kubelet kills a worker that is still finishing its records.
	if got := sts.Spec.Template.Spec.TerminationGracePeriodSeconds; got == nil || *got != 25 {
		t.Errorf("terminationGracePeriodSeconds = %v, want 25 (20s drain + 5s slack)", got)
	}

	if updated := h.job(t, job); !conditionIs(updated, dpev1alpha1.ConditionReady, metav1.ConditionTrue) {
		t.Errorf("Ready condition is not true; conditions = %+v", updated.Status.Conditions)
	}
}

// TestScaleUpOnLag asserts the autoscaler acts on backlog: a large batch
// should grow the worker set and then drain the lag back down.
func TestScaleUpOnLag(t *testing.T) {
	h := newHarness(t)
	job := h.newJob(t, "scaleup", nil)
	h.waitForReady(t, job, 1, 3*time.Minute)

	// 12000 records at 500 per worker asks for well past maxReplicas, so the
	// assertion is on the ceiling being reached, not on an exact count.
	keys := h.submit(t, job, 12000)

	waitFor(t, 3*time.Minute, "the worker set to grow towards maxReplicas", func() bool {
		return h.job(t, job).Status.Replicas >= 4
	})
	waitFor(t, 3*time.Minute, "the backlog to be worked off", func() bool {
		return h.job(t, job).Status.StreamLag == 0
	})

	sink := h.sinkKeys(t)
	var missing int
	for _, k := range keys {
		if _, ok := sink[k]; !ok {
			missing++
		}
	}
	if missing != 0 {
		t.Fatalf("%d of %d records never reached the sink", missing, len(keys))
	}
}

// TestPodEvictionRecovery is the engine's core guarantee, re-asserted through
// Kubernetes: deleting a worker pod mid-flight must cost no records, and the
// entries it was holding must be reclaimed rather than lost.
func TestPodEvictionRecovery(t *testing.T) {
	h := newHarness(t)
	job := h.newJob(t, "eviction", func(j *dpev1alpha1.ProcessingJob) {
		j.Spec.Autoscale.MinReplicas = 3
	})
	h.waitForReady(t, job, 3, 3*time.Minute)

	keys := h.submit(t, job, 8000)

	// Wait until the cluster is genuinely mid-flight; killing an idle pod
	// proves nothing.
	waitFor(t, 2*time.Minute, "records to be in flight", func() bool {
		return h.stats(t).GetPending() > 0
	})

	reclaimedBefore := totalReclaimed(h.stats(t))
	pods := h.workerPods(t, job)
	if len(pods) == 0 {
		t.Fatal("no worker pods found to evict")
	}
	victim := pods[0]
	deletedAt := time.Now()
	if err := h.k8s.Delete(context.Background(), &victim,
		client.GracePeriodSeconds(0)); err != nil {
		t.Fatalf("delete pod %s: %v", victim.Name, err)
	}

	// Its entries stay in the pending list until a peer's lease sweep takes
	// them. That sweep is the recovery path being measured here.
	waitFor(t, 3*time.Minute, "a surviving worker to reclaim the evicted pod's entries", func() bool {
		return totalReclaimed(h.stats(t)) > reclaimedBefore
	})
	t.Logf("first reclaim observed %s after the pod was deleted",
		time.Since(deletedAt).Round(time.Millisecond))

	waitFor(t, 5*time.Minute, "every record to reach the sink", func() bool {
		return h.stats(t).GetStreamLag() == 0 && h.stats(t).GetPending() == 0
	})

	sink := h.sinkKeys(t)
	for _, k := range keys {
		if _, ok := sink[k]; !ok {
			t.Fatalf("record %s was lost when pod %s was evicted", k, victim.Name)
		}
	}
}

// TestSafeScaleDown asserts the operator drains before it removes: no worker
// pod may disappear while it still holds checked-out records, and the sink
// must be complete when the dust settles.
func TestSafeScaleDown(t *testing.T) {
	h := newHarness(t)
	job := h.newJob(t, "scaledown", func(j *dpev1alpha1.ProcessingJob) {
		j.Spec.Autoscale.MinReplicas = 1
		j.Spec.Autoscale.StabilizationWindow = &metav1.Duration{Duration: 10 * time.Second}
	})
	h.waitForReady(t, job, 1, 3*time.Minute)

	keys := h.submit(t, job, 6000)
	waitFor(t, 3*time.Minute, "the worker set to grow", func() bool {
		return h.job(t, job).Status.Replicas >= 3
	})

	// Load stops here. Once the backlog clears, the desired count falls back
	// to the floor and the operator must drain its way down to it.
	waitFor(t, 3*time.Minute, "the backlog to clear", func() bool {
		return h.job(t, job).Status.StreamLag == 0
	})
	waitFor(t, 5*time.Minute, "the worker set to scale back to the floor", func() bool {
		return h.job(t, job).Status.Replicas == 1
	})

	// Nothing may be stranded: a forced scale-in would leave entries checked
	// out to a consumer that no longer exists.
	stats := h.stats(t)
	if stats.GetPending() != 0 {
		t.Errorf("scale-in left %d entries pending; the drain did not complete", stats.GetPending())
	}

	sink := h.sinkKeys(t)
	for _, k := range keys {
		if _, ok := sink[k]; !ok {
			t.Fatalf("record %s was lost during scale-in", k)
		}
	}
}

// TestGPUResourcePassThrough asserts an extended resource declared on the
// ProcessingJob reaches the pod spec unchanged.
//
// The assertion is on the rendered pod spec, not on GPU execution: a kind
// cluster has no GPUs and no device plugin, so the pod stays Pending. That is
// the correct scope — the operator's job is to pass the limit through, and
// whether a GPU exists to satisfy it is the cluster's business.
func TestGPUResourcePassThrough(t *testing.T) {
	h := newHarness(t)
	job := h.newJob(t, "gpu", func(j *dpev1alpha1.ProcessingJob) {
		j.Spec.Placement.Policy = dpev1alpha1.PlacementBinPack
		j.Spec.Placement.SchedulerName = "dpe-binpack"
		j.Spec.Resources = corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				"nvidia.com/gpu":      resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
		}
	})

	waitFor(t, 2*time.Minute, "the StatefulSet to be created", func() bool {
		var sts appsv1.StatefulSet
		key := types.NamespacedName{Name: job.Name + "-worker", Namespace: job.Namespace}
		return h.k8s.Get(context.Background(), key, &sts) == nil
	})

	pod := h.statefulSet(t, job).Spec.Template.Spec
	gpu, ok := pod.Containers[0].Resources.Limits["nvidia.com/gpu"]
	if !ok {
		t.Fatalf("nvidia.com/gpu missing from the rendered pod spec: %+v", pod.Containers[0].Resources)
	}
	if gpu.Value() != 1 {
		t.Errorf("nvidia.com/gpu = %s, want 1", gpu.String())
	}
	if pod.SchedulerName != "dpe-binpack" {
		t.Errorf("schedulerName = %q, want dpe-binpack", pod.SchedulerName)
	}
	if len(pod.TopologySpreadConstraints) != 0 {
		t.Errorf("BinPack emitted %d spread constraints, want none", len(pod.TopologySpreadConstraints))
	}
}

func totalReclaimed(stats *enginepb.ClusterStats) int64 {
	var total int64
	for _, n := range stats.GetNodes() {
		total += n.GetReclaimed()
	}
	return total
}
