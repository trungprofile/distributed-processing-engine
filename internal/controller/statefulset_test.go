package controller

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dpev1alpha1 "github.com/trungprofile/distributed-processing-engine/api/v1alpha1"
)

// gpuResource is the extended resource name a GPU device plugin advertises.
const gpuResource corev1.ResourceName = "nvidia.com/gpu"

func buildStatefulSet(t *testing.T, mutate func(*dpev1alpha1.ProcessingJob)) (*dpev1alpha1.ProcessingJob, corev1.PodSpec) {
	t.Helper()
	job := testJob()
	if mutate != nil {
		mutate(job)
	}
	spec := job.Spec.Defaulted()
	return job, desiredStatefulSet(job, spec, 3).Spec.Template.Spec
}

// TestGPULimitsReachThePodSpec is the whole of the operator's GPU story: the
// limit is passed through untouched so the scheduler and the device plugin
// can act on it.
func TestGPULimitsReachThePodSpec(t *testing.T) {
	_, pod := buildStatefulSet(t, func(job *dpev1alpha1.ProcessingJob) {
		job.Spec.Resources = corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				gpuResource:           resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("4Gi"),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("4Gi"),
			},
		}
	})

	limits := pod.Containers[0].Resources.Limits
	gpu, ok := limits[gpuResource]
	if !ok {
		t.Fatalf("nvidia.com/gpu missing from the container limits: %+v", limits)
	}
	if gpu.Value() != 1 {
		t.Fatalf("nvidia.com/gpu = %s, want 1", gpu.String())
	}
	if got := pod.Containers[0].Resources.Requests.Cpu(); got.Value() != 2 {
		t.Fatalf("cpu request = %s, want 2", got.String())
	}
}

// TestResourcesAreCopiedNotAliased guards against the operator handing the
// cached ProcessingJob's own resource maps to the pod template, where a later
// mutation would corrupt the informer cache.
func TestResourcesAreCopiedNotAliased(t *testing.T) {
	job := testJob()
	job.Spec.Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{gpuResource: resource.MustParse("1")},
	}
	spec := job.Spec.Defaulted()
	sts := desiredStatefulSet(job, spec, 1)

	sts.Spec.Template.Spec.Containers[0].Resources.Limits[gpuResource] = resource.MustParse("8")

	if got := job.Spec.Resources.Limits[gpuResource]; got.Value() != 1 {
		t.Fatalf("mutating the pod template changed the job spec: gpu = %s", got.String())
	}
}

func TestSpreadPolicyEmitsTopologyConstraints(t *testing.T) {
	job, pod := buildStatefulSet(t, func(job *dpev1alpha1.ProcessingJob) {
		job.Spec.Placement.Policy = dpev1alpha1.PlacementSpread
	})

	if len(pod.TopologySpreadConstraints) != 2 {
		t.Fatalf("got %d spread constraints, want hostname and zone", len(pod.TopologySpreadConstraints))
	}
	for _, c := range pod.TopologySpreadConstraints {
		if c.WhenUnsatisfiable != corev1.ScheduleAnyway {
			t.Fatalf("constraint on %s is %s; spread must never block scheduling",
				c.TopologyKey, c.WhenUnsatisfiable)
		}
		if got := c.LabelSelector.MatchLabels["dpe.trungprofile.dev/processing-job"]; got != job.Name {
			t.Fatalf("constraint selects %q, want only this job's workers", got)
		}
	}
}

// TestBinPackPolicyEmitsNoSpreadConstraints asserts the two policies are
// genuinely different: bin-packing must not carry anti-affinity that would
// fight the scheduler profile doing the packing.
func TestBinPackPolicyEmitsNoSpreadConstraints(t *testing.T) {
	_, pod := buildStatefulSet(t, func(job *dpev1alpha1.ProcessingJob) {
		job.Spec.Placement.Policy = dpev1alpha1.PlacementBinPack
		job.Spec.Placement.SchedulerName = "dpe-binpack"
	})

	if len(pod.TopologySpreadConstraints) != 0 {
		t.Fatalf("BinPack emitted %d spread constraints, want none", len(pod.TopologySpreadConstraints))
	}
	if pod.SchedulerName != "dpe-binpack" {
		t.Fatalf("schedulerName = %q, want dpe-binpack", pod.SchedulerName)
	}
}

// TestDefaultSchedulerWhenUnset asserts the operator leaves schedulerName
// empty rather than inventing one, so pods go to the cluster default.
func TestDefaultSchedulerWhenUnset(t *testing.T) {
	_, pod := buildStatefulSet(t, nil)
	if pod.SchedulerName != "" {
		t.Fatalf("schedulerName = %q, want empty so the default scheduler is used", pod.SchedulerName)
	}
}

// TestGracePeriodExceedsDrainTimeout is the property that makes a graceful
// drain actually complete: the kubelet must not SIGKILL a worker that is still
// inside its own drain budget.
func TestGracePeriodExceedsDrainTimeout(t *testing.T) {
	_, pod := buildStatefulSet(t, func(job *dpev1alpha1.ProcessingJob) {
		job.Spec.DrainTimeout = &metav1.Duration{Duration: 45 * time.Second}
	})

	if pod.TerminationGracePeriodSeconds == nil {
		t.Fatal("terminationGracePeriodSeconds is unset")
	}
	if got := *pod.TerminationGracePeriodSeconds; got != 50 {
		t.Fatalf("terminationGracePeriodSeconds = %d, want 50 (drainTimeout + 5s slack)", got)
	}
}

func TestWorkerArgsCarryEveryTunable(t *testing.T) {
	job := testJob()
	job.Spec.Stream = "orders"
	job.Spec.Group = "order-workers"
	job.Spec.Concurrency = 32
	job.Spec.MinIdle = &metav1.Duration{Duration: 7 * time.Second}
	spec := job.Spec.Defaulted()

	args := strings.Join(workerArgs(spec), " ")
	for _, want := range []string{
		"-redis=redis:6379",
		"-stream=orders",
		"-group=order-workers",
		"-concurrency=32",
		"-min-idle=7s",
		"-drain-timeout=30s",
		"-metrics-addr=:9100",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("worker args missing %q; got %s", want, args)
		}
	}
}

// TestSpecHashIgnoresReplicaCount keeps a scale event from reading as a
// template change, which would roll every pod on every scale.
func TestSpecHashIgnoresReplicaCount(t *testing.T) {
	job := testJob()
	spec := job.Spec.Defaulted()

	small := specHash(desiredStatefulSet(job, spec, 1))
	large := specHash(desiredStatefulSet(job, spec, 12))
	if small != large {
		t.Fatal("spec hash changed with replica count; scaling would trigger a pod roll")
	}

	spec.Concurrency = 128
	if changed := specHash(desiredStatefulSet(job, spec, 1)); changed == small {
		t.Fatal("spec hash did not change when the worker args did")
	}
}

// TestPodIdentityIsStable pins the naming contract the whole design rests on:
// the pod name is the consumer name in the Redis group, and the highest
// ordinal is the pod a scale-in removes.
func TestPodIdentityIsStable(t *testing.T) {
	job := testJob()
	if got := workloadName(job); got != "records-worker" {
		t.Fatalf("workload name = %q, want records-worker", got)
	}
	if got := podName(job, 3); got != "records-worker-3" {
		t.Fatalf("pod name = %q, want records-worker-3", got)
	}
	if got := victimNode(job, 4); got != "records-worker-3" {
		t.Fatalf("scale-in victim at 4 replicas = %q, want records-worker-3", got)
	}
}
