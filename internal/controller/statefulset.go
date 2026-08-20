package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	dpev1alpha1 "github.com/trungprofile/distributed-processing-engine/api/v1alpha1"
)

const (
	// metricsPort is where the worker serves /metrics and /healthz. It is
	// fixed in cmd/worker's default flags and in the Dockerfile's EXPOSE.
	metricsPort = 9100

	// specHashAnnotation records a digest of the pod template the operator
	// last wrote. The API server defaults dozens of fields inside a pod spec,
	// so comparing the live object against a freshly built one always differs;
	// comparing digests of what *we* asked for does not, and keeps the
	// controller from rewriting the StatefulSet on every reconcile.
	specHashAnnotation = "dpe.trungprofile.dev/spec-hash"

	// gracePeriodSlack is added to drainTimeout when setting the pod's
	// termination grace period, so the kubelet never SIGKILLs a drain that is
	// still inside its own budget.
	gracePeriodSlack = 5 * time.Second
)

// workloadName is the StatefulSet (and therefore pod-name prefix) for a job.
// Worker pods take their node ID from the hostname, so this prefix is also the
// consumer-name prefix inside the Redis consumer group.
func workloadName(job *dpev1alpha1.ProcessingJob) string {
	return job.Name + "-worker"
}

// serviceName is the headless Service that gives the StatefulSet stable DNS.
func serviceName(job *dpev1alpha1.ProcessingJob) string {
	return job.Name + "-workers"
}

// podName is the name of the pod at the given ordinal, which is also the
// node ID that pod registers under.
func podName(job *dpev1alpha1.ProcessingJob, ordinal int32) string {
	return fmt.Sprintf("%s-%d", workloadName(job), ordinal)
}

// selectorLabels identify the pods belonging to one ProcessingJob. They are
// immutable on a StatefulSet, so nothing derived from spec may appear here.
func selectorLabels(job *dpev1alpha1.ProcessingJob) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":              "dpe-worker",
		"app.kubernetes.io/instance":          job.Name,
		"app.kubernetes.io/component":         "worker",
		"dpe.trungprofile.dev/processing-job": job.Name,
	}
}

func podLabels(job *dpev1alpha1.ProcessingJob) map[string]string {
	labels := selectorLabels(job)
	labels["app.kubernetes.io/managed-by"] = "dpe-operator"
	return labels
}

// desiredService builds the headless Service that governs the StatefulSet.
// It carries no cluster IP: its only job is per-pod DNS and endpoint
// membership, which is also what makes the worker pods scrapeable by name.
func desiredService(job *dpev1alpha1.ProcessingJob) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName(job),
			Namespace: job.Namespace,
			Labels:    podLabels(job),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  selectorLabels(job),
			Ports: []corev1.ServicePort{{
				Name:       "metrics",
				Port:       metricsPort,
				TargetPort: intstr.FromInt32(metricsPort),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
}

// workerArgs renders the worker command line from the spec. Everything the
// worker needs is a flag, so the pod template is the whole configuration
// surface and there is no second copy of it in a ConfigMap to drift.
func workerArgs(spec dpev1alpha1.ProcessingJobSpec) []string {
	return []string{
		"-redis=" + spec.RedisAddr,
		"-stream=" + spec.Stream,
		"-group=" + spec.Group,
		"-concurrency=" + strconv.Itoa(int(spec.Concurrency)),
		"-min-idle=" + spec.MinIdle.Duration.String(),
		"-drain-timeout=" + spec.DrainTimeout.Duration.String(),
		"-metrics-addr=:" + strconv.Itoa(metricsPort),
	}
}

// desiredStatefulSet builds the workload for a job at a given replica count.
//
// A StatefulSet rather than a Deployment is the load-bearing choice here: a
// consumer group member is identified by name, and a stable name is what lets
// a restarted pod rejoin as the same consumer and find its own pending entries
// rather than leaving them for a reclaim sweep. See docs/DESIGN.md.
func desiredStatefulSet(job *dpev1alpha1.ProcessingJob, spec dpev1alpha1.ProcessingJobSpec, replicas int32) *appsv1.StatefulSet {
	grace := int64((spec.DrainTimeout.Duration + gracePeriodSlack).Seconds())

	pod := corev1.PodSpec{
		TerminationGracePeriodSeconds: &grace,
		Containers: []corev1.Container{{
			Name:            "worker",
			Image:           spec.Image,
			Command:         []string{"/usr/local/bin/worker"},
			Args:            workerArgs(spec),
			ImagePullPolicy: corev1.PullIfNotPresent,
			Ports: []corev1.ContainerPort{{
				Name:          "metrics",
				ContainerPort: metricsPort,
				Protocol:      corev1.ProtocolTCP,
			}},
			// The worker takes its node ID from the hostname, which the
			// kubelet sets to the pod name. Passing it explicitly as well
			// makes the identity visible in `kubectl describe` and survives
			// any future change to that kubelet behaviour.
			Env: []corev1.EnvVar{{
				Name: "NODE_ID",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
				},
			}},
			ReadinessProbe: httpProbe(3, 2, 3),
			// Liveness polls far more slowly than readiness: a worker that is
			// merely saturated must be taken out of service, not restarted,
			// because restarting it strands its in-flight records for a full
			// lease.
			LivenessProbe: httpProbe(15, 10, 6),
		}},
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      workloadName(job),
			Namespace: job.Namespace,
			Labels:    podLabels(job),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: serviceName(job),
			Selector:    &metav1.LabelSelector{MatchLabels: selectorLabels(job)},
			// Consumer-group members are independent: pod N does not need pod
			// N-1 to be ready before it can read. Parallel management is what
			// makes a scale-up from 2 to 12 arrive in one pod-start latency
			// instead of ten of them in series.
			PodManagementPolicy: appsv1.ParallelPodManagement,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels(job),
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/port":   strconv.Itoa(metricsPort),
						"prometheus.io/path":   "/metrics",
					},
				},
				Spec: pod,
			},
		},
	}
}

// httpProbe builds a GET /healthz probe against the worker's metrics port.
func httpProbe(period, initialDelay, failureThreshold int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/healthz",
				Port: intstr.FromInt32(metricsPort),
			},
		},
		InitialDelaySeconds: initialDelay,
		PeriodSeconds:       period,
		TimeoutSeconds:      2,
		FailureThreshold:    failureThreshold,
	}
}

// specHash digests the parts of a StatefulSet the operator owns, excluding
// replicas: replica count is driven by the autoscaler and by the drain
// protocol, so a scale event must not read as a template change.
func specHash(sts *appsv1.StatefulSet) string {
	subject := struct {
		Template            corev1.PodTemplateSpec `json:"template"`
		ServiceName         string                 `json:"serviceName"`
		PodManagementPolicy string                 `json:"podManagementPolicy"`
	}{
		Template:            sts.Spec.Template,
		ServiceName:         sts.Spec.ServiceName,
		PodManagementPolicy: string(sts.Spec.PodManagementPolicy),
	}
	blob, err := json.Marshal(subject)
	if err != nil {
		// Marshalling a pod template cannot fail in practice; degrade to a
		// hash that never matches so the reconcile writes rather than skips.
		return "unhashable"
	}
	sum := sha256.Sum256(blob)
	return hex.EncodeToString(sum[:])
}
