package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PlacementPolicy selects how worker pods are spread over nodes.
type PlacementPolicy string

const (
	// PlacementBinPack packs workers onto as few nodes as possible. It is the
	// right policy when a worker requests a GPU: leaving whole nodes empty is
	// what lets a later, larger job schedule at all. The packing itself is
	// done by kube-scheduler, not by this operator — see SchedulerName.
	PlacementBinPack PlacementPolicy = "BinPack"

	// PlacementSpread distributes workers across nodes so that losing one node
	// costs a bounded fraction of the consumer group. This is the default: the
	// engine's recovery path is cheap but not free, and a spread group keeps
	// each reclaim sweep small.
	PlacementSpread PlacementPolicy = "Spread"
)

// Condition types reported on ProcessingJob.Status.Conditions.
const (
	// ConditionReady is true when every desired worker replica is ready and
	// heartbeating into the node registry.
	ConditionReady = "Ready"
	// ConditionProgressing is true while the operator is still converging the
	// StatefulSet on the desired replica count, including during a drain.
	ConditionProgressing = "Progressing"
	// ConditionDegraded is true when the operator cannot observe or converge
	// the job: the coordinator is unreachable, or the StatefulSet is wedged.
	ConditionDegraded = "Degraded"
)

// AutoscaleSpec drives replica count from consumer-group backlog rather than
// from CPU. Stream lag is the only signal that is proportional to unfinished
// work; a saturated worker sits at the same CPU whether the backlog is ten
// records or ten million.
type AutoscaleSpec struct {
	// MinReplicas is the floor the operator will not scale below, so a job
	// keeps a consumer in the group even at zero backlog.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	// +optional
	MinReplicas int32 `json:"minReplicas,omitempty"`

	// MaxReplicas is the ceiling. It exists to bound Redis connection count
	// and cluster cost, not because the engine has a scaling limit.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=16
	// +optional
	MaxReplicas int32 `json:"maxReplicas,omitempty"`

	// TargetLagPerWorker is how many backlog entries one worker is expected to
	// absorb. Desired replicas is ceil(streamLag / targetLagPerWorker), so this
	// value sets both the aggressiveness of scale-up and the steady-state
	// backlog the job is willing to carry.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=500
	// +optional
	TargetLagPerWorker int64 `json:"targetLagPerWorker,omitempty"`

	// StabilizationWindow is how long a lower desired replica count must hold
	// continuously before the operator scales in. Scale-up is immediate;
	// scale-down is damped because it costs a drain, and a backlog that
	// oscillates around the threshold would otherwise drain a worker every
	// reconcile.
	// +kubebuilder:default="60s"
	// +optional
	StabilizationWindow *metav1.Duration `json:"stabilizationWindow,omitempty"`
}

// PlacementSpec controls where worker pods land.
//
// The operator does not make placement decisions itself. It emits the
// scheduling inputs — topology spread constraints, and optionally the name of
// a scheduler profile configured for bin-packing — and kube-scheduler decides.
type PlacementSpec struct {
	// Policy selects spread (default) or bin-pack intent.
	// +kubebuilder:validation:Enum=BinPack;Spread
	// +kubebuilder:default=Spread
	// +optional
	Policy PlacementPolicy `json:"policy,omitempty"`

	// SchedulerName routes pods to a non-default scheduler profile. Set it to
	// the profile shipped in deploy/scheduler when Policy is BinPack; the
	// default scheduler's LeastAllocated scoring will otherwise spread pods
	// regardless of Policy.
	// +optional
	SchedulerName string `json:"schedulerName,omitempty"`
}

// ProcessingJobSpec is the desired state of one consumer group.
type ProcessingJobSpec struct {
	// Stream is the Redis stream key workers read from.
	// +kubebuilder:default="dpe:records"
	// +optional
	Stream string `json:"stream,omitempty"`

	// Group is the Redis consumer group name. Every worker pod joins it under
	// its own stable pod name.
	// +kubebuilder:default="dpe-workers"
	// +optional
	Group string `json:"group,omitempty"`

	// RedisAddr is the host:port of the Redis instance holding the stream, the
	// idempotency keys and the node registry.
	// +kubebuilder:validation:MinLength=1
	RedisAddr string `json:"redisAddr"`

	// Image is the worker image. It must contain the worker binary at
	// /usr/local/bin/worker, which is what the repository Dockerfile builds.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// CoordinatorAddr is the host:port of the coordinator's gRPC control
	// plane. The operator dials GetClusterStats there for backlog and per-node
	// progress instead of reading Redis itself. Defaults to
	// dpe-coordinator.<namespace>.svc:9090 when empty.
	// +optional
	CoordinatorAddr string `json:"coordinatorAddr,omitempty"`

	// Concurrency is each worker's bounded in-flight window. It is the pool
	// size, and therefore also the backpressure threshold.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=64
	// +optional
	Concurrency int32 `json:"concurrency,omitempty"`

	// MinIdle is the lease length: how long an entry may sit unacked before
	// another worker may reclaim it. Lower recovers faster from a lost pod;
	// higher wastes less duplicate work on a slow-but-alive one.
	// +kubebuilder:default="3s"
	// +optional
	MinIdle *metav1.Duration `json:"minIdle,omitempty"`

	// DrainTimeout is how long a worker may spend finishing in-flight records
	// after SIGTERM. It also sets the pod's termination grace period, with a
	// small margin so the kubelet does not SIGKILL a drain that is still
	// inside its own budget.
	// +kubebuilder:default="30s"
	// +optional
	DrainTimeout *metav1.Duration `json:"drainTimeout,omitempty"`

	// Autoscale drives replica count from stream lag.
	// +optional
	Autoscale AutoscaleSpec `json:"autoscale,omitempty"`

	// Resources is passed through verbatim to the worker container. This is
	// where nvidia.com/gpu limits are set; the operator does not interpret
	// them, it only makes sure they reach the pod spec so the scheduler and
	// the device plugin can act on them.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Placement controls scheduling inputs for the worker pods.
	// +optional
	Placement PlacementSpec `json:"placement,omitempty"`
}

// ProcessingJobStatus is the observed state of a consumer group.
type ProcessingJobStatus struct {
	// Replicas is the replica count currently requested on the StatefulSet.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is how many worker pods are ready.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// StreamLag is the number of entries never delivered to any consumer, as
	// reported by the coordinator. It is the autoscaling input.
	// +optional
	StreamLag int64 `json:"streamLag,omitempty"`

	// Pending is the size of the group's pending entries list: records
	// delivered but not yet acked, i.e. in flight cluster-wide.
	// +optional
	Pending int64 `json:"pending,omitempty"`

	// ProcessedTotal is the sum of per-node processed counters. It resets when
	// every worker pod is replaced, so treat it as a progress signal rather
	// than an accounting total.
	// +optional
	ProcessedTotal int64 `json:"processedTotal,omitempty"`

	// ReclaimedTotal is the sum of per-node reclaim counters: records taken
	// over from a lease that expired, i.e. work recovered from a lost pod.
	// +optional
	ReclaimedTotal int64 `json:"reclaimedTotal,omitempty"`

	// Draining lists node IDs the operator has asked to drain and that have
	// not yet left the registry. It is non-empty only during a scale-in.
	// +optional
	Draining []string `json:"draining,omitempty"`

	// ObservedGeneration is the spec generation this status was computed from.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions carries Ready, Progressing and Degraded.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=pj,categories=dpe
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Lag",type=integer,JSONPath=`.status.streamLag`
// +kubebuilder:printcolumn:name="Pending",type=integer,JSONPath=`.status.pending`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ProcessingJob is one consumer group of the distributed processing engine,
// managed as a StatefulSet of workers whose replica count follows stream lag.
type ProcessingJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ProcessingJobSpec   `json:"spec,omitempty"`
	Status ProcessingJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ProcessingJobList is a list of ProcessingJob.
type ProcessingJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ProcessingJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ProcessingJob{}, &ProcessingJobList{})
}

// Defaulted returns a copy of the spec with every optional field resolved to
// the value the CRD schema would have applied. The controller works from this
// copy so its behaviour does not depend on whether the object arrived through
// the API server (defaulted) or was constructed in a test (not).
func (s ProcessingJobSpec) Defaulted() ProcessingJobSpec {
	out := *s.DeepCopy()
	if out.Stream == "" {
		out.Stream = "dpe:records"
	}
	if out.Group == "" {
		out.Group = "dpe-workers"
	}
	if out.Concurrency <= 0 {
		out.Concurrency = 64
	}
	if out.MinIdle == nil || out.MinIdle.Duration <= 0 {
		out.MinIdle = &metav1.Duration{Duration: defaultMinIdle}
	}
	if out.DrainTimeout == nil || out.DrainTimeout.Duration <= 0 {
		out.DrainTimeout = &metav1.Duration{Duration: defaultDrainTimeout}
	}
	if out.Autoscale.MaxReplicas <= 0 {
		out.Autoscale.MaxReplicas = defaultMaxReplicas
	}
	if out.Autoscale.MinReplicas < 0 {
		out.Autoscale.MinReplicas = 0
	}
	if out.Autoscale.MinReplicas > out.Autoscale.MaxReplicas {
		out.Autoscale.MinReplicas = out.Autoscale.MaxReplicas
	}
	if out.Autoscale.TargetLagPerWorker <= 0 {
		out.Autoscale.TargetLagPerWorker = defaultTargetLagPerWorker
	}
	if out.Autoscale.StabilizationWindow == nil || out.Autoscale.StabilizationWindow.Duration < 0 {
		out.Autoscale.StabilizationWindow = &metav1.Duration{Duration: defaultStabilizationWindow}
	}
	if out.Placement.Policy == "" {
		out.Placement.Policy = PlacementSpread
	}
	return out
}
