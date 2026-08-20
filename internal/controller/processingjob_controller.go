// Package controller holds the Kubernetes reconciler for the ProcessingJob
// API. It owns one StatefulSet of workers per job and keeps its replica count,
// pod template and status in step with the consumer group the engine is
// actually running.
//
// The reconciler is deliberately the only writer of that StatefulSet's replica
// count: scaling in requires draining a worker first, and a second autoscaler
// acting on the same object would delete pods holding in-flight records.
package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	enginepb "github.com/trungprofile/distributed-processing-engine/api/proto"
	dpev1alpha1 "github.com/trungprofile/distributed-processing-engine/api/v1alpha1"
)

// requeueInterval is the steady-state reconcile period. Stream lag is a
// polled signal, so this is also the autoscaler's sampling rate: fast enough
// that a backlog spike is acted on within a few seconds, slow enough that a
// hundred jobs do not flood one coordinator with stats calls.
const requeueInterval = 5 * time.Second

// ProcessingJobReconciler reconciles a ProcessingJob into a StatefulSet of
// workers plus the headless Service that governs it.
type ProcessingJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Engine reads cluster state and requests drains through the
	// coordinator's gRPC control plane.
	Engine EngineClient
}

// +kubebuilder:rbac:groups=dpe.trungprofile.dev,resources=processingjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dpe.trungprofile.dev,resources=processingjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dpe.trungprofile.dev,resources=processingjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets/status,verbs=get
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives one ProcessingJob towards its spec.
//
// The loop is: read the job, converge the Service and StatefulSet template,
// ask the coordinator what the cluster is actually doing, and write that back
// to status. Replica count is left alone here — it is owned by the autoscaling
// and drain paths added on top of this loop.
func (r *ProcessingJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var job dpev1alpha1.ProcessingJob
	if err := r.Get(ctx, req.NamespacedName, &job); err != nil {
		// Not found means deleted. The Service and StatefulSet carry owner
		// references back to the job, so garbage collection has already
		// removed them; there is nothing left to do.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	spec := job.Spec.Defaulted()

	if err := r.ensureService(ctx, &job); err != nil {
		return ctrl.Result{}, err
	}

	sts, err := r.ensureStatefulSet(ctx, &job, spec)
	if err != nil {
		return ctrl.Result{}, err
	}

	stats, statsErr := r.Engine.ClusterStats(ctx, coordinatorAddr(&job, spec))
	if statsErr != nil {
		// A coordinator that cannot be reached is a real degradation, but not
		// a reason to touch the workload: the workers are reading Redis
		// directly and are unaffected by the control plane being down.
		logger.Info("coordinator unreachable, leaving replica count unchanged", "error", statsErr)
	}

	if err := r.updateStatus(ctx, &job, sts, stats, statsErr); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// coordinatorAddr resolves where to reach the control plane. The convention
// matches what the Helm chart installs, so a ProcessingJob in the same
// namespace as the chart release needs no explicit address.
func coordinatorAddr(job *dpev1alpha1.ProcessingJob, spec dpev1alpha1.ProcessingJobSpec) string {
	if spec.CoordinatorAddr != "" {
		return spec.CoordinatorAddr
	}
	return fmt.Sprintf("dpe-coordinator.%s.svc:9090", job.Namespace)
}

// ensureService creates or updates the headless Service backing the
// StatefulSet. Its spec never changes after creation, so the update path only
// exists to repair manual edits.
func (r *ProcessingJobReconciler) ensureService(ctx context.Context, job *dpev1alpha1.ProcessingJob) error {
	desired := desiredService(job)
	if err := controllerutil.SetControllerReference(job, desired, r.Scheme); err != nil {
		return fmt.Errorf("set owner on service: %w", err)
	}

	var current corev1.Service
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &current)
	switch {
	case apierrors.IsNotFound(err):
		if err := r.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create service %s: %w", desired.Name, err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("get service %s: %w", desired.Name, err)
	}

	// ClusterIP and several port fields are immutable or defaulted by the API
	// server; carry them forward and only reassert what the operator owns.
	patched := current.DeepCopy()
	patched.Labels = desired.Labels
	patched.Spec.Selector = desired.Spec.Selector
	patched.Spec.Ports = desired.Spec.Ports
	patched.OwnerReferences = desired.OwnerReferences
	if equalService(&current, patched) {
		return nil
	}
	if err := r.Update(ctx, patched); err != nil {
		return fmt.Errorf("update service %s: %w", desired.Name, err)
	}
	return nil
}

// ensureStatefulSet creates the workload if it is missing and repairs its pod
// template if it has drifted from the spec. Replica count is set only at
// creation; afterwards it belongs to the scaling paths.
func (r *ProcessingJobReconciler) ensureStatefulSet(
	ctx context.Context,
	job *dpev1alpha1.ProcessingJob,
	spec dpev1alpha1.ProcessingJobSpec,
) (*appsv1.StatefulSet, error) {
	logger := log.FromContext(ctx)

	desired := desiredStatefulSet(job, spec, spec.Autoscale.MinReplicas)
	desired.Annotations = map[string]string{specHashAnnotation: specHash(desired)}
	if err := controllerutil.SetControllerReference(job, desired, r.Scheme); err != nil {
		return nil, fmt.Errorf("set owner on statefulset: %w", err)
	}

	var current appsv1.StatefulSet
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), &current)
	switch {
	case apierrors.IsNotFound(err):
		if err := r.Create(ctx, desired); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return &current, nil
			}
			return nil, fmt.Errorf("create statefulset %s: %w", desired.Name, err)
		}
		logger.Info("created worker statefulset", "name", desired.Name, "replicas", spec.Autoscale.MinReplicas)
		return desired, nil
	case err != nil:
		return nil, fmt.Errorf("get statefulset %s: %w", desired.Name, err)
	}

	if current.Annotations[specHashAnnotation] == desired.Annotations[specHashAnnotation] {
		return &current, nil
	}

	// The template drifted. Keep the live replica count — rewriting it here
	// would undo a drain in progress — and roll only the pod spec.
	patched := current.DeepCopy()
	patched.Labels = desired.Labels
	patched.Spec.Template = desired.Spec.Template
	patched.Spec.UpdateStrategy = desired.Spec.UpdateStrategy
	patched.OwnerReferences = desired.OwnerReferences
	if patched.Annotations == nil {
		patched.Annotations = map[string]string{}
	}
	patched.Annotations[specHashAnnotation] = desired.Annotations[specHashAnnotation]

	if err := r.Update(ctx, patched); err != nil {
		return nil, fmt.Errorf("update statefulset %s: %w", desired.Name, err)
	}
	logger.Info("rolled worker statefulset to match spec", "name", desired.Name)
	return patched, nil
}

// updateStatus writes observed state back to the status subresource. It is the
// only place conditions are set, so their meaning stays consistent: Ready
// tracks pod readiness, Progressing tracks convergence, Degraded tracks the
// operator's own ability to observe the cluster.
func (r *ProcessingJobReconciler) updateStatus(
	ctx context.Context,
	job *dpev1alpha1.ProcessingJob,
	sts *appsv1.StatefulSet,
	stats *enginepb.ClusterStats,
	statsErr error,
) error {
	desired := job.Status.DeepCopy()
	desired.ObservedGeneration = job.Generation
	desired.ReadyReplicas = sts.Status.ReadyReplicas
	desired.Replicas = replicasOf(sts)

	if stats != nil {
		desired.StreamLag = stats.GetStreamLag()
		desired.Pending = stats.GetPending()
		desired.ProcessedTotal, desired.ReclaimedTotal = totals(stats)
		desired.Draining = drainingNodes(stats)
	}

	setConditions(desired, sts, statsErr)

	if apiequalStatus(&job.Status, desired) {
		return nil
	}
	job.Status = *desired
	if err := r.Status().Update(ctx, job); err != nil {
		if apierrors.IsConflict(err) {
			// Another writer won; the next reconcile recomputes from fresh
			// state, so a conflict here is not worth an error-backoff.
			return nil
		}
		return fmt.Errorf("update status of %s: %w", job.Name, err)
	}
	return nil
}

// setConditions derives Ready, Progressing and Degraded from what was observed.
func setConditions(status *dpev1alpha1.ProcessingJobStatus, sts *appsv1.StatefulSet, statsErr error) {
	converged := status.ReadyReplicas == status.Replicas

	ready := metav1.Condition{
		Type:    dpev1alpha1.ConditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  "ReplicasNotReady",
		Message: fmt.Sprintf("%d of %d worker replicas ready", status.ReadyReplicas, status.Replicas),
	}
	if converged && status.Replicas > 0 {
		ready.Status = metav1.ConditionTrue
		ready.Reason = "AllReplicasReady"
	}
	if status.Replicas == 0 {
		ready.Reason = "ScaledToZero"
		ready.Message = "no worker replicas requested"
	}
	meta.SetStatusCondition(&status.Conditions, ready)

	progressing := metav1.Condition{
		Type:    dpev1alpha1.ConditionProgressing,
		Status:  metav1.ConditionFalse,
		Reason:  "Converged",
		Message: fmt.Sprintf("statefulset %s is at its desired replica count", sts.Name),
	}
	if !converged || len(status.Draining) > 0 {
		progressing.Status = metav1.ConditionTrue
		progressing.Reason = "Converging"
		progressing.Message = fmt.Sprintf("%d ready of %d desired, %d draining",
			status.ReadyReplicas, status.Replicas, len(status.Draining))
	}
	meta.SetStatusCondition(&status.Conditions, progressing)

	degraded := metav1.Condition{
		Type:    dpev1alpha1.ConditionDegraded,
		Status:  metav1.ConditionFalse,
		Reason:  "Observed",
		Message: "cluster state is observable through the coordinator",
	}
	if statsErr != nil {
		degraded.Status = metav1.ConditionTrue
		degraded.Reason = "CoordinatorUnreachable"
		degraded.Message = statsErr.Error()
	}
	meta.SetStatusCondition(&status.Conditions, degraded)
}

// totals sums per-node progress counters reported by the coordinator.
func totals(stats *enginepb.ClusterStats) (processed, reclaimed int64) {
	for _, n := range stats.GetNodes() {
		processed += n.GetProcessed()
		reclaimed += n.GetReclaimed()
	}
	return processed, reclaimed
}

// drainingNodes lists nodes that report themselves as draining, in the order
// the coordinator returned them.
func drainingNodes(stats *enginepb.ClusterStats) []string {
	var out []string
	for _, n := range stats.GetNodes() {
		if n.GetDraining() {
			out = append(out, n.GetNodeId())
		}
	}
	return out
}

// replicasOf reads a StatefulSet's requested replica count, treating an unset
// pointer as the API server's default of one.
func replicasOf(sts *appsv1.StatefulSet) int32 {
	if sts.Spec.Replicas == nil {
		return 1
	}
	return *sts.Spec.Replicas
}

// SetupWithManager registers the reconciler and the objects it owns. Watching
// the StatefulSet means a pod going ready wakes the loop immediately rather
// than waiting out the requeue interval.
func (r *ProcessingJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Engine == nil {
		r.Engine = NewEngineClient()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&dpev1alpha1.ProcessingJob{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
