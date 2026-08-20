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

// drainPollInterval is how often the operator re-checks a drain it requested.
// A drain finishes in well under a second on an idle worker, so polling faster
// than the steady-state interval turns a scale-in from "eventually" into
// "promptly" without adding meaningful load.
const drainPollInterval = time.Second

// ProcessingJobReconciler reconciles a ProcessingJob into a StatefulSet of
// workers plus the headless Service that governs it.
type ProcessingJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Engine reads cluster state and requests drains through the
	// coordinator's gRPC control plane.
	Engine EngineClient

	// scaleDown damps scale-in so a backlog oscillating around the threshold
	// does not drain a worker on every reconcile.
	scaleDown *scaleDownTracker

	// drains tracks the worker each job is currently draining ahead of a
	// scale-in, so a reconcile can tell "waiting" from "not started".
	drains *drainTracker
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
		if apierrors.IsNotFound(err) {
			// Deleted. The Service and StatefulSet carry owner references back
			// to the job, so garbage collection has already removed them; only
			// the in-memory scale-down timer is ours to clean up.
			r.scaleDown.forget(req.NamespacedName)
			r.drains.clear(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
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
		// directly and are unaffected by the control plane being down. Report
		// it and leave the replica count where it is.
		logger.Info("coordinator unreachable, leaving replica count unchanged", "error", statsErr)
		if err := r.updateStatus(ctx, &job, sts, nil, statsErr); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	requeue, err := r.converge(ctx, &job, spec, sts, stats)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.updateStatus(ctx, &job, sts, stats, nil); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// converge moves the StatefulSet towards the replica count the backlog calls
// for and returns how long to wait before looking again.
//
// Scaling up is a one-line patch. Scaling down is deliberately not: it is
// damped by the stabilization window, and even once permitted it goes through
// the drain protocol rather than deleting a pod outright.
func (r *ProcessingJobReconciler) converge(
	ctx context.Context,
	job *dpev1alpha1.ProcessingJob,
	spec dpev1alpha1.ProcessingJobSpec,
	sts *appsv1.StatefulSet,
	stats *enginepb.ClusterStats,
) (time.Duration, error) {
	logger := log.FromContext(ctx)

	now := time.Now()
	key := client.ObjectKeyFromObject(job)
	current := replicasOf(sts)
	desired := desiredReplicas(spec, stats.GetStreamLag())

	// A drain already in flight takes precedence over the current backlog
	// reading: the worker has been told to stop, and the only question left is
	// whether it has finished.
	if attempt, ok := r.drains.current(key); ok {
		if desired >= current {
			// Backlog came back while the victim was draining. Let it go: the
			// StatefulSet restarts the drained pod under the same name, and it
			// rejoins the group as the same consumer.
			logger.Info("backlog recovered mid-drain, abandoning scale-in", "node", attempt.node)
			r.drains.clear(key)
			r.scaleDown.forget(key)
			return r.scaleUpTo(ctx, sts, current, desired, stats)
		}
		return r.finishDrain(ctx, job, spec, sts, stats, key, attempt, now)
	}

	if desired == current {
		return requeueInterval, nil
	}
	if desired > current {
		return r.scaleUpTo(ctx, sts, current, desired, stats)
	}

	window := spec.Autoscale.StabilizationWindow.Duration
	allowed, readyAt := r.scaleDown.allow(key, current, desired, window, now)
	if !allowed {
		// Come back exactly when the window closes rather than polling: the
		// backlog may well rise again before then and reset the timer.
		if wait := readyAt.Sub(now); wait > requeueInterval {
			return wait, nil
		}
		return requeueInterval, nil
	}

	// Scale in one replica at a time, and drain that replica before removing
	// it. Doing several at once would take a large slice of the group's
	// capacity out of service simultaneously.
	victim := victimNode(job, current)
	if err := r.beginDrain(ctx, coordinatorAddr(job, spec), victim); err != nil {
		// The drain request failed to reach the coordinator. Do not scale in
		// blind — leave the replica running and try again next reconcile.
		logger.Info("drain request failed, holding replica count", "node", victim, "error", err)
		return requeueInterval, nil
	}
	r.drains.start(key, victim, now)
	return drainPollInterval, nil
}

// scaleUpTo raises the replica count immediately. Backlog is already costing
// latency and an extra consumer costs nothing but a pod, so there is no
// stabilization window on the way up.
func (r *ProcessingJobReconciler) scaleUpTo(
	ctx context.Context,
	sts *appsv1.StatefulSet,
	current, desired int32,
	stats *enginepb.ClusterStats,
) (time.Duration, error) {
	if desired <= current {
		return requeueInterval, nil
	}
	if err := r.scaleTo(ctx, sts, desired); err != nil {
		return 0, err
	}
	log.FromContext(ctx).Info("scaled up on backlog",
		"from", current, "to", desired, "stream_lag", stats.GetStreamLag())
	return requeueInterval, nil
}

// finishDrain decides what to do about a drain the operator already requested.
func (r *ProcessingJobReconciler) finishDrain(
	ctx context.Context,
	job *dpev1alpha1.ProcessingJob,
	spec dpev1alpha1.ProcessingJobSpec,
	sts *appsv1.StatefulSet,
	stats *enginepb.ClusterStats,
	key client.ObjectKey,
	attempt drainAttempt,
	now time.Time,
) (time.Duration, error) {
	logger := log.FromContext(ctx)
	current := replicasOf(sts)

	switch waitForDrain(stats, attempt.node, attempt.startedAt, spec.DrainTimeout.Duration, now) {
	case drainPending:
		return drainPollInterval, nil

	case drainExpired:
		// The worker did not finish inside its own budget. Do not force the
		// pod away: whatever it still holds is checked out of the stream, and
		// deleting it now would strand those records for a full lease. Give up
		// on this scale-in and let the next reconcile decide afresh.
		logger.Info("drain exceeded its budget, leaving replica count unchanged",
			"node", attempt.node, "budget", spec.DrainTimeout.Duration.String())
		r.drains.clear(key)
		return requeueInterval, nil

	default: // drainGone
		if err := r.scaleTo(ctx, sts, current-1); err != nil {
			return 0, err
		}
		r.drains.clear(key)
		logger.Info("scaled down after a clean drain",
			"node", attempt.node, "from", current, "to", current-1,
			"drain_duration", now.Sub(attempt.startedAt).Round(time.Millisecond).String())
		return requeueInterval, nil
	}
}

// scaleTo patches the StatefulSet's replica count in place.
func (r *ProcessingJobReconciler) scaleTo(ctx context.Context, sts *appsv1.StatefulSet, replicas int32) error {
	patch := client.MergeFrom(sts.DeepCopy())
	sts.Spec.Replicas = &replicas
	if err := r.Patch(ctx, sts, patch); err != nil {
		return fmt.Errorf("scale %s to %d: %w", sts.Name, replicas, err)
	}
	return nil
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
	if r.scaleDown == nil {
		r.scaleDown = newScaleDownTracker()
	}
	if r.drains == nil {
		r.drains = newDrainTracker()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&dpev1alpha1.ProcessingJob{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
