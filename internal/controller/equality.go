package controller

import (
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"

	dpev1alpha1 "github.com/trungprofile/distributed-processing-engine/api/v1alpha1"
)

// equalService compares the fields the operator owns on a Service. The API
// server defaults the rest, so a full DeepEqual would report a difference on
// every reconcile and turn the loop into a write amplifier.
func equalService(a, b *corev1.Service) bool {
	return apiequality.Semantic.DeepEqual(a.Labels, b.Labels) &&
		apiequality.Semantic.DeepEqual(a.Spec.Selector, b.Spec.Selector) &&
		apiequality.Semantic.DeepEqual(a.Spec.Ports, b.Spec.Ports) &&
		apiequality.Semantic.DeepEqual(a.OwnerReferences, b.OwnerReferences)
}

// apiequalStatus reports whether a recomputed status is indistinguishable from
// the stored one. Conditions carry a LastTransitionTime that only moves when
// the status itself changes, so semantic equality is stable across reconciles
// and a no-op reconcile issues no write.
func apiequalStatus(a, b *dpev1alpha1.ProcessingJobStatus) bool {
	return apiequality.Semantic.DeepEqual(a, b)
}
