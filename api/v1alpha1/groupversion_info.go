// Package v1alpha1 contains the ProcessingJob API: the declarative surface a
// cluster operator uses to run the engine on Kubernetes. One ProcessingJob
// describes one consumer group — which stream to drain, how wide the worker
// pool may grow, and where its pods are allowed to land.
//
// +kubebuilder:object:generate=true
// +groupName=dpe.trungprofile.dev
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// GroupVersion is the group and version this package registers.
var GroupVersion = schema.GroupVersion{Group: "dpe.trungprofile.dev", Version: "v1alpha1"}

// SchemeBuilder collects the types registered into a runtime scheme.
var SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

// AddToScheme registers this API group with a scheme.
var AddToScheme = SchemeBuilder.AddToScheme
