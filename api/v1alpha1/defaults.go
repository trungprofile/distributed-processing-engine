package v1alpha1

import "time"

// Defaults mirrored from the kubebuilder markers on ProcessingJobSpec. The
// API server applies them from the CRD schema; these constants let the
// controller and its tests resolve the same values without a live API server.
const (
	defaultMinIdle             = 3 * time.Second
	defaultDrainTimeout        = 30 * time.Second
	defaultMaxReplicas         = 16
	defaultTargetLagPerWorker  = 500
	defaultStabilizationWindow = 60 * time.Second
)
