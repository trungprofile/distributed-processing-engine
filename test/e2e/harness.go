//go:build e2e

// Package e2e drives the operator against a real cluster: apply a
// ProcessingJob, push load through the coordinator, and assert on what the
// cluster and the sink actually did.
//
// The suite is behind the `e2e` build tag so `go test ./...` never compiles
// it, and it expects a cluster with the Helm chart already installed — see
// test/e2e/README.md and `make kind-up`.
package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	enginepb "github.com/trungprofile/distributed-processing-engine/api/proto"
	dpev1alpha1 "github.com/trungprofile/distributed-processing-engine/api/v1alpha1"
)

// env is everything the suite needs to find the installed release. Defaults
// match what `make kind-up` installs, so a local run needs no environment.
type env struct {
	Namespace       string
	Image           string
	RedisAddr       string
	CoordinatorAddr string
}

func loadEnv() env {
	return env{
		Namespace:       envOr("DPE_E2E_NAMESPACE", "dpe-system"),
		Image:           envOr("DPE_E2E_IMAGE", "dpe:e2e"),
		RedisAddr:       envOr("DPE_E2E_REDIS_ADDR", "localhost:6379"),
		CoordinatorAddr: envOr("DPE_E2E_COORDINATOR_ADDR", "localhost:9090"),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// harness bundles the three clients every test needs: the Kubernetes API, the
// coordinator's gRPC control plane, and Redis for asserting on the sink.
type harness struct {
	env   env
	k8s   client.Client
	rpc   enginepb.EngineClient
	redis redis.UniversalClient
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	e := loadEnv()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("register core scheme: %v", err)
	}
	if err := dpev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("register dpe scheme: %v", err)
	}

	cfg, err := config.GetConfig()
	if err != nil {
		t.Skipf("no kubeconfig available (%v); run `make kind-up` first", err)
	}
	k8s, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("build kubernetes client: %v", err)
	}

	conn, err := grpc.NewClient(e.CoordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial coordinator at %s: %v", e.CoordinatorAddr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	rdb := redis.NewClient(&redis.Options{Addr: e.RedisAddr, PoolSize: 32})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unreachable at %s (%v); is the port-forward up?", e.RedisAddr, err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	return &harness{env: e, k8s: k8s, rpc: enginepb.NewEngineClient(conn), redis: rdb}
}

// newJob builds a ProcessingJob against a namespace unique to this test, so
// parallel or repeated runs never share a stream, a group or a sink.
func (h *harness) newJob(t *testing.T, name string, mutate func(*dpev1alpha1.ProcessingJob)) *dpev1alpha1.ProcessingJob {
	t.Helper()
	ns := fmt.Sprintf("%s-%d", name, time.Now().UnixNano())

	job := &dpev1alpha1.ProcessingJob{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: h.env.Namespace},
		Spec: dpev1alpha1.ProcessingJobSpec{
			Stream:          ns + ":records",
			Group:           ns + ":group",
			RedisAddr:       "dpe-redis:6379",
			Image:           h.env.Image,
			CoordinatorAddr: fmt.Sprintf("dpe-coordinator.%s.svc:9090", h.env.Namespace),
			Concurrency:     16,
			MinIdle:         &metav1.Duration{Duration: 2 * time.Second},
			DrainTimeout:    &metav1.Duration{Duration: 20 * time.Second},
			Autoscale: dpev1alpha1.AutoscaleSpec{
				MinReplicas:         1,
				MaxReplicas:         6,
				TargetLagPerWorker:  500,
				StabilizationWindow: &metav1.Duration{Duration: 10 * time.Second},
			},
		},
	}
	if mutate != nil {
		mutate(job)
	}

	ctx := context.Background()
	if err := h.k8s.Create(ctx, job); err != nil {
		t.Fatalf("create ProcessingJob: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_ = h.k8s.Delete(cleanupCtx, job)
		h.waitForWorkloadGone(t, job)
	})
	return job
}

// submit pushes n records through the coordinator's SubmitBatch RPC and
// returns the keys it produced.
func (h *harness) submit(t *testing.T, job *dpev1alpha1.ProcessingJob, n int) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	keys := make([]string, 0, n)
	const chunk = 1000
	for start := 0; start < n; start += chunk {
		end := min(start+chunk, n)
		batch := make([]*enginepb.Record, 0, end-start)
		for i := start; i < end; i++ {
			key := fmt.Sprintf("%s-rec-%06d", job.Name, i)
			keys = append(keys, key)
			batch = append(batch, &enginepb.Record{Key: key, Payload: []byte(key)})
		}
		if _, err := h.rpc.SubmitBatch(ctx, &enginepb.SubmitBatchRequest{Records: batch}); err != nil {
			t.Fatalf("submit batch at %d: %v", start, err)
		}
	}
	return keys
}

func (h *harness) stats(t *testing.T) *enginepb.ClusterStats {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stats, err := h.rpc.GetClusterStats(ctx, &enginepb.GetClusterStatsRequest{})
	if err != nil {
		t.Fatalf("cluster stats: %v", err)
	}
	return stats
}

func (h *harness) job(t *testing.T, job *dpev1alpha1.ProcessingJob) *dpev1alpha1.ProcessingJob {
	t.Helper()
	var out dpev1alpha1.ProcessingJob
	key := types.NamespacedName{Name: job.Name, Namespace: job.Namespace}
	if err := h.k8s.Get(context.Background(), key, &out); err != nil {
		t.Fatalf("get ProcessingJob: %v", err)
	}
	return &out
}

func (h *harness) statefulSet(t *testing.T, job *dpev1alpha1.ProcessingJob) *appsv1.StatefulSet {
	t.Helper()
	var sts appsv1.StatefulSet
	key := types.NamespacedName{Name: job.Name + "-worker", Namespace: job.Namespace}
	if err := h.k8s.Get(context.Background(), key, &sts); err != nil {
		t.Fatalf("get StatefulSet: %v", err)
	}
	return &sts
}

func (h *harness) workerPods(t *testing.T, job *dpev1alpha1.ProcessingJob) []corev1.Pod {
	t.Helper()
	var pods corev1.PodList
	err := h.k8s.List(context.Background(), &pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{"dpe.trungprofile.dev/processing-job": job.Name})
	if err != nil {
		t.Fatalf("list worker pods: %v", err)
	}
	return pods.Items
}

// sinkKeys reads the idempotency-keyed sink hash the workers write into.
func (h *harness) sinkKeys(t *testing.T) map[string]struct{} {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	keys, err := h.redis.HKeys(ctx, "dpe:results").Result()
	if err != nil {
		t.Fatalf("read sink: %v", err)
	}
	out := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		out[k] = struct{}{}
	}
	return out
}

// waitFor polls cond until it holds or the deadline passes. Everything in an
// operator is eventually consistent, so every assertion in this suite is a
// convergence assertion rather than a point-in-time one.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

func (h *harness) waitForWorkloadGone(t *testing.T, job *dpev1alpha1.ProcessingJob) {
	t.Helper()
	key := types.NamespacedName{Name: job.Name + "-worker", Namespace: job.Namespace}
	waitFor(t, 2*time.Minute, "the owned StatefulSet to be garbage collected", func() bool {
		var sts appsv1.StatefulSet
		err := h.k8s.Get(context.Background(), key, &sts)
		return apierrors.IsNotFound(err)
	})
}

// waitForReady blocks until the job reports the given number of ready workers.
func (h *harness) waitForReady(t *testing.T, job *dpev1alpha1.ProcessingJob, replicas int32, timeout time.Duration) {
	t.Helper()
	waitFor(t, timeout, fmt.Sprintf("%d ready worker replicas", replicas), func() bool {
		return h.job(t, job).Status.ReadyReplicas >= replicas
	})
}

// conditionIs reports whether a status condition holds the expected value.
func conditionIs(job *dpev1alpha1.ProcessingJob, condType string, status metav1.ConditionStatus) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == condType {
			return c.Status == status
		}
	}
	return false
}
