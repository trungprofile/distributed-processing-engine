// Command operator runs the Kubernetes controller for the ProcessingJob API.
// It reconciles each job into a StatefulSet of workers, scales that set on
// consumer-group backlog, and drains a worker before ever removing it.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"

	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	dpev1alpha1 "github.com/trungprofile/distributed-processing-engine/api/v1alpha1"
	"github.com/trungprofile/distributed-processing-engine/internal/controller"
)

// scheme carries the core Kubernetes types the operator writes plus the
// ProcessingJob API it owns.
var scheme = runtime.NewScheme()

func init() {
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := dpev1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
}

func main() {
	var (
		metricsAddr = flag.String("metrics-addr", ":8080", "address for the controller-runtime /metrics endpoint")
		probeAddr   = flag.String("health-probe-addr", ":8081", "address for /healthz and /readyz")
		leaderElect = flag.Bool("leader-elect", true, "run leader election so only one replica reconciles at a time")
		namespace   = flag.String("namespace", os.Getenv("WATCH_NAMESPACE"), "restrict the cache to one namespace; empty watches all")
		logLevel    = flag.String("zap-log-level", "info", "log verbosity: debug, info or error")
	)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(*logLevel == "debug"), zap.Level(zapLevel(*logLevel))))
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	options := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: *metricsAddr},
		HealthProbeBindAddress: *probeAddr,
		// Leader election is on by default: two operators reconciling one
		// ProcessingJob could each pick a different scale-down victim and
		// drain two workers for one unit of scale-in.
		LeaderElection:   *leaderElect,
		LeaderElectionID: "processingjob.dpe.trungprofile.dev",
	}
	if *namespace != "" {
		options.Cache = namespacedCache(*namespace)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), options)
	if err != nil {
		log.Error("manager setup failed", "error", err)
		os.Exit(1)
	}

	engine := controller.NewEngineClient()
	defer func() { _ = engine.Close() }()

	reconciler := &controller.ProcessingJobReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Engine: engine,
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		log.Error("controller setup failed", "error", err)
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		log.Error("healthz setup failed", "error", err)
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("ping", healthz.Ping); err != nil {
		log.Error("readyz setup failed", "error", err)
		os.Exit(1)
	}

	log.Info("operator starting", "metrics", *metricsAddr, "probes", *probeAddr, "leader_election", *leaderElect)

	// SIGTERM cancels this context; the manager stops the reconcile loop and
	// releases the leader lease so a rolling update hands over immediately.
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("operator stopped", "error", err)
		os.Exit(1)
	}
	log.Info("operator stopped cleanly")
}

// zapLevel maps the --zap-log-level flag onto a zap level, defaulting to info
// for any value that is not recognised.
func zapLevel(name string) zapcore.Level {
	switch name {
	case "debug":
		return zapcore.DebugLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

// namespacedCache restricts the manager's informers to one namespace, which is
// what lets the operator run with a namespaced Role instead of a ClusterRole.
func namespacedCache(namespace string) cache.Options {
	return cache.Options{
		DefaultNamespaces: map[string]cache.Config{namespace: {}},
	}
}
