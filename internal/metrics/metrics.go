// Package metrics defines the Prometheus instrumentation exported by every
// node in the cluster. Metric names are stable and are consumed by the
// dashboards and by the benchmark harness in the Makefile.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "dpe"

// Metrics holds every collector used by the engine. Instances are cheap to
// pass around and safe for concurrent use.
type Metrics struct {
	registry *prometheus.Registry

	RecordsProcessed *prometheus.CounterVec
	RecordsDuplicate prometheus.Counter
	RecordsFailed    *prometheus.CounterVec
	RecordsPublished prometheus.Counter
	MessagesAcked    prometheus.Counter

	ReclaimedTotal   prometheus.Counter
	ReclaimBatches   prometheus.Counter
	RecoverySeconds  prometheus.Histogram
	ProcessingSecond prometheus.Histogram

	InFlight    prometheus.Gauge
	PoolSize    prometheus.Gauge
	StreamLag   prometheus.Gauge
	PendingSize prometheus.Gauge
	Draining    prometheus.Gauge
}

// New builds a Metrics bound to a fresh registry. Callers own the registry, so
// several engines can run inside one process (the integration test relies on
// this).
func New(nodeID string) *Metrics {
	reg := prometheus.NewRegistry()
	m := NewWith(reg, nodeID)
	reg.MustRegister(prometheus.NewGoCollector())
	return m
}

// NewWith registers the collectors on an existing registerer.
func NewWith(reg prometheus.Registerer, nodeID string) *Metrics {
	labels := prometheus.Labels{"node": nodeID}
	factory := promauto{reg}

	m := &Metrics{
		RecordsProcessed: factory.counterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "records_processed_total",
			Help: "Records committed to the sink, by outcome.", ConstLabels: labels,
		}, []string{"outcome"}),
		RecordsDuplicate: factory.counter(prometheus.CounterOpts{
			Namespace: namespace, Name: "records_duplicate_total",
			Help: "Redeliveries suppressed by the idempotency key.", ConstLabels: labels,
		}),
		RecordsFailed: factory.counterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "records_failed_total",
			Help: "Handler failures, by reason.", ConstLabels: labels,
		}, []string{"reason"}),
		RecordsPublished: factory.counter(prometheus.CounterOpts{
			Namespace: namespace, Name: "records_published_total",
			Help: "Records appended to the stream via XADD.", ConstLabels: labels,
		}),
		MessagesAcked: factory.counter(prometheus.CounterOpts{
			Namespace: namespace, Name: "messages_acked_total",
			Help: "Stream entries acknowledged via XACK.", ConstLabels: labels,
		}),
		ReclaimedTotal: factory.counter(prometheus.CounterOpts{
			Namespace: namespace, Name: "reclaimed_messages_total",
			Help: "Messages taken over from an expired lease via XAUTOCLAIM.", ConstLabels: labels,
		}),
		ReclaimBatches: factory.counter(prometheus.CounterOpts{
			Namespace: namespace, Name: "reclaim_batches_total",
			Help: "XAUTOCLAIM calls that returned at least one message.", ConstLabels: labels,
		}),
		RecoverySeconds: factory.histogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "recovery_duration_seconds",
			Help:        "Age of a reclaimed message when it was taken over, i.e. time to recover from a node loss.",
			Buckets:     []float64{0.25, 0.5, 1, 1.5, 2, 3, 5, 10, 30},
			ConstLabels: labels,
		}),
		ProcessingSecond: factory.histogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "processing_duration_seconds",
			Help:        "Wall time spent in the record handler.",
			Buckets:     prometheus.DefBuckets,
			ConstLabels: labels,
		}),
		InFlight: factory.gauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "inflight_records",
			Help: "Records checked out of the stream but not yet acked.", ConstLabels: labels,
		}),
		PoolSize: factory.gauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "pool_capacity",
			Help: "Configured bounded-concurrency window.", ConstLabels: labels,
		}),
		StreamLag: factory.gauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "stream_lag_records",
			Help: "Consumer-group lag reported by XINFO GROUPS.", ConstLabels: labels,
		}),
		PendingSize: factory.gauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "pending_entries",
			Help: "Size of the group's pending entries list.", ConstLabels: labels,
		}),
		Draining: factory.gauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "draining",
			Help: "1 while the node is draining after SIGTERM.", ConstLabels: labels,
		}),
	}
	if r, ok := reg.(*prometheus.Registry); ok {
		m.registry = r
	}
	return m
}

// ObserveProcessing records handler latency.
func (m *Metrics) ObserveProcessing(d time.Duration) {
	m.ProcessingSecond.Observe(d.Seconds())
}

// ObserveRecovery records how long a message sat idle before another node
// picked it up. This is the "recovery time after node loss" figure.
func (m *Metrics) ObserveRecovery(d time.Duration) {
	m.RecoverySeconds.Observe(d.Seconds())
}

// Handler exposes the registry over HTTP.
func (m *Metrics) Handler() http.Handler {
	if m.registry == nil {
		return promhttp.Handler()
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Serve starts a /metrics endpoint. It blocks until the server exits.
func (m *Metrics) Serve(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return srv.ListenAndServe()
}

// promauto is a tiny local helper so collectors register on the supplied
// registerer instead of the global default.
type promauto struct{ reg prometheus.Registerer }

func (p promauto) counter(o prometheus.CounterOpts) prometheus.Counter {
	c := prometheus.NewCounter(o)
	p.reg.MustRegister(c)
	return c
}

func (p promauto) counterVec(o prometheus.CounterOpts, labels []string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(o, labels)
	p.reg.MustRegister(c)
	return c
}

func (p promauto) gauge(o prometheus.GaugeOpts) prometheus.Gauge {
	g := prometheus.NewGauge(o)
	p.reg.MustRegister(g)
	return g
}

func (p promauto) histogram(o prometheus.HistogramOpts) prometheus.Histogram {
	h := prometheus.NewHistogram(o)
	p.reg.MustRegister(h)
	return h
}
