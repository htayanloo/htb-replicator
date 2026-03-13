package metrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	// ObjectsTotal counts objects processed by destination and status.
	ObjectsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "replication_objects_total",
		Help: "Total objects processed",
	}, []string{"destination", "status"})

	// BytesTotal counts bytes replicated per destination.
	BytesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "replication_bytes_total",
		Help: "Total bytes replicated",
	}, []string{"destination"})

	// ErrorsTotal counts errors by destination and error type.
	ErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "replication_errors_total",
		Help: "Total errors",
	}, []string{"destination", "error_type"})

	// QueueSize tracks the current number of tasks in the worker queue.
	QueueSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "replication_queue_size",
		Help: "Current queue size",
	})

	// WorkersActive tracks the number of active worker goroutines.
	WorkersActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "replication_workers_active",
		Help: "Active workers",
	})

	// SyncCycleDuration tracks the duration of each sync cycle.
	SyncCycleDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "replication_sync_cycle_duration_seconds",
		Help:    "Duration of sync cycles in seconds",
		Buckets: prometheus.DefBuckets,
	})

	// SyncCyclesTotal counts the total number of sync cycles.
	SyncCyclesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "replication_sync_cycles_total",
		Help: "Total number of sync cycles",
	}, []string{"status"})

	// ObjectsListed tracks how many objects were listed from source per cycle.
	ObjectsListed = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "replication_objects_listed",
		Help: "Number of objects listed from source in last cycle",
	})

	// RetentionDeletesTotal counts objects deleted by the retention policy.
	RetentionDeletesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "replication_retention_deletes_total",
		Help: "Total objects deleted by retention policy",
	}, []string{"location"})
)

// StartServer starts the Prometheus metrics HTTP server on the given port.
// It is non-blocking; the server runs in a background goroutine.
func StartServer(port int, logger *zap.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	addr := fmt.Sprintf(":%d", port)
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		logger.Info("metrics server listening", zap.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server error", zap.Error(err))
		}
	}()
}
