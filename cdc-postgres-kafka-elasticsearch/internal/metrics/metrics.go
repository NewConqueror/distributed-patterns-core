package metrics

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// EventsConsumedTotal counts total CDC events processed
	EventsConsumedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdc_events_consumed_total",
			Help: "Total number of CDC events consumed from Kafka categorized by status (success or failure).",
		},
		[]string{"status"},
	)

	// BulkFlushDurationSeconds tracks the latency of Elasticsearch _bulk index/delete requests
	BulkFlushDurationSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "cdc_bulk_flush_duration_seconds",
			Help:    "Duration in seconds of Elasticsearch bulk flush operations.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		},
	)

	// BatchSizeGauge tracks the size of the micro-batches being processed
	BatchSizeGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "cdc_batch_size",
			Help: "Current or most recent micro-batch size processed by the pipeline.",
		},
	)

	// E2ELatencySeconds measures the end-to-end latency: T_indexed - T_wal_commit
	E2ELatencySeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "cdc_e2e_latency_seconds",
			Help:    "End-to-end latency from PostgreSQL WAL commit to Elasticsearch indexing completion (in seconds).",
			Buckets: []float64{0.01, 0.025, 0.05, 0.085, 0.1, 0.15, 0.25, 0.5, 1.0, 2.0, 5.0},
		},
	)

	// DLQEventsTotal counts total poison pill messages routed to Dead Letter Queue
	DLQEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdc_dlq_events_total",
			Help: "Total number of poison pill messages routed to DLQ categorized by error type.",
		},
		[]string{"error_type"},
	)

	// CircuitBreakerStateGauge tracks 0 = CLOSED (normal), 1 = OPEN (tripped / paused)
	CircuitBreakerStateGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "cdc_circuit_breaker_state",
			Help: "State of Elasticsearch Circuit Breaker (0 = CLOSED, 1 = OPEN).",
		},
	)
)

func init() {
	prometheus.MustRegister(
		EventsConsumedTotal,
		BulkFlushDurationSeconds,
		BatchSizeGauge,
		E2ELatencySeconds,
		DLQEventsTotal,
		CircuitBreakerStateGauge,
	)
}

// RecordEventConsumed increments success or failure event count
func RecordEventConsumed(status string) {
	EventsConsumedTotal.WithLabelValues(status).Inc()
}

// RecordBulkFlush tracks bulk write duration
func RecordBulkFlush(durationSec float64) {
	BulkFlushDurationSeconds.Observe(durationSec)
}

// SetBatchSize updates current batch size gauge
func SetBatchSize(size float64) {
	BatchSizeGauge.Set(size)
}

// RecordE2ELatency measures end-to-end latency: T_indexed - T_wal_commit
func RecordE2ELatency(durationSec float64) {
	if durationSec > 0 {
		E2ELatencySeconds.Observe(durationSec)
	}
}

// RecordDLQEvent increments count of messages sent to DLQ
func RecordDLQEvent(errorType string) {
	DLQEventsTotal.WithLabelValues(errorType).Inc()
}

// SetCircuitBreakerState updates state gauge: 0 = CLOSED, 1 = OPEN
func SetCircuitBreakerState(isOpen bool) {
	if isOpen {
		CircuitBreakerStateGauge.Set(1)
	} else {
		CircuitBreakerStateGauge.Set(0)
	}
}

// StartMetricsServer runs an HTTP server exposing /metrics and /healthz
func StartMetricsServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		log.Printf("[Observability] Prometheus metrics server listening on http://%s/metrics", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[Metrics Error] HTTP metrics server error: %v", err)
		}
	}()

	return srv
}

// ShutdownMetricsServer stops the metrics HTTP server
func ShutdownMetricsServer(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("[Metrics Error] Server shutdown error: %v", err)
	}
}
