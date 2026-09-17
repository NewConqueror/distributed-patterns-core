package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus metrics registered for GeoPulse.
type Metrics struct {
	// 5.1 Required Metrics
	ActiveConnections prometheus.Gauge
	LocationPings     *prometheus.CounterVec
	BatchFlushDuration prometheus.Histogram

	// Additional Engine & Buffer Observability Metrics
	RingBufferDepth    prometheus.Gauge
	BatchFlushRecords  prometheus.Counter
	BatchFlushErrors   prometheus.Counter
	RedisLatency       prometheus.Histogram
	RedisPrunedDrivers prometheus.Counter
}

var defaultMetrics *Metrics

// NewMetrics initializes and registers all Prometheus collectors.
func NewMetrics() *Metrics {
	m := &Metrics{
		ActiveConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "active_connections_count",
			Help: "Current count of active streaming gRPC client connections",
		}),

		LocationPings: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "location_pings_total",
			Help: "Total count of telemetry pings handled by the engine",
		}, []string{"status"}), // status: received, processed, dropped_oldest, dropped_newest

		BatchFlushDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "batch_flush_duration_seconds",
			Help:    "Latency histogram of database batch flush operations",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		}),

		RingBufferDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ring_buffer_depth",
			Help: "Current number of queued records in the ingestion ring buffer",
		}),

		BatchFlushRecords: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batch_flush_records_total",
			Help: "Total number of telemetry records successfully flushed to PostgreSQL",
		}),

		BatchFlushErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "batch_flush_errors_total",
			Help: "Total number of batch persistence errors encountered",
		}),

		RedisLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "redis_spatial_duration_seconds",
			Help:    "Latency histogram of Redis Geospatial indexing and pub/sub operations",
			Buckets: []float64{0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1},
		}),

		RedisPrunedDrivers: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "redis_stale_drivers_pruned_total",
			Help: "Total count of ghost drivers evicted by the background sweeper",
		}),
	}

	// Register collectors with default Prometheus registry
	prometheus.MustRegister(
		m.ActiveConnections,
		m.LocationPings,
		m.BatchFlushDuration,
		m.RingBufferDepth,
		m.BatchFlushRecords,
		m.BatchFlushErrors,
		m.RedisLatency,
		m.RedisPrunedDrivers,
	)

	defaultMetrics = m
	return m
}

// Default returns the singleton registered metrics instance.
func Default() *Metrics {
	if defaultMetrics == nil {
		return NewMetrics()
	}
	return defaultMetrics
}

// Server provides an HTTP endpoint exposing /metrics for Prometheus scrapers.
type Server struct {
	httpServer *http.Server
	logger     *slog.Logger
}

// StartMetricsServer starts the HTTP server exposing Prometheus metrics on the given address.
func StartMetricsServer(addr string, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	// Ensure metrics collectors are initialized and registered
	_ = Default()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	logger.Info("Starting Prometheus metrics server", "listen_addr", addr, "endpoint", "/metrics")

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Metrics HTTP server error", "err", err)
		}
	}()

	return &Server{
		httpServer: srv,
		logger:     logger,
	}
}

// Stop gracefully shuts down the Prometheus HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("Shutting down Prometheus metrics server")
	return s.httpServer.Shutdown(ctx)
}
