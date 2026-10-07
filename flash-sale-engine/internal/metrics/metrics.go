package metrics

import (
	"database/sql"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// HTTPRequestsTotal counts total HTTP requests segmented by method, endpoint and status code.
	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "flashsale",
			Name:      "http_requests_total",
			Help:      "Total number of HTTP requests processed by the flash sale engine.",
		},
		[]string{"method", "endpoint", "status"},
	)

	// HTTPRequestDurationSeconds tracks response latency distributions across endpoints.
	HTTPRequestDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "flashsale",
			Name:      "http_request_duration_seconds",
			Help:      "HTTP request latencies in seconds.",
			Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"method", "endpoint"},
	)

	// StockDepletionDurationSeconds measures the elapsed duration (in seconds)
	// from the start of a flash sale burst until stock hits 0.
	StockDepletionDurationSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "flashsale",
			Name:      "stock_depletion_duration_seconds",
			Help:      "Time in seconds taken for product stock to reach zero during a flash sale.",
		},
		[]string{"product_id"},
	)

	// KafkaConsumerLag tracks the number of uncommitted/unprocessed messages in the queue/stream.
	KafkaConsumerLag = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "flashsale",
			Name:      "kafka_consumer_lag",
			Help:      "Current consumer lag (unprocessed events pending in queue/stream).",
		},
	)

	// ProductStockGauge tracks real-time inventory level in Redis / PostgreSQL.
	ProductStockGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "flashsale",
			Name:      "product_stock",
			Help:      "Current available product stock.",
		},
		[]string{"product_id", "tier"},
	)

	// DBPoolOpenConnections tracks open connections in the PostgreSQL connection pool.
	DBPoolOpenConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "flashsale",
			Name:      "db_pool_open_connections",
			Help:      "Current number of established connections in the database pool.",
		},
	)

	// DBPoolInUseConnections tracks active in-use connections.
	DBPoolInUseConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "flashsale",
			Name:      "db_pool_in_use_connections",
			Help:      "Current number of in-use connections being actively queried.",
		},
	)
)

func init() {
	prometheus.MustRegister(
		HTTPRequestsTotal,
		HTTPRequestDurationSeconds,
		StockDepletionDurationSeconds,
		KafkaConsumerLag,
		ProductStockGauge,
		DBPoolOpenConnections,
		DBPoolInUseConnections,
	)
}

// RegisterDBMetrics starts a background goroutine to periodically update database pool stats.
func RegisterDBMetrics(db *sql.DB, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			stats := db.Stats()
			DBPoolOpenConnections.Set(float64(stats.OpenConnections))
			DBPoolInUseConnections.Set(float64(stats.InUse))
		}
	}()
}

// Handler returns standard Prometheus metrics HTTP handler.
func Handler() http.Handler {
	return promhttp.Handler()
}

// StatusResponseWriter wraps http.ResponseWriter to capture HTTP status code.
type StatusResponseWriter struct {
	http.ResponseWriter
	StatusCode int
}

func (rw *StatusResponseWriter) WriteHeader(code int) {
	rw.StatusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// MetricsMiddleware wraps an HTTP handler to record request count and latency.
func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		srw := &StatusResponseWriter{
			ResponseWriter: w,
			StatusCode:     http.StatusOK,
		}

		next.ServeHTTP(srw, r)

		duration := time.Since(start).Seconds()
		endpoint := r.URL.Path
		method := r.Method
		statusStr := strconv.Itoa(srw.StatusCode)

		HTTPRequestsTotal.WithLabelValues(method, endpoint, statusStr).Inc()
		HTTPRequestDurationSeconds.WithLabelValues(method, endpoint).Observe(duration)
	})
}
