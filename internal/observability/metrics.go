package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics, sistemin finansal ve operasyonel gözlemlenebilirlik sayaç ve histogramlarını tutar.
type Metrics struct {
	// İşlem Gecikmesi (Histogram)
	TransactionDuration *prometheus.HistogramVec

	// Idempotency Durum Oranları (Counter) - hit, miss, in_flight, conflict, key_reuse
	IdempotencyRequests *prometheus.CounterVec

	// Bakiye Mutabakat Hataları (Counter) - unbalanced, tampered_hash, constraint_violation
	ReconciliationErrors *prometheus.CounterVec

	// Anlık Aktif İşlem Sayısı (Gauge)
	ActiveTransactions prometheus.Gauge

	// Veritabanı Bağlantı Havuzu Metrikleri
	DBPoolTotalConns    prometheus.Gauge
	DBPoolAcquiredConns prometheus.Gauge
	DBPoolIdleConns     prometheus.Gauge
}

var globalMetrics *Metrics

// InitMetrics, prometheus kayıt defterine (registry) metrikleri kaydeder veya varsayılanı döner.
func InitMetrics(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	m := &Metrics{
		TransactionDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "ledger",
				Subsystem: "transaction",
				Name:      "duration_seconds",
				Help:      "Ledger işlem tamamlama süresi (saniye cinsinden).",
				Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
			},
			[]string{"status", "strategy"},
		),

		IdempotencyRequests: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "ledger",
				Subsystem: "idempotency",
				Name:      "requests_total",
				Help:      "Idempotency anahtarı ile gelen isteklerin toplam sayısı ve durum dağılımı.",
			},
			[]string{"status"}, // "hit", "miss", "in_flight", "conflict", "error"
		),

		ReconciliationErrors: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "ledger",
				Subsystem: "reconciliation",
				Name:      "errors_total",
				Help:      "Bakiye denkliği, hash zinciri bozulması veya mutabakat hatalarının toplam sayısı.",
			},
			[]string{"reason"}, // "unbalanced", "tampered_hash", "negative_balance"
		),

		ActiveTransactions: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "ledger",
				Subsystem: "transaction",
				Name:      "active_count",
				Help:      "Şu anda yürütülmekte olan aktif işlem sayısı.",
			},
		),

		DBPoolTotalConns: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "ledger",
				Subsystem: "db_pool",
				Name:      "total_connections",
				Help:      "Veritabanı bağlantı havuzundaki toplam bağlantı sayısı.",
			},
		),

		DBPoolAcquiredConns: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "ledger",
				Subsystem: "db_pool",
				Name:      "acquired_connections",
				Help:      "Şu anda kullanımda olan veritabanı bağlantı sayısı.",
			},
		),

		DBPoolIdleConns: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "ledger",
				Subsystem: "db_pool",
				Name:      "idle_connections",
				Help:      "Boşta bekleyen veritabanı bağlantı sayısı.",
			},
		),
	}

	globalMetrics = m
	return m
}

// GetMetrics, global olarak ilklendirilmiş metrikleri döner.
func GetMetrics() *Metrics {
	if globalMetrics == nil {
		globalMetrics = InitMetrics(prometheus.DefaultRegisterer)
	}
	return globalMetrics
}
