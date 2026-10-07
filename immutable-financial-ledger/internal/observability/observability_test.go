package observability_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"Immutable_Double-Entry_FinancialLedger_with_CryptographicAuditTrails_StrictIdempotency/internal/observability"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel/attribute"
)

func TestMetrics_RecordingAndScraping(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := observability.InitMetrics(reg)

	// 1. Transaction Duration Histogram
	start := time.Now()
	time.Sleep(10 * time.Millisecond)
	m.TransactionDuration.WithLabelValues("201", "pessimistic").Observe(time.Since(start).Seconds())

	// 2. Idempotency Counter
	m.IdempotencyRequests.WithLabelValues("hit").Inc()
	m.IdempotencyRequests.WithLabelValues("miss").Inc()
	m.IdempotencyRequests.WithLabelValues("conflict").Inc()

	// 3. Reconciliation Errors Counter
	m.ReconciliationErrors.WithLabelValues("unbalanced").Inc()

	// 4. Gauges
	m.ActiveTransactions.Set(5)
	m.DBPoolTotalConns.Set(50)
	m.DBPoolAcquiredConns.Set(12)
	m.DBPoolIdleConns.Set(38)

	// Doğrulamalar (testutil ile)
	hitCount := testutil.ToFloat64(m.IdempotencyRequests.WithLabelValues("hit"))
	if hitCount != 1.0 {
		t.Fatalf("Beklenen idempotency hit 1, gelen: %f", hitCount)
	}

	missCount := testutil.ToFloat64(m.IdempotencyRequests.WithLabelValues("miss"))
	if missCount != 1.0 {
		t.Fatalf("Beklenen idempotency miss 1, gelen: %f", missCount)
	}

	reconCount := testutil.ToFloat64(m.ReconciliationErrors.WithLabelValues("unbalanced"))
	if reconCount != 1.0 {
		t.Fatalf("Beklenen bakiye mutabakat hatası 1, gelen: %f", reconCount)
	}

	activeCount := testutil.ToFloat64(m.ActiveTransactions)
	if activeCount != 5.0 {
		t.Fatalf("Beklenen aktif işlem 5, gelen: %f", activeCount)
	}
}

func TestOpenTelemetry_Tracer_SpanCreation(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer

	tp, shutdown, err := observability.InitTracer(ctx, "financial-ledger-test", &buf)
	if err != nil {
		t.Fatalf("Tracer başlatılamadı: %v", err)
	}
	defer func() { _ = shutdown(ctx) }()

	tracer := observability.Tracer()
	ctx, span := tracer.Start(ctx, "TransferFunds")
	span.SetAttributes(
		attribute.String("transaction.id", "tx-test-trace-1"),
		attribute.String("currency", "USD"),
		attribute.Int64("amount", 1000),
		attribute.String("locking.strategy", "pessimistic"),
	)
	span.End()

	// Flush spans
	_ = tp.ForceFlush(ctx)

	output := buf.String()
	if !strings.Contains(output, "TransferFunds") {
		t.Fatalf("Trace çıktısında 'TransferFunds' span adı bulunamadı. Çıktı:\n%s", output)
	}
	if !strings.Contains(output, "tx-test-trace-1") {
		t.Fatalf("Trace çıktısında 'tx-test-trace-1' attribute bulunamadı. Çıktı:\n%s", output)
	}
}
