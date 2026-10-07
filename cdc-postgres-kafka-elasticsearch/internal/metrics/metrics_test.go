package metrics

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestMetrics_RecordingAndEndpoint(t *testing.T) {
	// 1. Record metrics
	RecordEventConsumed("success")
	RecordEventConsumed("failure")
	RecordBulkFlush(0.045)
	SetBatchSize(350)
	RecordE2ELatency(0.085) // 85ms
	RecordDLQEvent("MALFORMED_JSON")
	SetCircuitBreakerState(false)

	// 2. Start metrics server on random test port
	testPort := 21199
	testAddr := fmt.Sprintf("127.0.0.1:%d", testPort)
	srv := StartMetricsServer(testAddr)
	defer ShutdownMetricsServer(srv)

	// Give a small pause to bind
	time.Sleep(50 * time.Millisecond)

	// 3. Query /metrics
	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", testAddr))
	if err != nil {
		t.Fatalf("failed to query /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	bodyStr := string(body)

	// 4. Assert key metrics are present in Prometheus output
	expectedMetrics := []string{
		"cdc_events_consumed_total",
		"cdc_bulk_flush_duration_seconds",
		"cdc_batch_size",
		"cdc_e2e_latency_seconds",
		"cdc_dlq_events_total",
		"cdc_circuit_breaker_state",
	}

	for _, metricName := range expectedMetrics {
		if !strings.Contains(bodyStr, metricName) {
			t.Errorf("expected prometheus output to contain '%s'", metricName)
		}
	}
}
