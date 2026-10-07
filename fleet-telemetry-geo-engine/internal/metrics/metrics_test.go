package metrics

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestPrometheusMetrics_RegistrationAndScrape(t *testing.T) {
	m := Default()

	// Simulate metric updates
	m.ActiveConnections.Inc()
	m.LocationPings.WithLabelValues("received").Add(42)
	m.BatchFlushDuration.Observe(0.015)
	m.RingBufferDepth.Set(5)
	m.BatchFlushRecords.Add(250)

	server := StartMetricsServer(":29191", nil)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Stop(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	// Test /healthz endpoint
	resp, err := http.Get("http://localhost:29191/healthz")
	if err != nil {
		t.Fatalf("Failed to GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200 for /healthz, got %d", resp.StatusCode)
	}

	// Test /metrics endpoint
	respMetrics, err := http.Get("http://localhost:29191/metrics")
	if err != nil {
		t.Fatalf("Failed to GET /metrics: %v", err)
	}
	defer respMetrics.Body.Close()

	body, err := io.ReadAll(respMetrics.Body)
	if err != nil {
		t.Fatalf("Failed reading /metrics body: %v", err)
	}

	bodyStr := string(body)
	expectedMetrics := []string{
		"active_connections_count",
		"location_pings_total",
		"batch_flush_duration_seconds",
		"ring_buffer_depth",
		"batch_flush_records_total",
	}

	for _, metricName := range expectedMetrics {
		if !strings.Contains(bodyStr, metricName) {
			t.Errorf("Expected Prometheus scraped output to contain %q", metricName)
		}
	}
}
