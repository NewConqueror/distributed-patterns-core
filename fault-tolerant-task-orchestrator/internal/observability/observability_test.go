package observability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"DistributedFault-Tolerant_TaskOrchestrator/internal/task"
)

func TestMetrics_Collection(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	// Increment and record values
	m.IncActiveWorkers()
	m.SetQueueDepth(42)

	m.RecordJobCompleted("email_task", 150*time.Millisecond)
	m.RecordJobFailed("flaky_task", 50*time.Millisecond)
	m.RecordJobDLQ("corrupt_task")
	m.RecordJobRetry("retry_task")

	// Gather metrics from isolated registry
	gathered, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	foundMetrics := make(map[string]bool)
	for _, mf := range gathered {
		foundMetrics[mf.GetName()] = true
	}

	expected := []string{
		"task_orchestrator_jobs_total",
		"task_orchestrator_job_duration_seconds",
		"task_orchestrator_active_workers",
		"task_orchestrator_queue_depth",
	}

	for _, name := range expected {
		if !foundMetrics[name] {
			t.Errorf("expected metric %s to be present in gathered metrics", name)
		}
	}
}

func TestTracingManager_ContextPropagation(t *testing.T) {
	// Setup in-memory SpanRecorder to capture exported spans
	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))

	tm := NewTracingManager(tp)

	// 1. Simulate Producer generating a span
	producerCtx, producerSpan := tp.Tracer("producer-tracer").Start(context.Background(), "producer_enqueue")
	producerTraceID := producerSpan.SpanContext().TraceID().String()

	job := task.NewJob("job-trace-1", "payment_process", []byte("data"), 2)

	// Inject trace context into job.Metadata
	tm.InjectTraceContext(producerCtx, job)
	producerSpan.End()

	// Verify trace context was injected into job metadata (W3C traceparent)
	traceparent := job.GetMetadata("traceparent")
	if traceparent == "" {
		t.Fatalf("expected traceparent header to be present in job metadata")
	}
	if !strings.Contains(traceparent, producerTraceID) {
		t.Fatalf("traceparent header should contain producer trace ID %s, got: %s", producerTraceID, traceparent)
	}

	// 2. Simulate Worker extracting context and starting child span
	workerCtx, workerSpan := tm.StartJobSpan(context.Background(), job, "worker-node-1")
	if !workerSpan.SpanContext().IsValid() {
		t.Fatalf("worker span should be valid")
	}

	// Child span should share the exact same TraceID as the Producer!
	if workerSpan.SpanContext().TraceID().String() != producerTraceID {
		t.Fatalf("worker span trace ID (%s) does not match producer trace ID (%s)",
			workerSpan.SpanContext().TraceID().String(), producerTraceID)
	}

	// 3. Record Error on Span
	simulatedErr := errors.New("upstream payment gateway timeout")
	tm.RecordSpanError(workerSpan, simulatedErr)
	workerSpan.End()

	_ = workerCtx

	// 4. Verify recorded spans
	endedSpans := spanRecorder.Ended()
	if len(endedSpans) < 2 {
		t.Fatalf("expected at least 2 recorded spans, got %d", len(endedSpans))
	}

	foundWorkerSpan := false
	for _, s := range endedSpans {
		if s.Name() == "job:payment_process" {
			foundWorkerSpan = true
			if len(s.Events()) == 0 {
				t.Fatalf("expected error event on worker span")
			}
			if s.Status().Code != codes.Error {
				t.Fatalf("expected span status to be codes.Error, got: %v", s.Status().Code)
			}
		}
	}
	if !foundWorkerSpan {
		t.Fatalf("worker span 'job:payment_process' was not found in recorded spans")
	}
}

func TestObservabilityServer_Endpoints(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer s.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer rdb.Close()

	reg := prometheus.NewRegistry()
	metrics := NewMetrics(reg)
	metrics.RecordJobCompleted("demo_job", 100*time.Millisecond)

	cfg := ServerConfig{
		Addr: "127.0.0.1:0", // dynamic free port
	}
	srv := NewServer(cfg, metrics, rdb)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start observability server: %v", err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	baseURL := fmt.Sprintf("http://%s", srv.Addr())

	// 1. Test /healthz (Liveness)
	resp, err := http.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for /healthz, got %d", resp.StatusCode)
	}

	var liveness HealthStatus
	if err := json.NewDecoder(resp.Body).Decode(&liveness); err != nil {
		t.Fatalf("failed to parse /healthz response: %v", err)
	}
	if liveness.Status != "healthy" {
		t.Fatalf("expected status 'healthy', got: %s", liveness.Status)
	}

	// 2. Test /ready (Readiness - Redis Up)
	readyResp, err := http.Get(baseURL + "/ready")
	if err != nil {
		t.Fatalf("GET /ready failed: %v", err)
	}
	defer readyResp.Body.Close()
	if readyResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for /ready, got %d", readyResp.StatusCode)
	}

	var readiness HealthStatus
	if err := json.NewDecoder(readyResp.Body).Decode(&readiness); err != nil {
		t.Fatalf("failed to parse /ready response: %v", err)
	}
	if readiness.Status != "ready" || readiness.Checks["redis"] != "up" {
		t.Fatalf("expected status 'ready' and redis 'up', got status=%s, redis=%s", readiness.Status, readiness.Checks["redis"])
	}

	// 3. Test /ready with Redis Down
	s.Close() // Kill redis
	readyDownResp, err := http.Get(baseURL + "/ready")
	if err != nil {
		t.Fatalf("GET /ready after redis shutdown failed: %v", err)
	}
	defer readyDownResp.Body.Close()
	if readyDownResp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable when redis is down, got: %d", readyDownResp.StatusCode)
	}

	// 4. Test /metrics (Prometheus scrape)
	metricsResp, err := http.Get(baseURL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer metricsResp.Body.Close()
	if metricsResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for /metrics, got %d", metricsResp.StatusCode)
	}

	bodyBytes, _ := io.ReadAll(metricsResp.Body)
	bodyStr := string(bodyBytes)
	if !strings.Contains(bodyStr, "task_orchestrator_jobs_total") {
		t.Fatalf("expected /metrics response to contain 'task_orchestrator_jobs_total', got: %s", bodyStr)
	}
}
