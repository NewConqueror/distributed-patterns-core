package observability

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetrics_Counter_And_Histogram(t *testing.T) {
	reg := NewRegistry()

	// 1. Counter test
	reg.SemanticCacheHitsTotal.Inc()
	reg.SemanticCacheHitsTotal.Add(4)
	if reg.SemanticCacheHitsTotal.Get() != 5 {
		t.Fatalf("expected 5 cache hits, got %d", reg.SemanticCacheHitsTotal.Get())
	}

	reg.LLMTokensGeneratedTotal.Add(150, "openai-primary", "gpt-4o")
	if reg.LLMTokensGeneratedTotal.Get("openai-primary", "gpt-4o") != 150 {
		t.Fatalf("expected 150 tokens, got %d", reg.LLMTokensGeneratedTotal.Get("openai-primary", "gpt-4o"))
	}

	// 2. Histogram test (TTFT)
	reg.LLMTimeToFirstTokenSeconds.Observe(0.045, "openai-primary", "gpt-4o", "false") // 45ms
	reg.LLMTimeToFirstTokenSeconds.Observe(0.120, "openai-primary", "gpt-4o", "false") // 120ms

	// 3. Prometheus text exposition format
	var buf bytes.Buffer
	reg.WritePrometheusText(&buf)
	output := buf.String()

	if !strings.Contains(output, "semantic_cache_hits_total 5") {
		t.Errorf("expected semantic_cache_hits_total 5 in output, got:\n%s", output)
	}
	if !strings.Contains(output, `llm_tokens_generated_total{provider="openai-primary",model="gpt-4o"} 150`) {
		t.Errorf("expected tokens counter in output, got:\n%s", output)
	}
	if !strings.Contains(output, "llm_time_to_first_token_seconds_bucket") {
		t.Errorf("expected TTFT histogram in output, got:\n%s", output)
	}
}

func TestTracing_DistributedSpans(t *testing.T) {
	tracer := NewTracer(50)

	// Root span: HTTP request
	ctx, rootSpan := tracer.StartSpan(context.Background(), "HTTP POST /v1/chat/completions")
	rootSpan.SetAttribute("http.method", "POST")
	rootSpan.SetAttribute("http.path", "/v1/chat/completions")

	// Child span 1: TokenGuard
	ctxGuard, guardSpan := tracer.StartSpan(ctx, "TokenGuard.Evaluate")
	if guardSpan.TraceID != rootSpan.TraceID {
		t.Fatalf("expected child span to inherit TraceID, got %s vs %s", guardSpan.TraceID, rootSpan.TraceID)
	}
	if guardSpan.ParentSpanID != rootSpan.SpanID {
		t.Fatalf("expected guard span parent to be root span, got %s vs %s", guardSpan.ParentSpanID, rootSpan.SpanID)
	}
	guardSpan.End()

	// Child span 2: LLM Stream
	_, llmSpan := tracer.StartSpan(ctxGuard, "LLM.Stream")
	llmSpan.SetAttribute("llm.provider", "openai")
	llmSpan.AddEvent("first_token", map[string]any{"ttft_ms": 32.5})
	llmSpan.End()

	rootSpan.End()

	// Verify spans recorded
	spans := tracer.GetSpans(rootSpan.TraceID)
	if len(spans) != 3 {
		t.Fatalf("expected 3 completed spans for trace, got %d", len(spans))
	}

	// Verify W3C header format
	w3cHeader := FormatTraceparent(rootSpan)
	if !strings.HasPrefix(w3cHeader, "00-"+rootSpan.TraceID+"-") {
		t.Fatalf("invalid W3C header format: %s", w3cHeader)
	}
}

func TestProfilingRoutes(t *testing.T) {
	mux := http.NewServeMux()
	RegisterProfilingRoutes(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/goroutine?debug=1", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from pprof, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "goroutine profile:") {
		t.Fatalf("expected goroutine profile in body, got: %s", rec.Body.String())
	}
}
