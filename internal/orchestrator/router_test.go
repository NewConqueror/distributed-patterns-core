package orchestrator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/provider"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/sse"
)

type mockResponseWriter struct {
	*httptest.ResponseRecorder
}

func (m *mockResponseWriter) Flush() {}

func TestRouter_PrimarySuccess(t *testing.T) {
	primary := provider.NewMockProvider("primary",
		provider.WithMockChunks([]models.StreamChunk{
			{DeltaContent: "Hello "},
			{DeltaContent: "world!"},
		}),
	)

	router, err := NewRouter(RouterConfig{
		PrimaryProvider: primary,
	})
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	rec := &mockResponseWriter{ResponseRecorder: httptest.NewRecorder()}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	sseWriter, err := sse.NewWriter(rec, req)
	if err != nil {
		t.Fatalf("failed to create sse writer: %v", err)
	}

	err = router.StreamResponse(context.Background(), models.CompletionRequest{}, sseWriter)
	if err != nil {
		t.Fatalf("unexpected stream error: %v", err)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Hello ") || !strings.Contains(body, "world!") {
		t.Errorf("expected body to contain Hello world!, got: %s", body)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Errorf("expected body to contain [DONE], got: %s", body)
	}
}

func TestRouter_PreStreamFallback(t *testing.T) {
	// Primary fails immediately (pre-stream, 0 tokens)
	primary := provider.NewMockProvider("primary",
		provider.WithMockInitialError(&models.UpstreamError{
			StatusCode: 500,
			Provider:   "primary",
			Message:    "internal server error",
			Retryable:  true,
		}),
	)

	fallback := provider.NewMockProvider("fallback-ollama",
		provider.WithMockChunks([]models.StreamChunk{
			{DeltaContent: "Recovered from fallback"},
		}),
	)

	router, err := NewRouter(RouterConfig{
		PrimaryProvider:   primary,
		FallbackProviders: []provider.LLMProvider{fallback},
	})
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	rec := &mockResponseWriter{ResponseRecorder: httptest.NewRecorder()}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	sseWriter, err := sse.NewWriter(rec, req)
	if err != nil {
		t.Fatalf("failed to create sse writer: %v", err)
	}

	err = router.StreamResponse(context.Background(), models.CompletionRequest{}, sseWriter)
	if err != nil {
		t.Fatalf("unexpected stream error: %v", err)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Recovered from fallback") {
		t.Fatalf("expected fallback output in body, got: %s", body)
	}

	// Verify circuit breaker registered the failure on primary
	primaryCB, _ := router.GetCircuitBreaker("primary")
	if primaryCB.State() != StateClosed { // only 1 failure, threshold is 3
		t.Errorf("circuit breaker state expected closed after 1 failure, got %s", primaryCB.State())
	}
}

func TestRouter_CircuitBreakerSkipsToFallback(t *testing.T) {
	primary := provider.NewMockProvider("primary",
		provider.WithMockInitialError(&models.UpstreamError{
			StatusCode: 503,
			Provider:   "primary",
			Message:    "service unavailable",
			Retryable:  true,
		}),
	)

	fallback := provider.NewMockProvider("fallback-ollama",
		provider.WithMockChunks([]models.StreamChunk{
			{DeltaContent: "fallback-ok"},
		}),
	)

	router, err := NewRouter(RouterConfig{
		PrimaryProvider:   primary,
		FallbackProviders: []provider.LLMProvider{fallback},
		CircuitBreakerCfg: CircuitBreakerConfig{
			WindowSize:       5,
			FailureThreshold: 2,
			OpenCooldown:     5 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	// Trip the breaker with 2 failures
	for i := 0; i < 2; i++ {
		rec := &mockResponseWriter{ResponseRecorder: httptest.NewRecorder()}
		sseWriter, _ := sse.NewWriter(rec, httptest.NewRequest(http.MethodPost, "/", nil))
		_ = router.StreamResponse(context.Background(), models.CompletionRequest{}, sseWriter)
	}

	primaryCB, _ := router.GetCircuitBreaker("primary")
	if primaryCB.State() != StateOpen {
		t.Fatalf("expected primary circuit breaker to be OPEN, got %s", primaryCB.State())
	}

	// 3rd request: Primary should be skipped directly due to OPEN breaker, fallback served
	rec := &mockResponseWriter{ResponseRecorder: httptest.NewRecorder()}
	sseWriter, _ := sse.NewWriter(rec, httptest.NewRequest(http.MethodPost, "/", nil))
	err = router.StreamResponse(context.Background(), models.CompletionRequest{}, sseWriter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(rec.Body.String(), "fallback-ok") {
		t.Fatalf("expected fallback output, got: %s", rec.Body.String())
	}
}

func TestRouter_MidStreamFallback(t *testing.T) {
	// Primary emits 2 tokens, then fails
	primary := provider.NewMockProvider("primary",
		provider.WithMockChunks([]models.StreamChunk{
			{DeltaContent: "Part 1 "},
			{DeltaContent: "Part 2 "},
			{DeltaContent: "Part 3 "},
		}),
		provider.WithMockMidStreamError(&models.UpstreamError{
			StatusCode: 500,
			Message:    "connection reset by peer",
			Retryable:  true,
		}, 2),
	)

	fallback := provider.NewMockProvider("fallback-ollama",
		provider.WithMockChunks([]models.StreamChunk{
			{DeltaContent: "Continued from Ollama"},
		}),
	)

	router, err := NewRouter(RouterConfig{
		PrimaryProvider:   primary,
		FallbackProviders: []provider.LLMProvider{fallback},
	})
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	rec := &mockResponseWriter{ResponseRecorder: httptest.NewRecorder()}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	sseWriter, err := sse.NewWriter(rec, req)
	if err != nil {
		t.Fatalf("failed to create sse writer: %v", err)
	}

	err = router.StreamResponse(context.Background(), models.CompletionRequest{}, sseWriter)
	if err != nil {
		t.Fatalf("unexpected stream error: %v", err)
	}

	body := rec.Body.String()

	// Should contain the first 2 tokens
	if !strings.Contains(body, "Part 1 ") || !strings.Contains(body, "Part 2 ") {
		t.Errorf("expected initial tokens in body, got: %s", body)
	}

	// Should contain midstream_fallback event
	if !strings.Contains(body, "event: midstream_fallback") {
		t.Errorf("expected event: midstream_fallback in body, got: %s", body)
	}

	// Should contain the fallback response
	if !strings.Contains(body, "Continued from Ollama") {
		t.Errorf("expected fallback tokens in body, got: %s", body)
	}

	// Should finish with [DONE]
	if !strings.Contains(body, "data: [DONE]") {
		t.Errorf("expected [DONE] in body, got: %s", body)
	}
}

func TestRouter_ClientDisconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	primary := provider.NewMockProvider("primary",
		provider.WithMockChunks([]models.StreamChunk{
			{DeltaContent: "slow chunk 1"},
			{DeltaContent: "slow chunk 2"},
		}),
		provider.WithMockDelay(50*time.Millisecond),
	)

	router, err := NewRouter(RouterConfig{
		PrimaryProvider: primary,
	})
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	rec := &mockResponseWriter{ResponseRecorder: httptest.NewRecorder()}
	req := httptest.NewRequest(http.MethodPost, "/", nil).WithContext(ctx)
	sseWriter, _ := sse.NewWriter(rec, req)

	// Cancel context after 20ms (client aborted)
	time.AfterFunc(20*time.Millisecond, cancel)

	err = router.StreamResponse(ctx, models.CompletionRequest{}, sseWriter)
	if err != sse.ErrClientDisconnected {
		t.Fatalf("expected ErrClientDisconnected, got %v", err)
	}
}
