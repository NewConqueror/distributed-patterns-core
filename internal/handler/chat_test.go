package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/orchestrator"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/provider"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
)

type flushRecorder struct {
	*httptest.ResponseRecorder
	flushed bool
}

func (f *flushRecorder) Flush() {
	f.flushed = true
}

func TestGatewayHandler_ChatCompletions_Success(t *testing.T) {
	mockProv := provider.NewMockProvider("mock",
		provider.WithMockChunks([]models.StreamChunk{
			{DeltaContent: "Go "},
			{DeltaContent: "Rules!"},
		}),
	)

	router, err := orchestrator.NewRouter(orchestrator.RouterConfig{
		PrimaryProvider: mockProv,
	})
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	h := NewGatewayHandler(router)

	bodyBytes, _ := json.Marshal(models.CompletionRequest{
		Model: "test-model",
		Messages: []models.Message{
			{Role: models.RoleUser, Content: "Hello"},
		},
		Stream: true,
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(bodyBytes))
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}

	h.ChatCompletions(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	if rec.Header().Get("Content-Type") != "text/event-stream" {
		t.Errorf("expected text/event-stream, got %s", rec.Header().Get("Content-Type"))
	}

	output := rec.Body.String()
	if !strings.Contains(output, "Go ") || !strings.Contains(output, "Rules!") {
		t.Errorf("expected output to contain 'Go Rules!', got: %s", output)
	}
	if !strings.Contains(output, "data: [DONE]") {
		t.Errorf("expected output to contain [DONE], got: %s", output)
	}
}

func TestGatewayHandler_ChatCompletions_BadRequest(t *testing.T) {
	router, _ := orchestrator.NewRouter(orchestrator.RouterConfig{
		PrimaryProvider: provider.NewMockProvider("mock"),
	})
	h := NewGatewayHandler(router)

	// Empty body
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`))
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}

	h.ChatCompletions(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 for empty messages, got %d", rec.Code)
	}
}

func TestGatewayHandler_ChatCompletions_MethodNotAllowed(t *testing.T) {
	router, _ := orchestrator.NewRouter(orchestrator.RouterConfig{
		PrimaryProvider: provider.NewMockProvider("mock"),
	})
	h := NewGatewayHandler(router)

	req := httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil)
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}

	h.ChatCompletions(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405, got %d", rec.Code)
	}
}

func TestGatewayHandler_ChatCompletions_ClientDisconnect(t *testing.T) {
	mockProv := provider.NewMockProvider("mock",
		provider.WithMockChunks([]models.StreamChunk{
			{DeltaContent: "chunk 1"},
			{DeltaContent: "chunk 2"},
		}),
		provider.WithMockDelay(100*time.Millisecond),
	)

	router, _ := orchestrator.NewRouter(orchestrator.RouterConfig{
		PrimaryProvider: mockProv,
	})
	h := NewGatewayHandler(router)

	ctx, cancel := context.WithCancel(context.Background())
	bodyBytes, _ := json.Marshal(models.CompletionRequest{
		Model: "test-model",
		Messages: []models.Message{
			{Role: models.RoleUser, Content: "Hello"},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(bodyBytes)).WithContext(ctx)
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}

	// Abort request after 30ms
	time.AfterFunc(30*time.Millisecond, cancel)

	h.ChatCompletions(rec, req)
	// Should return cleanly without panicking or hanging
}
