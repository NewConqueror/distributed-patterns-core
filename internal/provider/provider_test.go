package provider

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
)

func TestOpenAIProvider_Stream_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("missing or invalid authorization header")
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("missing or invalid content-type")
		}

		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("expected flusher")
		}

		fmt.Fprint(w, "data: {\"id\":\"1\",\"model\":\"gpt-4o\",\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n")
		flusher.Flush()
		fmt.Fprint(w, "data: {\"id\":\"2\",\"model\":\"gpt-4o\",\"choices\":[{\"delta\":{\"content\":\" world\"}}]}\n\n")
		flusher.Flush()
		fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer server.Close()

	p := NewOpenAIProvider(OpenAIConfig{
		ID:         "openai-test",
		APIKey:     "test-key",
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	chunkChan, errChan := p.Stream(ctx, models.CompletionRequest{
		Model: "gpt-4o",
		Messages: []models.Message{
			{Role: models.RoleUser, Content: "hi"},
		},
	})

	var result string
	for chunk := range chunkChan {
		result += chunk.DeltaContent
	}

	if err := <-errChan; err != nil {
		t.Fatalf("unexpected stream error: %v", err)
	}

	if result != "Hello world" {
		t.Fatalf("expected 'Hello world', got %q", result)
	}
}

func TestOpenAIProvider_Stream_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":{"message":"Rate limit exceeded"}}`))
	}))
	defer server.Close()

	p := NewOpenAIProvider(OpenAIConfig{
		ID:         "openai-test",
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
	})

	chunkChan, errChan := p.Stream(context.Background(), models.CompletionRequest{
		Model: "gpt-4o",
	})

	for range chunkChan {
	}

	err := <-errChan
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	upErr, ok := err.(*models.UpstreamError)
	if !ok {
		t.Fatalf("expected UpstreamError, got %T", err)
	}

	if upErr.StatusCode != 429 || !upErr.Retryable {
		t.Fatalf("expected 429 and retryable=true, got %+v", upErr)
	}
}

func TestOllamaProvider_Stream_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("expected flusher")
		}

		fmt.Fprintln(w, `{"model":"llama3","message":{"role":"assistant","content":"Ollama"},"done":false}`)
		flusher.Flush()
		fmt.Fprintln(w, `{"model":"llama3","message":{"role":"assistant","content":" rocks!"},"done":true,"prompt_eval_count":10,"eval_count":5}`)
		flusher.Flush()
	}))
	defer server.Close()

	p := NewOllamaProvider(OllamaConfig{
		ID:         "ollama-test",
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
	})

	chunkChan, errChan := p.Stream(context.Background(), models.CompletionRequest{
		Model: "llama3",
		Messages: []models.Message{
			{Role: models.RoleUser, Content: "hi"},
		},
	})

	var result string
	var finalUsage *models.UsageMetadata
	for chunk := range chunkChan {
		result += chunk.DeltaContent
		if chunk.Usage != nil {
			finalUsage = chunk.Usage
		}
	}

	if err := <-errChan; err != nil {
		t.Fatalf("unexpected stream error: %v", err)
	}

	if result != "Ollama rocks!" {
		t.Fatalf("expected 'Ollama rocks!', got %q", result)
	}

	if finalUsage == nil || finalUsage.TotalTokens != 15 {
		t.Fatalf("expected usage total tokens 15, got %+v", finalUsage)
	}
}

func TestMockProvider_MidStreamError(t *testing.T) {
	p := NewMockProvider("mock",
		WithMockChunks([]models.StreamChunk{
			{DeltaContent: "token1 "},
			{DeltaContent: "token2 "},
			{DeltaContent: "token3 "},
		}),
		WithMockMidStreamError(&models.UpstreamError{StatusCode: 500, Message: "upstream crash"}, 2),
	)

	chunkChan, errChan := p.Stream(context.Background(), models.CompletionRequest{})

	var received []string
	for ch := range chunkChan {
		received = append(received, ch.DeltaContent)
	}

	err := <-errChan
	if err == nil {
		t.Fatal("expected mid-stream error, got nil")
	}

	if len(received) != 2 {
		t.Fatalf("expected 2 tokens before failure, got %d", len(received))
	}
}
