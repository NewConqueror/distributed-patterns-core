package embedding

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOpenAIEmbedder_Embed_Success(t *testing.T) {
	mockVec := make([]float32, 1536)
	mockVec[0] = 0.5
	mockVec[1] = -0.5

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/embeddings" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("unexpected authorization header: %s", r.Header.Get("Authorization"))
		}

		resp := openAIEmbeddingResponse{
			Data: []openAIEmbeddingData{
				{
					Embedding: mockVec,
					Index:     0,
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	embedder := NewOpenAIEmbedder(OpenAIEmbedderConfig{
		APIKey:     "test-key",
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
	})

	if embedder.Dimension() != 1536 {
		t.Fatalf("expected dimension 1536, got %d", embedder.Dimension())
	}

	vec, err := embedder.Embed(context.Background(), "  Hello  World! ")
	if err != nil {
		t.Fatalf("unexpected embed error: %v", err)
	}

	if len(vec) != 1536 {
		t.Fatalf("expected vector length 1536, got %d", len(vec))
	}
	if vec[0] != 0.5 || vec[1] != -0.5 {
		t.Fatalf("unexpected vector values: %v, %v", vec[0], vec[1])
	}
}

func TestMockEmbedder_Deterministic(t *testing.T) {
	mock := NewMockEmbedder(128)

	vec1, err := mock.Embed(context.Background(), "how is the weather?")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Different casing and spaces should normalize to the exact same embedding
	vec2, err := mock.Embed(context.Background(), "   How IS the   weather?   ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(vec1) != 128 || len(vec2) != 128 {
		t.Fatalf("expected length 128, got %d, %d", len(vec1), len(vec2))
	}

	for i := range vec1 {
		if vec1[i] != vec2[i] {
			t.Fatalf("expected matching vector components at %d: %f vs %f", i, vec1[i], vec2[i])
		}
	}
}
