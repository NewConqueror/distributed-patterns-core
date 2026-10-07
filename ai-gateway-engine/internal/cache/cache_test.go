package cache_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/cache"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/cache/memory"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/embedding"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/sse"
)

type mockFlushRecorder struct {
	*httptest.ResponseRecorder
}

func (m *mockFlushRecorder) Flush() {}

func TestMemoryStore_CosineDistance_And_Threshold(t *testing.T) {
	store := memory.NewStore()

	// Vector A: [1.0, 0.0]
	// Vector B: [0.999, 0.01] -> very close to A (distance ~ 0.0001)
	// Vector C: [0.0, 1.0] -> orthogonal to A (distance = 1.0)
	vecA := []float32{1.0, 0.0}
	vecB := []float32{0.999, 0.01}
	vecC := []float32{0.0, 1.0}

	ctx := context.Background()
	err := store.Store(ctx, "What is Go?", vecA, "Go is an open-source programming language.", nil)
	if err != nil {
		t.Fatalf("failed to store item: %v", err)
	}

	// 1. Search with similar vector B (should hit, distance < 0.08)
	hit, err := store.FindSimilar(ctx, vecB, 0.08)
	if err != nil {
		t.Fatalf("unexpected error finding similar: %v", err)
	}
	if hit == nil {
		t.Fatal("expected cache hit for similar vector, got nil")
	}
	if hit.ResponseText != "Go is an open-source programming language." {
		t.Fatalf("unexpected response text: %s", hit.ResponseText)
	}
	if hit.Distance >= 0.08 || hit.Similarity < 0.92 {
		t.Fatalf("expected distance < 0.08 and similarity >= 0.92, got dist=%f sim=%f", hit.Distance, hit.Similarity)
	}

	// 2. Search with orthogonal vector C (should miss, distance > 0.08)
	miss, err := store.FindSimilar(ctx, vecC, 0.08)
	if err != nil {
		t.Fatalf("unexpected error finding orthogonal: %v", err)
	}
	if miss != nil {
		t.Fatalf("expected cache miss for orthogonal vector, got %+v", miss)
	}
}

func TestReplayEngine_StreamsChunks(t *testing.T) {
	replay := cache.NewReplayEngine(cache.ReplayConfig{
		ChunkDelay:    5 * time.Millisecond,
		MaxChunkWords: 2,
	})

	rec := &mockFlushRecorder{ResponseRecorder: httptest.NewRecorder()}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	writer, err := sse.NewWriter(rec, req)
	if err != nil {
		t.Fatalf("failed to create sse writer: %v", err)
	}

	item := &cache.CachedItem{
		ID:           "test-cache-id",
		PromptText:   "hello world",
		ResponseText: "Hello there! How can I help you today?",
		Similarity:   0.98,
		Distance:     0.02,
	}

	err = replay.Replay(context.Background(), item, writer)
	if err != nil {
		t.Fatalf("unexpected replay error: %v", err)
	}

	output := rec.Body.String()
	if !strings.Contains(output, "event: cache_hit") {
		t.Errorf("expected cache_hit event, got: %s", output)
	}
	if !strings.Contains(output, "Hello there!") {
		t.Errorf("expected cached words in output, got: %s", output)
	}
	if !strings.Contains(output, "data: [DONE]") {
		t.Errorf("expected [DONE] in output, got: %s", output)
	}
}

func TestAsyncWriter_PersistsInBackground(t *testing.T) {
	store := memory.NewStore()
	embedder := embedding.NewMockEmbedder(4)
	writer := cache.NewAsyncWriter(store, embedder, 10, 1)
	defer writer.Stop()

	enqueued := writer.Enqueue(&cache.WriteTask{
		PromptText:   "golang async test",
		ResponseText: "async success",
	})
	if !enqueued {
		t.Fatal("failed to enqueue write task")
	}

	// Give worker time to process task
	time.Sleep(50 * time.Millisecond)

	emb, _ := embedder.Embed(context.Background(), "golang async test")
	hit, err := store.FindSimilar(context.Background(), emb, 0.05)
	if err != nil {
		t.Fatalf("unexpected search error: %v", err)
	}
	if hit == nil || hit.ResponseText != "async success" {
		t.Fatalf("expected async task to persist response, got %+v", hit)
	}
}
