package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/cache"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/cache/memory"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/embedding"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/handler"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/orchestrator"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/provider"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
)

type noopFlusherRecorder struct {
	*httptest.ResponseRecorder
}

func (r *noopFlusherRecorder) Flush() {}

func setupBenchmarkGateway(primaryErrRate float64) (*handler.GatewayHandler, *cache.AsyncWriter) {
	// Primary provider with optional chaos failure rate
	primary := provider.NewMockProvider("primary",
		provider.WithMockChunks([]models.StreamChunk{
			{DeltaContent: "TokenA "},
			{DeltaContent: "TokenB "},
		}),
	)

	// Chaos primary provider wrapper if error rate > 0
	var primaryProvider provider.LLMProvider = primary
	if primaryErrRate > 0 {
		primaryProvider = &chaosProvider{
			id:         "primary-chaos",
			delegate:   primary,
			failRate:   primaryErrRate,
			statusCode: 503,
		}
	}

	fallback := provider.NewMockProvider("fallback-ollama",
		provider.WithMockChunks([]models.StreamChunk{
			{DeltaContent: "FallbackToken "},
		}),
	)

	router, _ := orchestrator.NewRouter(orchestrator.RouterConfig{
		PrimaryProvider:   primaryProvider,
		FallbackProviders: []provider.LLMProvider{fallback},
		CircuitBreakerCfg: orchestrator.CircuitBreakerConfig{
			WindowSize:       10,
			FailureThreshold: 3,
			OpenCooldown:     100 * time.Millisecond,
		},
	})

	vectorStore := memory.NewStore()
	embedder := embedding.NewMockEmbedder(1536)
	asyncWriter := cache.NewAsyncWriter(vectorStore, embedder, 2000, 4)

	replay := cache.NewReplayEngine(cache.ReplayConfig{
		ChunkDelay: 1 * time.Millisecond,
	})

	h := handler.NewGatewayHandlerWithConfig(handler.GatewayHandlerConfig{
		Router:         router,
		VectorStore:    vectorStore,
		Embedder:       embedder,
		Replay:         replay,
		AsyncWriter:    asyncWriter,
		CacheThreshold: 0.08,
	})

	return h, asyncWriter
}

type chaosProvider struct {
	id         string
	delegate   provider.LLMProvider
	failRate   float64
	statusCode int
	mu         sync.Mutex
	rng        *rand.Rand
}

func (c *chaosProvider) ID() string {
	return c.id
}

func (c *chaosProvider) Stream(ctx context.Context, req models.CompletionRequest) (<-chan models.StreamChunk, <-chan error) {
	c.mu.Lock()
	if c.rng == nil {
		c.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	fail := c.rng.Float64() < c.failRate
	c.mu.Unlock()

	if fail {
		ch := make(chan models.StreamChunk)
		close(ch)
		errCh := make(chan error, 1)
		errCh <- &models.UpstreamError{
			StatusCode: c.statusCode,
			Provider:   c.id,
			Message:    "chaos random failure injected",
			Retryable:  true,
		}
		close(errCh)
		return ch, errCh
	}

	return c.delegate.Stream(ctx, req)
}

// Senaryo A: 200 eşzamanlı istek, tamamı Cache Miss
func TestScenarioA_200Concurrent_CacheMiss(t *testing.T) {
	// Start with empty store and don't write during test to guarantee 100% cache miss
	primary := provider.NewMockProvider("primary",
		provider.WithMockChunks([]models.StreamChunk{
			{DeltaContent: "TokenA "},
			{DeltaContent: "TokenB "},
		}),
	)
	router, _ := orchestrator.NewRouter(orchestrator.RouterConfig{
		PrimaryProvider: primary,
	})

	vectorStore := memory.NewStore()
	embedder := embedding.NewMockEmbedder(1536)

	h := handler.NewGatewayHandlerWithConfig(handler.GatewayHandlerConfig{
		Router:         router,
		VectorStore:    vectorStore,
		Embedder:       embedder,
		CacheThreshold: 0.08,
	})

	concurrent := 200
	var wg sync.WaitGroup
	wg.Add(concurrent)

	latencies := make([]time.Duration, concurrent)
	startAll := time.Now()

	for i := 0; i < concurrent; i++ {
		idx := i
		go func() {
			defer wg.Done()

			reqBody, _ := json.Marshal(models.CompletionRequest{
				Model: "gpt-4o",
				Messages: []models.Message{
					{Role: models.RoleUser, Content: fmt.Sprintf("Unique query %d to force cache miss", idx)},
				},
				Stream: true,
			})

			start := time.Now()
			rec := &noopFlusherRecorder{ResponseRecorder: httptest.NewRecorder()}
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(reqBody))

			h.ChatCompletions(rec, req)
			latencies[idx] = time.Since(start)

			if rec.Header().Get("X-Cache") != "MISS" {
				t.Errorf("expected X-Cache MISS on query %d, got %s", idx, rec.Header().Get("X-Cache"))
			}
		}()
	}

	wg.Wait()
	totalDuration := time.Since(startAll)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[int(float64(concurrent)*0.50)]
	p99 := latencies[int(float64(concurrent)*0.99)]

	t.Logf("[Senaryo A - 200 Concurrent Cache Misses] Total: %v, p50: %v, p99: %v, Throughput: %.1f req/s",
		totalDuration, p50, p99, float64(concurrent)/totalDuration.Seconds())
}

// Senaryo B: 1000 eşzamanlı istek, %50 Cache Hit oranı
func TestScenarioB_1000Requests_50PercentCacheHit(t *testing.T) {
	h, asyncWriter := setupBenchmarkGateway(0.0)
	defer asyncWriter.Stop()

	// Pre-populate 5 distinct prompts into cache
	for i := 0; i < 5; i++ {
		prompt := fmt.Sprintf("Cached question %d", i)
		reqBody, _ := json.Marshal(models.CompletionRequest{
			Model: "gpt-4o",
			Messages: []models.Message{
				{Role: models.RoleUser, Content: prompt},
			},
			Stream: true,
		})
		rec := &noopFlusherRecorder{ResponseRecorder: httptest.NewRecorder()}
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(reqBody))
		h.ChatCompletions(rec, req)
	}

	// Give async worker time to persist entries
	time.Sleep(50 * time.Millisecond)

	totalRequests := 1000
	var wg sync.WaitGroup
	wg.Add(totalRequests)

	hitCount := 0
	var countMu sync.Mutex
	latencies := make([]time.Duration, totalRequests)
	startAll := time.Now()

	for i := 0; i < totalRequests; i++ {
		idx := i
		isHitQuery := (i % 2 == 0) // 50% hit, 50% miss

		go func() {
			defer wg.Done()

			var prompt string
			if isHitQuery {
				prompt = fmt.Sprintf("Cached question %d", idx%5)
			} else {
				prompt = fmt.Sprintf("Brand new miss query %d", idx)
			}

			reqBody, _ := json.Marshal(models.CompletionRequest{
				Model: "gpt-4o",
				Messages: []models.Message{
					{Role: models.RoleUser, Content: prompt},
				},
				Stream: true,
			})

			start := time.Now()
			rec := &noopFlusherRecorder{ResponseRecorder: httptest.NewRecorder()}
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(reqBody))

			h.ChatCompletions(rec, req)
			elapsed := time.Since(start)
			latencies[idx] = elapsed

			if rec.Header().Get("X-Cache") == "HIT" {
				countMu.Lock()
				hitCount++
				countMu.Unlock()
			}
		}()
	}

	wg.Wait()
	totalDuration := time.Since(startAll)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[int(float64(totalRequests)*0.50)]
	p99 := latencies[int(float64(totalRequests)*0.99)]

	t.Logf("[Senaryo B - 1000 Requests 50%% Cache Hit] Hits: %d/%d (%.1f%%), Total: %v, p50: %v, p99: %v",
		hitCount, totalRequests, float64(hitCount)/float64(totalRequests)*100, totalDuration, p50, p99)

	if hitCount < 400 {
		t.Fatalf("expected at least ~50%% cache hits, got %d", hitCount)
	}
}

// Senaryo C (Chaos): Birincil sağlayıcı %30 hata verirken p99 gecikmesi ve devre kesici devreye girişi
func TestScenarioC_Chaos_30PercentError_FallbackLatency(t *testing.T) {
	// Primary fails 30% of the time, fallback router recovers
	h, asyncWriter := setupBenchmarkGateway(0.30)
	defer asyncWriter.Stop()

	requests := 300
	var wg sync.WaitGroup
	wg.Add(requests)

	latencies := make([]time.Duration, requests)
	successCount := 0
	var mu sync.Mutex

	startAll := time.Now()

	for i := 0; i < requests; i++ {
		idx := i
		go func() {
			defer wg.Done()

			reqBody, _ := json.Marshal(models.CompletionRequest{
				Model: "gpt-4o",
				Messages: []models.Message{
					{Role: models.RoleUser, Content: fmt.Sprintf("Chaos query %d", idx)},
				},
				Stream: true,
			})

			start := time.Now()
			rec := &noopFlusherRecorder{ResponseRecorder: httptest.NewRecorder()}
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(reqBody))

			h.ChatCompletions(rec, req)
			latencies[idx] = time.Since(start)

			if rec.Code == http.StatusOK {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	totalDuration := time.Since(startAll)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[int(float64(requests)*0.50)]
	p99 := latencies[int(float64(requests)*0.99)]

	t.Logf("[Senaryo C - Chaos 30%% Error Fallback] Success: %d/%d (%.1f%%), Total: %v, p50: %v, p99: %v",
		successCount, requests, float64(successCount)/float64(requests)*100, totalDuration, p50, p99)

	if successCount != requests {
		t.Fatalf("expected 100%% recovery via fallback router, but got %d/%d", successCount, requests)
	}
}

// Bellek ve Goroutine Profiling: Goroutine sızıntısı olmadığını doğrula
func TestGoroutineLeak_AfterLoad(t *testing.T) {
	h, asyncWriter := setupBenchmarkGateway(0.20)
	defer asyncWriter.Stop()

	// Wait for any runtime background routines to settle
	time.Sleep(30 * time.Millisecond)
	runtime.GC()
	baselineGoroutines := runtime.NumGoroutine()

	// Fire 300 concurrent requests (with cancellations, hits, misses, fallbacks)
	requests := 300
	var wg sync.WaitGroup
	wg.Add(requests)

	for i := 0; i < requests; i++ {
		idx := i
		go func() {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			if idx%10 == 0 {
				// Simüle edilen istemci erken kopması (client abort)
				time.AfterFunc(10*time.Millisecond, cancel)
			} else {
				defer cancel()
			}

			reqBody, _ := json.Marshal(models.CompletionRequest{
				Model: "gpt-4o",
				Messages: []models.Message{
					{Role: models.RoleUser, Content: fmt.Sprintf("Leak test query %d", idx)},
				},
				Stream: true,
			})

			rec := &noopFlusherRecorder{ResponseRecorder: httptest.NewRecorder()}
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(reqBody)).WithContext(ctx)

			h.ChatCompletions(rec, req)
		}()
	}

	wg.Wait()

	// Allow goroutines to wind down and invoke GC
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	finalGoroutines := runtime.NumGoroutine()

	t.Logf("[Goroutine Leak Test] Baseline: %d, Final: %d, Diff: %+d",
		baselineGoroutines, finalGoroutines, finalGoroutines-baselineGoroutines)

	// Allow maximum small variance (+3) for runtime internal threads/GC
	if finalGoroutines > baselineGoroutines+3 {
		t.Fatalf("potential goroutine leak detected: baseline %d, final %d (leaked %d goroutines)",
			baselineGoroutines, finalGoroutines, finalGoroutines-baselineGoroutines)
	}
}
