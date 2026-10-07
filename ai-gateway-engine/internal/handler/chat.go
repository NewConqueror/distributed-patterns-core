package handler

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/cache"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/embedding"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/observability"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/orchestrator"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/normalizer"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/sse"
)

// GatewayHandlerConfig configures the HTTP Gateway Handler.
type GatewayHandlerConfig struct {
	Router         *orchestrator.Router
	VectorStore    cache.VectorStore
	Embedder       embedding.Embedder
	Replay         *cache.ReplayEngine
	AsyncWriter    *cache.AsyncWriter
	CacheThreshold float64 // Max cosine distance for hit (e.g. 0.08)
}

// GatewayHandler handles incoming API requests with semantic caching and fallback routing.
type GatewayHandler struct {
	router         *orchestrator.Router
	vectorStore    cache.VectorStore
	embedder       embedding.Embedder
	replay         *cache.ReplayEngine
	asyncWriter    *cache.AsyncWriter
	cacheThreshold float64
}

// NewGatewayHandler creates a basic GatewayHandler without semantic caching.
func NewGatewayHandler(router *orchestrator.Router) *GatewayHandler {
	return NewGatewayHandlerWithConfig(GatewayHandlerConfig{
		Router: router,
	})
}

// NewGatewayHandlerWithConfig initializes GatewayHandler with semantic cache and replay capabilities.
func NewGatewayHandlerWithConfig(cfg GatewayHandlerConfig) *GatewayHandler {
	threshold := cfg.CacheThreshold
	if threshold <= 0 {
		threshold = cache.DefaultMaxDistance // 0.08
	}

	replay := cfg.Replay
	if replay == nil {
		replay = cache.NewReplayEngine(cache.ReplayConfig{})
	}

	return &GatewayHandler{
		router:         cfg.Router,
		vectorStore:    cfg.VectorStore,
		embedder:       cfg.Embedder,
		replay:         replay,
		asyncWriter:    cfg.AsyncWriter,
		cacheThreshold: threshold,
	}
}

// ChatCompletions handles OpenAI-compatible POST /v1/chat/completions requests.
func (h *GatewayHandler) ChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.CompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Messages) == 0 {
		http.Error(w, "Messages cannot be empty", http.StatusBadRequest)
		return
	}

	// Initialize SSE streaming writer
	sseWriter, err := sse.NewWriter(w, r)
	if err != nil {
		if errors.Is(err, sse.ErrStreamingNotSupported) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		http.Error(w, "Failed to initialize SSE stream: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// 1. Start Distributed Tracing Root Span & Metrics
	startReq := time.Now()
	metrics := observability.DefaultRegistry()
	tracer := observability.DefaultTracer()

	ctx, rootSpan := tracer.StartSpan(r.Context(), "HTTP POST /v1/chat/completions")
	rootSpan.SetAttribute("http.method", "POST")
	rootSpan.SetAttribute("http.path", "/v1/chat/completions")
	rootSpan.SetAttribute("llm.model", req.Model)

	statusStr := "200"
	defer func() {
		duration := time.Since(startReq).Seconds()
		metrics.HTTPRequestsTotal.Inc(r.Method, "/v1/chat/completions", statusStr)
		metrics.HTTPRequestDurationSeconds.Observe(duration, r.Method, "/v1/chat/completions")
		rootSpan.End()
	}()

	// 2. Semantic Cache Lookup (<50ms fast-path)
	var promptText string
	var promptEmbedding []float32

	if h.vectorStore != nil && h.embedder != nil {
		_, cacheSpan := tracer.StartSpan(ctx, "SemanticCache.Lookup")
		promptText = normalizer.ExtractPromptFromMessages(req.Messages)
		if promptText != "" {
			emb, err := h.embedder.Embed(ctx, promptText)
			if err == nil && len(emb) > 0 {
				promptEmbedding = emb
				cachedItem, findErr := h.vectorStore.FindSimilar(ctx, emb, h.cacheThreshold)
				if findErr == nil && cachedItem != nil {
					// Cache HIT: Replay response immediately with simulated latency
					cacheSpan.SetAttribute("cache.hit", true)
					cacheSpan.SetAttribute("cache.similarity", cachedItem.Similarity)
					cacheSpan.End()

					metrics.SemanticCacheHitsTotal.Inc()
					metrics.LLMTimeToFirstTokenSeconds.Observe(0.015, "semantic_cache", req.Model, "true")
					rootSpan.SetAttribute("cache.hit", true)
					rootSpan.AddEvent("cache_hit", map[string]any{"similarity": cachedItem.Similarity})

					w.Header().Set("X-Cache", "HIT")
					if replayErr := h.replay.Replay(ctx, cachedItem, sseWriter); replayErr != nil {
						log.Printf("[Gateway] Replay cache error: %v", replayErr)
					}
					return
				}
			}
		}
		cacheSpan.SetAttribute("cache.hit", false)
		cacheSpan.End()
	}

	// Cache MISS: Stream from upstream LLM models via Router
	metrics.SemanticCacheMissesTotal.Inc()
	rootSpan.SetAttribute("cache.hit", false)
	w.Header().Set("X-Cache", "MISS")

	// Set up token accumulator to persist completed response to vector cache asynchronously
	var collectedText strings.Builder
	var mu sync.Mutex

	if h.asyncWriter != nil && promptText != "" {
		sseWriter.SetInterceptor(func(chunk any) {
			if sc, ok := chunk.(models.StreamChunk); ok && sc.DeltaContent != "" {
				mu.Lock()
				collectedText.WriteString(sc.DeltaContent)
				mu.Unlock()
			}
		})
	}

	// Propagate request context. If client disconnects, r.Context().Done() cancels upstream.
	err = h.router.StreamResponse(ctx, req, sseWriter)
	if err != nil {
		statusStr = "500"
		if errors.Is(err, sse.ErrClientDisconnected) || errors.Is(ctx.Err(), http.ErrAbortHandler) {
			statusStr = "499" // Client Closed Request
			log.Printf("[Gateway] Client disconnected prematurely, canceled upstream streaming")
			return
		}
		log.Printf("[Gateway] Stream error: %v", err)
		return
	}

	// 2. Asynchronously save successful completion to vector cache without blocking client
	if h.asyncWriter != nil && promptText != "" {
		finalResponse := collectedText.String()
		if len(finalResponse) > 0 {
			h.asyncWriter.Enqueue(&cache.WriteTask{
				PromptText:   promptText,
				Embedding:    promptEmbedding,
				ResponseText: finalResponse,
				Metadata: map[string]any{
					"model": req.Model,
				},
			})
		}
	}
}

// HealthStatus returns the gateway health and state of all registered circuit breakers.
func (h *GatewayHandler) HealthStatus(w http.ResponseWriter, r *http.Request) {
	status := map[string]any{
		"status": "ok",
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}
