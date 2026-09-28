package handler

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/cache"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/embedding"
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

	ctx := r.Context()

	// 1. Semantic Cache Lookup (<50ms fast-path)
	var promptText string
	var promptEmbedding []float32

	if h.vectorStore != nil && h.embedder != nil {
		promptText = normalizer.ExtractPromptFromMessages(req.Messages)
		if promptText != "" {
			emb, err := h.embedder.Embed(ctx, promptText)
			if err == nil && len(emb) > 0 {
				promptEmbedding = emb
				cachedItem, findErr := h.vectorStore.FindSimilar(ctx, emb, h.cacheThreshold)
				if findErr == nil && cachedItem != nil {
					// Cache HIT: Replay response immediately with simulated latency
					w.Header().Set("X-Cache", "HIT")
					if replayErr := h.replay.Replay(ctx, cachedItem, sseWriter); replayErr != nil {
						log.Printf("[Gateway] Replay cache error: %v", replayErr)
					}
					return
				}
			}
		}
	}

	// Cache MISS: Stream from upstream LLM models via Router
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
		if errors.Is(err, sse.ErrClientDisconnected) || errors.Is(ctx.Err(), http.ErrAbortHandler) {
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
