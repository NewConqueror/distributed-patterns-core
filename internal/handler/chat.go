package handler

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/orchestrator"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/sse"
)

// GatewayHandler handles incoming API requests.
type GatewayHandler struct {
	router *orchestrator.Router
}

// NewGatewayHandler creates a new GatewayHandler.
func NewGatewayHandler(router *orchestrator.Router) *GatewayHandler {
	return &GatewayHandler{
		router: router,
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

	// Propagate request context (r.Context()). If client disconnects or aborts,
	// r.Context().Done() fires immediately, stopping upstream streaming.
	ctx := r.Context()
	err = h.router.StreamResponse(ctx, req, sseWriter)
	if err != nil {
		if errors.Is(err, sse.ErrClientDisconnected) || errors.Is(ctx.Err(), http.ErrAbortHandler) {
			log.Printf("[Gateway] Client disconnected prematurely, canceled upstream streaming")
			return
		}
		log.Printf("[Gateway] Stream error: %v", err)
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
