package handler

import (
	"encoding/json"
	"net/http"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/safetool"
)

// ToolHandler provides HTTP endpoints for discovering and executing safe tools.
type ToolHandler struct {
	executor *safetool.Executor
}

// NewToolHandler creates a new ToolHandler.
func NewToolHandler(executor *safetool.Executor) *ToolHandler {
	return &ToolHandler{
		executor: executor,
	}
}

// ListTools returns registered tools in OpenAI compatible format.
func (h *ToolHandler) ListTools(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	tools := h.executor.Registry().ToOpenAITools()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"tools": tools,
	})
}

type executeToolRequest struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

// ExecuteTool safely executes a tool with schema validation and policy checks.
func (h *ToolHandler) ExecuteTool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req executeToolRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Name == "" {
		http.Error(w, "Tool name is required", http.StatusBadRequest)
		return
	}

	output, err := h.executor.Execute(r.Context(), req.Name, req.Arguments)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"output": output,
	})
}
