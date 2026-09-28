package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/safetool"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/tool"
)

func TestToolHandler_ListAndExecute(t *testing.T) {
	reg := tool.NewRegistry()
	_ = reg.Register(tool.Tool{
		Definition: tool.Definition{
			Name:        "get_weather",
			Description: "Get current weather for city",
			ReadOnly:    true,
			Parameters: tool.JSONSchema{
				Type:     tool.TypeObject,
				Required: []string{"city"},
				Properties: map[string]tool.JSONSchema{
					"city": {Type: tool.TypeString},
				},
			},
		},
		Handler: func(ctx context.Context, args map[string]any) (any, error) {
			return map[string]any{
				"city":        args["city"],
				"temperature": 22.5,
			}, nil
		},
	})

	exec := safetool.NewExecutor(safetool.ExecutorConfig{
		Registry: reg,
	})
	h := NewToolHandler(exec)

	// 1. GET /v1/tools
	reqList := httptest.NewRequest(http.MethodGet, "/v1/tools", nil)
	recList := httptest.NewRecorder()
	h.ListTools(recList, reqList)

	if recList.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", recList.Code)
	}
	if !strings.Contains(recList.Body.String(), "get_weather") {
		t.Fatalf("expected get_weather in tools list, got %s", recList.Body.String())
	}

	// 2. POST /v1/tools/execute - Success
	execBody, _ := json.Marshal(map[string]any{
		"name": "get_weather",
		"arguments": map[string]any{
			"city": "Istanbul",
		},
	})
	reqExec := httptest.NewRequest(http.MethodPost, "/v1/tools/execute", bytes.NewReader(execBody))
	recExec := httptest.NewRecorder()
	h.ExecuteTool(recExec, reqExec)

	if recExec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", recExec.Code)
	}
	if !strings.Contains(recExec.Body.String(), "Istanbul") {
		t.Fatalf("expected Istanbul in output, got: %s", recExec.Body.String())
	}

	// 3. POST /v1/tools/execute - Schema Error
	badBody, _ := json.Marshal(map[string]any{
		"name":      "get_weather",
		"arguments": map[string]any{},
	})
	reqBad := httptest.NewRequest(http.MethodPost, "/v1/tools/execute", bytes.NewReader(badBody))
	recBad := httptest.NewRecorder()
	h.ExecuteTool(recBad, reqBad)

	if recBad.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", recBad.Code)
	}
	if !strings.Contains(recBad.Body.String(), "Şema hatası") {
		t.Fatalf("expected schema error message, got %s", recBad.Body.String())
	}
}
