package mcp

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

func setupTestMCPServer() *Server {
	reg := tool.NewRegistry()
	_ = reg.Register(tool.Tool{
		Definition: tool.Definition{
			Name:        "fetch_order_status",
			Description: "Get order status",
			ReadOnly:    true,
			Parameters: tool.JSONSchema{
				Type:     tool.TypeObject,
				Required: []string{"order_id"},
				Properties: map[string]tool.JSONSchema{
					"order_id": {Type: tool.TypeString},
				},
			},
		},
		Handler: func(ctx context.Context, args map[string]any) (any, error) {
			return map[string]string{"status": "shipped"}, nil
		},
	})

	resMgr := NewResourceManager()
	_ = resMgr.Register(ResourceInfo{
		URI:      "db://system_info",
		Name:     "System Info",
		MimeType: "text/plain",
	}, func(ctx context.Context, uri string) (string, error) {
		return "System healthy and operational", nil
	})

	promptMgr := NewPromptManager()
	_ = promptMgr.Register(PromptInfo{
		Name:        "greet",
		Description: "Greeting prompt",
	}, func(ctx context.Context, args map[string]string) ([]PromptMessage, error) {
		user := args["user"]
		if user == "" {
			user = "Guest"
		}
		return []PromptMessage{
			{Role: "user", Content: ToolContent{Type: "text", Text: "Hello " + user}},
		}, nil
	})

	executor := safetool.NewExecutor(safetool.ExecutorConfig{
		Registry: reg,
	})

	return NewServer(ServerConfig{
		Name:            "test-mcp-server",
		Version:         "1.0.0",
		Executor:        executor,
		ResourceManager: resMgr,
		PromptManager:   promptMgr,
	})
}

func TestMCPServer_Initialize(t *testing.T) {
	srv := setupTestMCPServer()

	req := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}`
	respBytes, err := srv.HandleRPC(context.Background(), []byte(req))
	if err != nil {
		t.Fatalf("unexpected RPC error: %v", err)
	}

	var resp JSONRPCResponse
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		t.Fatalf("failed to unmarshal RPC response: %v", err)
	}

	if resp.Error != nil {
		t.Fatalf("unexpected RPC error object: %+v", resp.Error)
	}

	resMap := resp.Result.(map[string]any)
	if resMap["protocolVersion"] != "2024-11-05" {
		t.Fatalf("expected protocolVersion 2024-11-05, got %v", resMap["protocolVersion"])
	}
}

func TestMCPServer_ToolsList_And_Call(t *testing.T) {
	srv := setupTestMCPServer()

	// 1. tools/list
	listReq := `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`
	listRespBytes, _ := srv.HandleRPC(context.Background(), []byte(listReq))

	var listResp JSONRPCResponse
	_ = json.Unmarshal(listRespBytes, &listResp)
	toolsData := listResp.Result.(map[string]any)["tools"].([]any)
	if len(toolsData) != 1 {
		t.Fatalf("expected 1 tool, got %d", len(toolsData))
	}

	// 2. tools/call - Success
	callReq := `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"fetch_order_status","arguments":{"order_id":"ORD-999"}}}`
	callRespBytes, _ := srv.HandleRPC(context.Background(), []byte(callReq))

	var callResp JSONRPCResponse
	_ = json.Unmarshal(callRespBytes, &callResp)
	callResult := callResp.Result.(map[string]any)
	if callResult["isError"].(bool) {
		t.Fatalf("expected isError false, got true: %+v", callResult)
	}
	contentList := callResult["content"].([]any)
	text := contentList[0].(map[string]any)["text"].(string)
	if !strings.Contains(text, "shipped") {
		t.Fatalf("expected 'shipped' in tool output, got %s", text)
	}

	// 3. tools/call - Schema Error (missing required order_id)
	invalidReq := `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"fetch_order_status","arguments":{}}}`
	invalidRespBytes, _ := srv.HandleRPC(context.Background(), []byte(invalidReq))

	var invalidResp JSONRPCResponse
	_ = json.Unmarshal(invalidRespBytes, &invalidResp)
	invResult := invalidResp.Result.(map[string]any)
	if !invResult["isError"].(bool) {
		t.Fatal("expected isError true on missing parameter")
	}
	invText := invResult["content"].([]any)[0].(map[string]any)["text"].(string)
	if !strings.Contains(invText, "Şema hatası") || !strings.Contains(invText, "order_id") {
		t.Fatalf("expected schema error mentioning order_id, got: %s", invText)
	}
}

func TestMCPServer_Resources_And_Prompts(t *testing.T) {
	srv := setupTestMCPServer()

	// 1. resources/read
	resReq := `{"jsonrpc":"2.0","id":5,"method":"resources/read","params":{"uri":"db://system_info"}}`
	resBytes, _ := srv.HandleRPC(context.Background(), []byte(resReq))

	var resResp JSONRPCResponse
	_ = json.Unmarshal(resBytes, &resResp)
	contents := resResp.Result.(map[string]any)["contents"].([]any)
	text := contents[0].(map[string]any)["text"].(string)
	if !strings.Contains(text, "System healthy") {
		t.Fatalf("expected system healthy text, got: %s", text)
	}

	// 2. prompts/get
	promptReq := `{"jsonrpc":"2.0","id":6,"method":"prompts/get","params":{"name":"greet","arguments":{"user":"Antigravity"}}}`
	promptBytes, _ := srv.HandleRPC(context.Background(), []byte(promptReq))

	var promptResp JSONRPCResponse
	_ = json.Unmarshal(promptBytes, &promptResp)
	messages := promptResp.Result.(map[string]any)["messages"].([]any)
	firstMsg := messages[0].(map[string]any)
	content := firstMsg["content"].(map[string]any)["text"].(string)
	if content != "Hello Antigravity" {
		t.Fatalf("expected 'Hello Antigravity', got %s", content)
	}
}

func TestMCPServer_ServeHTTP_And_Stdio(t *testing.T) {
	srv := setupTestMCPServer()

	// 1. HTTP JSON-RPC POST
	httpReq := httptest.NewRequest(http.MethodPost, "/mcp/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":10,"method":"tools/list"}`))
	httpRec := httptest.NewRecorder()
	srv.ServeHTTP(httpRec, httpReq)

	if httpRec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", httpRec.Code)
	}
	if !strings.Contains(httpRec.Body.String(), "fetch_order_status") {
		t.Fatalf("expected tool in HTTP response, got: %s", httpRec.Body.String())
	}

	// 2. Stdio Transport
	inBuf := bytes.NewBufferString("{\"jsonrpc\":\"2.0\",\"id\":11,\"method\":\"initialize\"}\n")
	var outBuf bytes.Buffer
	err := srv.ServeStdio(context.Background(), inBuf, &outBuf)
	if err != nil {
		t.Fatalf("unexpected Stdio error: %v", err)
	}

	if !strings.Contains(outBuf.String(), "2024-11-05") {
		t.Fatalf("expected protocolVersion in Stdio output, got: %s", outBuf.String())
	}
}
