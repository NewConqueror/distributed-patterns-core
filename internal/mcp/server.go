package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/safetool"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/sse"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/tool"
)

// ServerConfig configures the Model Context Protocol (MCP) Server.
type ServerConfig struct {
	Name            string
	Version         string
	Executor        *safetool.Executor
	ResourceManager *ResourceManager
	PromptManager   *PromptManager
}

// Server implements Model Context Protocol (MCP) server over JSON-RPC 2.0.
type Server struct {
	name            string
	version         string
	executor        *safetool.Executor
	resourceManager *ResourceManager
	promptManager   *PromptManager
	mu              sync.RWMutex
}

// NewServer initializes an MCP Server instance.
func NewServer(cfg ServerConfig) *Server {
	name := cfg.Name
	if name == "" {
		name = "ai-gateway-mcp"
	}
	version := cfg.Version
	if version == "" {
		version = "1.0.0"
	}

	exec := cfg.Executor
	if exec == nil {
		exec = safetool.NewExecutor(safetool.ExecutorConfig{})
	}

	resMgr := cfg.ResourceManager
	if resMgr == nil {
		resMgr = NewResourceManager()
	}

	promptMgr := cfg.PromptManager
	if promptMgr == nil {
		promptMgr = NewPromptManager()
	}

	return &Server{
		name:            name,
		version:         version,
		executor:        exec,
		resourceManager: resMgr,
		promptManager:   promptMgr,
	}
}

// HandleRPC processes a single JSON-RPC 2.0 message and returns the JSON-RPC response.
func (s *Server) HandleRPC(ctx context.Context, reqBytes []byte) ([]byte, error) {
	var req JSONRPCRequest
	if err := json.Unmarshal(reqBytes, &req); err != nil {
		resp := JSONRPCResponse{
			JSONRPC: "2.0",
			Error: &JSONRPCError{
				Code:    CodeParseError,
				Message: "Parse error: invalid JSON",
			},
		}
		return json.Marshal(resp)
	}

	resp := s.dispatch(ctx, req)
	return json.Marshal(resp)
}

func (s *Server) dispatch(ctx context.Context, req JSONRPCRequest) JSONRPCResponse {
	resp := JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
	}

	switch req.Method {
	case "initialize":
		var params InitializeParams
		if len(req.Params) > 0 {
			_ = json.Unmarshal(req.Params, &params)
		}

		result := InitializeResult{
			ProtocolVersion: "2024-11-05",
			Capabilities: map[string]any{
				"tools":     map[string]any{},
				"resources": map[string]any{},
				"prompts":   map[string]any{},
			},
		}
		result.ServerInfo.Name = s.name
		result.ServerInfo.Version = s.version
		resp.Result = result

	case "notifications/initialized":
		// Acknowledged, no result required for notification
		resp.Result = map[string]string{"status": "initialized"}

	case "tools/list":
		defs := s.executor.Registry().List()
		type mcpTool struct {
			Name        string          `json:"name"`
			Description string          `json:"description"`
			InputSchema tool.JSONSchema `json:"inputSchema"`
		}

		toolList := make([]mcpTool, 0, len(defs))
		for _, d := range defs {
			toolList = append(toolList, mcpTool{
				Name:        d.Name,
				Description: d.Description,
				InputSchema: d.Parameters,
			})
		}
		resp.Result = map[string]any{
			"tools": toolList,
		}

	case "tools/call":
		var params ToolCallParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			resp.Error = &JSONRPCError{
				Code:    CodeInvalidParams,
				Message: "Invalid params for tools/call: " + err.Error(),
			}
			return resp
		}

		output, err := s.executor.Execute(ctx, params.Name, params.Arguments)
		if err != nil {
			// Feed schema validation errors or execution failures cleanly back to LLM
			resp.Result = ToolCallResult{
				Content: []ToolContent{
					{Type: "text", Text: err.Error()},
				},
				IsError: true,
			}
			return resp
		}

		var outStr string
		switch v := output.(type) {
		case string:
			outStr = v
		case []byte:
			outStr = string(v)
		default:
			b, _ := json.Marshal(v)
			outStr = string(b)
		}

		resp.Result = ToolCallResult{
			Content: []ToolContent{
				{Type: "text", Text: outStr},
			},
			IsError: false,
		}

	case "resources/list":
		resp.Result = map[string]any{
			"resources": s.resourceManager.List(),
		}

	case "resources/read":
		var params ResourceReadParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			resp.Error = &JSONRPCError{
				Code:    CodeInvalidParams,
				Message: "Invalid params for resources/read: " + err.Error(),
			}
			return resp
		}

		content, err := s.resourceManager.Read(ctx, params.URI)
		if err != nil {
			resp.Error = &JSONRPCError{
				Code:    CodeInternalError,
				Message: err.Error(),
			}
			return resp
		}

		resp.Result = ResourceReadResult{
			Contents: []ResourceContent{*content},
		}

	case "prompts/list":
		resp.Result = map[string]any{
			"prompts": s.promptManager.List(),
		}

	case "prompts/get":
		var params PromptGetParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			resp.Error = &JSONRPCError{
				Code:    CodeInvalidParams,
				Message: "Invalid params for prompts/get: " + err.Error(),
			}
			return resp
		}

		res, err := s.promptManager.Get(ctx, params.Name, params.Arguments)
		if err != nil {
			resp.Error = &JSONRPCError{
				Code:    CodeInternalError,
				Message: err.Error(),
			}
			return resp
		}

		resp.Result = res

	default:
		resp.Error = &JSONRPCError{
			Code:    CodeMethodNotFound,
			Message: fmt.Sprintf("Method not found: %s", req.Method),
		}
	}

	return resp
}

// ServeHTTP handles incoming HTTP POST JSON-RPC 2.0 requests.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	resBytes, err := s.HandleRPC(r.Context(), body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(resBytes)
}

// ServeSSE handles SSE connections for MCP clients that listen to streamed events.
func (s *Server) ServeSSE(w http.ResponseWriter, r *http.Request) {
	writer, err := sse.NewWriter(w, r)
	if err != nil {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	// Send endpoint event to inform client where to post RPC requests
	_ = writer.WriteEvent(sse.Event{
		Event: "endpoint",
		Data:  "/mcp/rpc",
	})

	<-r.Context().Done()
}

// ServeStdio continuously handles JSON-RPC 2.0 lines over standard input/output (for Claude Desktop / CLI).
func (s *Server) ServeStdio(ctx context.Context, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		respBytes, err := s.HandleRPC(ctx, line)
		if err != nil {
			return err
		}

		if _, err := out.Write(append(respBytes, '\n')); err != nil {
			return err
		}
	}
	return scanner.Err()
}
