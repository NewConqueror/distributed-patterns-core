package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/cache"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/cache/memory"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/cache/pgvector"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/embedding"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/handler"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/mcp"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/orchestrator"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/provider"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/safetool"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/tokenguard"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/tool"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvFloat(key string, defaultVal float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultVal
}

func main() {
	port := getEnv("PORT", "8080")
	openAIKey := getEnv("OPENAI_API_KEY", "")
	openAIBaseURL := getEnv("OPENAI_BASE_URL", "https://api.openai.com/v1")
	ollamaBaseURL := getEnv("OLLAMA_BASE_URL", "http://localhost:11434")
	databaseURL := getEnv("DATABASE_URL", "")

	maxRPM := getEnvInt("RATE_LIMIT_RPM", 60)
	maxTPM := getEnvInt("RATE_LIMIT_TPM", 40000)
	cacheThreshold := getEnvFloat("CACHE_DISTANCE_THRESHOLD", cache.DefaultMaxDistance) // 0.08 (~92% similarity)

	log.Println("==================================================================")
	log.Println(" AI Gateway: Faz 1 (Streaming) + Faz 2 (Cache) + Faz 3 (Safe Tools & MCP)")
	log.Println("==================================================================")

	// 1. Initialize Upstream LLM Providers
	primaryProvider := provider.NewOpenAIProvider(provider.OpenAIConfig{
		ID:      "openai-primary",
		APIKey:  openAIKey,
		BaseURL: openAIBaseURL,
	})

	fallbackProvider := provider.NewOllamaProvider(provider.OllamaConfig{
		ID:      "ollama-fallback",
		BaseURL: ollamaBaseURL,
	})

	// 2. Initialize Fallback Router & Circuit Breakers
	circuitBreakerCfg := orchestrator.CircuitBreakerConfig{
		WindowSize:       10,
		FailureThreshold: 3,
		SuccessThreshold: 2,
		OpenCooldown:     10 * time.Second,
	}

	router, err := orchestrator.NewRouter(orchestrator.RouterConfig{
		PrimaryProvider:   primaryProvider,
		FallbackProviders: []provider.LLMProvider{fallbackProvider},
		CircuitBreakerCfg: circuitBreakerCfg,
		RecoveryMode:      orchestrator.ModeCleanEventAndRestart,
	})
	if err != nil {
		log.Fatalf("Failed to initialize router: %v", err)
	}

	// 3. Initialize Embedding Client
	var embedder embedding.Embedder
	if openAIKey != "" {
		embedder = embedding.NewOpenAIEmbedder(embedding.OpenAIEmbedderConfig{
			APIKey:  openAIKey,
			BaseURL: openAIBaseURL,
			Model:   "text-embedding-3-small",
		})
		log.Println("[Gateway] Embedder: OpenAI text-embedding-3-small")
	} else {
		embedder = embedding.NewMockEmbedder(1536)
		log.Println("[Gateway] Embedder: Deterministic MockEmbedder (Set OPENAI_API_KEY for live embeddings)")
	}

	// 4. Initialize Vector Store (Pgvector or Memory Store)
	var vectorStore cache.VectorStore
	if databaseURL != "" {
		db, dbErr := sql.Open("postgres", databaseURL)
		if dbErr != nil {
			log.Fatalf("Failed to open postgres database: %v", dbErr)
		}
		vectorStore = pgvector.NewStore(db)
		log.Println("[Gateway] Vector Store: PostgreSQL pgvector (HNSW Index enabled)")
	} else {
		vectorStore = memory.NewStore()
		log.Println("[Gateway] Vector Store: In-Memory Cosine Similarity Store")
	}

	// 5. Initialize Cache Replay Engine and Async Cache Writer
	replayEngine := cache.NewReplayEngine(cache.ReplayConfig{
		ChunkDelay:    15 * time.Millisecond,
		MaxChunkWords: 2,
	})
	asyncWriter := cache.NewAsyncWriter(vectorStore, embedder, 500, 2)

	// 6. Initialize Token Guard & Rate Limiter
	rateLimiter := tokenguard.NewMemorySlidingWindowLimiter(tokenguard.LimiterConfig{
		MaxRPM:         maxRPM,
		MaxTPM:         maxTPM,
		WindowDuration: time.Minute,
	})
	tokenGuardMiddleware := tokenguard.NewMiddleware(rateLimiter)

	// 7. Initialize Safe Tool Execution Engine & Policy Engine
	toolRegistry := tool.NewRegistry()

	// Register sample safe tools
	_ = toolRegistry.Register(tool.Tool{
		Definition: tool.Definition{
			Name:        "fetch_order_status",
			Description: "Safely fetches current order status by Order ID",
			ReadOnly:    true,
			Parameters: tool.JSONSchema{
				Type:     tool.TypeObject,
				Required: []string{"order_id"},
				Properties: map[string]tool.JSONSchema{
					"order_id": {Type: tool.TypeString, Description: "Unique order identifier (e.g. ORD-123)"},
				},
			},
			RequiredPermissions: []string{"orders:read"},
		},
		Handler: func(ctx context.Context, args map[string]any) (any, error) {
			orderID := args["order_id"].(string)
			return map[string]any{
				"order_id":   orderID,
				"status":     "in_transit",
				"updated_at": time.Now().UTC().Format(time.RFC3339),
			}, nil
		},
	})

	_ = toolRegistry.Register(tool.Tool{
		Definition: tool.Definition{
			Name:        "system_status",
			Description: "Fetches AI Gateway system status and latency metrics",
			ReadOnly:    true,
			Parameters: tool.JSONSchema{
				Type: tool.TypeObject,
			},
		},
		Handler: func(ctx context.Context, args map[string]any) (any, error) {
			return map[string]any{
				"gateway_status": "healthy",
				"uptime_seconds": 3600,
			}, nil
		},
	})

	safeExecutor := safetool.NewExecutor(safetool.ExecutorConfig{
		Registry:    toolRegistry,
		Policy:      safetool.NewPolicyEngine(),
		AuditLogger: safetool.NewJSONAuditLogger(os.Stdout),
		Timeout:     3 * time.Second,
	})

	// 8. Initialize Model Context Protocol (MCP) Server
	resourceManager := mcp.NewResourceManager()
	_ = resourceManager.Register(mcp.ResourceInfo{
		URI:      "system://gateway_config",
		Name:     "Gateway Configuration",
		MimeType: "application/json",
	}, func(ctx context.Context, uri string) (string, error) {
		return fmt.Sprintf(`{"rate_limit_rpm":%d,"cache_distance_threshold":%.2f}`, maxRPM, cacheThreshold), nil
	})

	promptManager := mcp.NewPromptManager()
	_ = promptManager.Register(mcp.PromptInfo{
		Name:        "order_inquiry",
		Description: "Template for customer order status inquiry",
		Arguments: []mcp.PromptArgument{
			{Name: "customer_name", Required: true},
			{Name: "order_id", Required: true},
		},
	}, func(ctx context.Context, args map[string]string) ([]mcp.PromptMessage, error) {
		return []mcp.PromptMessage{
			{
				Role: "user",
				Content: mcp.ToolContent{
					Type: "text",
					Text: fmt.Sprintf("Hello, my name is %s. Could you check the status of my order %s?",
						args["customer_name"], args["order_id"]),
				},
			},
		}, nil
	})

	mcpServer := mcp.NewServer(mcp.ServerConfig{
		Name:            "Production-AI-Gateway-MCP",
		Version:         "1.0.0",
		Executor:        safeExecutor,
		ResourceManager: resourceManager,
		PromptManager:   promptManager,
	})

	// 9. Register HTTP Routes
	gatewayHandler := handler.NewGatewayHandlerWithConfig(handler.GatewayHandlerConfig{
		Router:         router,
		VectorStore:    vectorStore,
		Embedder:       embedder,
		Replay:         replayEngine,
		AsyncWriter:    asyncWriter,
		CacheThreshold: cacheThreshold,
	})
	toolHandler := handler.NewToolHandler(safeExecutor)

	mux := http.NewServeMux()
	mux.Handle("/v1/chat/completions", tokenGuardMiddleware.Handler(http.HandlerFunc(gatewayHandler.ChatCompletions)))
	mux.HandleFunc("/v1/tools", toolHandler.ListTools)
	mux.HandleFunc("/v1/tools/execute", toolHandler.ExecuteTool)
	mux.HandleFunc("/mcp/rpc", mcpServer.ServeHTTP)
	mux.HandleFunc("/mcp/sse", mcpServer.ServeSSE)
	mux.HandleFunc("/health", gatewayHandler.HealthStatus)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 0, // Streaming responses require context-driven write timeouts
		IdleTimeout:  60 * time.Second,
	}

	// 10. Start HTTP Server
	go func() {
		log.Printf("[Gateway] Server running on http://localhost:%s", port)
		log.Printf("[Gateway] Tools & MCP: Registered %d tools, JSON-RPC at /mcp/rpc, SSE at /mcp/sse", len(toolRegistry.List()))
		log.Printf("[Gateway] Endpoints: /v1/chat/completions, /v1/tools, /v1/tools/execute, /mcp/rpc, /mcp/sse, /health")

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[Gateway] Server error: %v", err)
		}
	}()

	// 11. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Println("\n[Gateway] Shutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	asyncWriter.Stop()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[Gateway] Server forced shutdown: %v", err)
	} else {
		log.Println("[Gateway] Server stopped cleanly")
	}
}
