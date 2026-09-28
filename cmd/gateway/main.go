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
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/orchestrator"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/provider"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/tokenguard"
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
	log.Println(" AI Gateway: Faz 1 (Streaming & Fallbacks) + Faz 2 (Semantic Cache & Token Guard)")
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

	// 7. Register HTTP Handlers
	gatewayHandler := handler.NewGatewayHandlerWithConfig(handler.GatewayHandlerConfig{
		Router:         router,
		VectorStore:    vectorStore,
		Embedder:       embedder,
		Replay:         replayEngine,
		AsyncWriter:    asyncWriter,
		CacheThreshold: cacheThreshold,
	})

	mux := http.NewServeMux()
	mux.Handle("/v1/chat/completions", tokenGuardMiddleware.Handler(http.HandlerFunc(gatewayHandler.ChatCompletions)))
	mux.HandleFunc("/health", gatewayHandler.HealthStatus)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 0, // Streaming responses require context-driven write timeouts
		IdleTimeout:  60 * time.Second,
	}

	// 8. Start HTTP Server
	go func() {
		log.Printf("[Gateway] Server running on http://localhost:%s", port)
		log.Printf("[Gateway] Rate Limits: %d RPM, %d TPM (Sliding Window)", maxRPM, maxTPM)
		log.Printf("[Gateway] Semantic Cache: Max Cosine Distance %.2f (>= %.0f%% similarity)",
			cacheThreshold, (1.0-cacheThreshold)*100)
		log.Printf("[Gateway] Primary Provider: %s | Fallback: %s", primaryProvider.ID(), fallbackProvider.ID())

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[Gateway] Server error: %v", err)
		}
	}()

	// 9. Graceful Shutdown
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
