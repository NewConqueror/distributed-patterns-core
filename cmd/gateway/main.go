package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/handler"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/orchestrator"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/provider"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	port := getEnv("PORT", "8080")
	openAIKey := getEnv("OPENAI_API_KEY", "")
	openAIBaseURL := getEnv("OPENAI_BASE_URL", "https://api.openai.com/v1")
	ollamaBaseURL := getEnv("OLLAMA_BASE_URL", "http://localhost:11434")

	log.Println("==================================================")
	log.Println(" Starting AI Gateway: Faz 1 - Core Streaming & Fallback Router")
	log.Println("==================================================")

	// 1. Initialize Upstream Providers
	primaryProvider := provider.NewOpenAIProvider(provider.OpenAIConfig{
		ID:      "openai-primary",
		APIKey:  openAIKey,
		BaseURL: openAIBaseURL,
	})

	fallbackProvider := provider.NewOllamaProvider(provider.OllamaConfig{
		ID:      "ollama-fallback",
		BaseURL: ollamaBaseURL,
	})

	// 2. Initialize Fallback Router & Circuit Breaker
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

	// 3. Register HTTP Routes
	gatewayHandler := handler.NewGatewayHandler(router)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", gatewayHandler.ChatCompletions)
	mux.HandleFunc("/health", gatewayHandler.HealthStatus)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 0, // Streaming responses require 0 or context-driven write timeouts
		IdleTimeout:  60 * time.Second,
	}

	// 4. Start HTTP Server in background
	go func() {
		log.Printf("[Gateway] Server listening on http://localhost:%s", port)
		log.Printf("[Gateway] Primary Provider: %s (%s)", primaryProvider.ID(), openAIBaseURL)
		log.Printf("[Gateway] Fallback Provider: %s (%s)", fallbackProvider.ID(), ollamaBaseURL)
		log.Printf("[Gateway] Endpoints: POST /v1/chat/completions, GET /health")

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[Gateway] Server error: %v", err)
		}
	}()

	// 5. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Println("\n[Gateway] Shutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[Gateway] Server forced shutdown: %v", err)
	} else {
		log.Println("[Gateway] Server stopped cleanly")
	}
}
