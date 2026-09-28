package orchestrator

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/observability"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/provider"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/sse"
)

var (
	// ErrAllProvidersFailed is returned when primary and all fallback providers fail.
	ErrAllProvidersFailed = errors.New("all LLM providers failed to complete the request")
)

// MidStreamRecoveryMode defines how mid-stream failures are handled.
type MidStreamRecoveryMode string

const (
	// ModeCleanEventAndRestart sends an SSE event to inform the client of the fallback switch,
	// and starts the fallback provider from clean context.
	ModeCleanEventAndRestart MidStreamRecoveryMode = "event_and_restart"
	// ModeContinue appends the partial generation to the prompt/messages and continues.
	ModeContinue MidStreamRecoveryMode = "continue"
)

// FallbackNotification is the SSE payload sent when mid-stream fallback is activated.
type FallbackNotification struct {
	Event            string `json:"event"`
	PrimaryProvider  string `json:"primary_provider"`
	FallbackProvider string `json:"fallback_provider"`
	TokensEmitted    int    `json:"tokens_emitted"`
	Message          string `json:"message"`
	Timestamp        int64  `json:"timestamp"`
}

// RouterConfig holds settings for the Orchestrator Router.
type RouterConfig struct {
	PrimaryProvider   provider.LLMProvider
	FallbackProviders []provider.LLMProvider
	CircuitBreakerCfg CircuitBreakerConfig
	RecoveryMode      MidStreamRecoveryMode
}

// Router routes completion requests with circuit breaking and seamless fallbacks.
type Router struct {
	mu                sync.RWMutex
	primaryProvider   provider.LLMProvider
	fallbackProviders []provider.LLMProvider
	breakers          map[string]*CircuitBreaker
	recoveryMode      MidStreamRecoveryMode
}

// NewRouter constructs a new Router instance with attached circuit breakers.
func NewRouter(cfg RouterConfig) (*Router, error) {
	if cfg.PrimaryProvider == nil {
		return nil, errors.New("primary provider is required")
	}

	breakers := make(map[string]*CircuitBreaker)

	// Primary breaker
	breakers[cfg.PrimaryProvider.ID()] = NewCircuitBreaker(
		cfg.PrimaryProvider.ID(),
		cfg.CircuitBreakerCfg,
	)

	// Fallback breakers
	for _, fb := range cfg.FallbackProviders {
		breakers[fb.ID()] = NewCircuitBreaker(fb.ID(), cfg.CircuitBreakerCfg)
	}

	mode := cfg.RecoveryMode
	if mode == "" {
		mode = ModeCleanEventAndRestart
	}

	return &Router{
		primaryProvider:   cfg.PrimaryProvider,
		fallbackProviders: cfg.FallbackProviders,
		breakers:          breakers,
		recoveryMode:      mode,
	}, nil
}

// GetCircuitBreaker returns the circuit breaker for a given provider ID.
func (r *Router) GetCircuitBreaker(providerID string) (*CircuitBreaker, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cb, exists := r.breakers[providerID]
	return cb, exists
}

// StreamResponse executes the completion request through circuit breakers and handles fallbacks.
func (r *Router) StreamResponse(ctx context.Context, req models.CompletionRequest, sseWriter *sse.Writer) error {
	// 1. Try Primary Provider if allowed by Circuit Breaker
	primaryCB := r.breakers[r.primaryProvider.ID()]

	if primaryCB.Allow() {
		err := r.attemptStream(ctx, r.primaryProvider, primaryCB, req, sseWriter)
		if err == nil {
			return nil
		}

		if errors.Is(err, sse.ErrClientDisconnected) || errors.Is(ctx.Err(), context.Canceled) {
			// Client disconnected, do not trigger fallback, just exit immediately
			return err
		}

		log.Printf("[Router] Primary provider %s failed: %v. Initiating fallback...", r.primaryProvider.ID(), err)
	} else {
		log.Printf("[Router] Primary provider %s circuit breaker is %s. Skipping to fallback...",
			r.primaryProvider.ID(), primaryCB.State())
	}

	// 2. Try Fallback Providers in sequence
	for _, fbProvider := range r.fallbackProviders {
		select {
		case <-ctx.Done():
			return sse.ErrClientDisconnected
		default:
		}

		fbCB := r.breakers[fbProvider.ID()]
		if !fbCB.Allow() {
			log.Printf("[Router] Fallback provider %s circuit breaker is %s. Skipping...", fbProvider.ID(), fbCB.State())
			continue
		}

		observability.DefaultRegistry().FallbackTriggeredTotal.Inc(r.primaryProvider.ID(), fbProvider.ID(), "primary_unavailable")
		err := r.attemptStream(ctx, fbProvider, fbCB, req, sseWriter)
		if err == nil {
			return nil
		}

		if errors.Is(err, sse.ErrClientDisconnected) || errors.Is(ctx.Err(), context.Canceled) {
			return err
		}

		log.Printf("[Router] Fallback provider %s failed: %v", fbProvider.ID(), err)
	}

	// All providers failed
	_ = sseWriter.WriteEvent(sse.Event{
		Event: "error",
		Data: map[string]string{
			"message": "All LLM providers are currently unavailable",
		},
	})
	return ErrAllProvidersFailed
}

func (r *Router) attemptStream(
	ctx context.Context,
	p provider.LLMProvider,
	cb *CircuitBreaker,
	req models.CompletionRequest,
	sseWriter *sse.Writer,
) error {
	streamStart := time.Now()
	chunkChan, errChan := p.Stream(ctx, req)

	tokensEmitted := 0
	var partialContent string

	for {
		select {
		case <-ctx.Done():
			// Client disconnected: do not penalize circuit breaker for client cancellation
			return sse.ErrClientDisconnected

		case err, ok := <-errChan:
			if ok && err != nil {
				// Drain any remaining buffered chunks that were already emitted before handling error
				for {
					select {
					case chunk, ok := <-chunkChan:
						if ok {
							if tokensEmitted == 0 {
								ttft := time.Since(streamStart).Seconds()
								observability.DefaultRegistry().LLMTimeToFirstTokenSeconds.Observe(ttft, p.ID(), req.Model, "false")
							}
							if writeErr := sseWriter.WriteChunk(chunk); writeErr != nil {
								return writeErr
							}
							tokensEmitted++
							partialContent += chunk.DeltaContent
							observability.DefaultRegistry().LLMTokensGeneratedTotal.Inc(p.ID(), req.Model)
							continue
						}
					default:
					}
					break
				}
				return r.handleProviderFailure(ctx, p, cb, err, tokensEmitted, partialContent, req, sseWriter)
			}

		case chunk, ok := <-chunkChan:
			if !ok {
				// chunkChan closed. Check if an error was reported on errChan before marking success!
				select {
				case err, ok := <-errChan:
					if ok && err != nil {
						return r.handleProviderFailure(ctx, p, cb, err, tokensEmitted, partialContent, req, sseWriter)
					}
				default:
				}

				// Stream ended normally from this provider
				cb.RecordSuccess()
				return sseWriter.WriteDone()
			}

			// Record Time To First Token (TTFT) on first chunk
			if tokensEmitted == 0 {
				ttft := time.Since(streamStart).Seconds()
				observability.DefaultRegistry().LLMTimeToFirstTokenSeconds.Observe(ttft, p.ID(), req.Model, "false")
				if span := observability.SpanFromContext(ctx); span != nil {
					span.AddEvent("first_token", map[string]any{"ttft_ms": ttft * 1000, "provider": p.ID()})
				}
			}

			// Write token chunk to SSE client
			if err := sseWriter.WriteChunk(chunk); err != nil {
				return err
			}

			tokensEmitted++
			partialContent += chunk.DeltaContent
			observability.DefaultRegistry().LLMTokensGeneratedTotal.Inc(p.ID(), req.Model)
		}
	}
}

func (r *Router) handleProviderFailure(
	ctx context.Context,
	p provider.LLMProvider,
	cb *CircuitBreaker,
	err error,
	tokensEmitted int,
	partialContent string,
	req models.CompletionRequest,
	sseWriter *sse.Writer,
) error {
	cb.RecordFailure(err)

	// Case A: Pre-stream failure (no tokens emitted yet)
	if tokensEmitted == 0 {
		return err
	}

	// Case B: Mid-stream failure (tokens already emitted)
	log.Printf("[Router] Mid-stream failure on provider %s after %d tokens: %v", p.ID(), tokensEmitted, err)

	// Emit clean SSE fallback notification event before handing off
	notifyErr := sseWriter.WriteEvent(sse.Event{
		Event: "midstream_fallback",
		Data: FallbackNotification{
			Event:            "midstream_fallback",
			PrimaryProvider:  p.ID(),
			FallbackProvider: "fallback",
			TokensEmitted:    tokensEmitted,
			Message:          "Primary stream interrupted; routing to fallback engine",
			Timestamp:        time.Now().Unix(),
		},
	})
	if notifyErr != nil {
		return notifyErr
	}

	// If in continue mode, append partial generation as assistant prefix
	fallbackReq := req
	if r.recoveryMode == ModeContinue && partialContent != "" {
		fallbackReq.Messages = append(fallbackReq.Messages, models.Message{
			Role:    models.RoleAssistant,
			Content: partialContent,
		})
	}

	// Run fallback through remaining providers
	return r.runMidStreamFallback(ctx, p.ID(), fallbackReq, sseWriter)
}

func (r *Router) runMidStreamFallback(
	ctx context.Context,
	failedProviderID string,
	req models.CompletionRequest,
	sseWriter *sse.Writer,
) error {
	for _, fbProvider := range r.fallbackProviders {
		if fbProvider.ID() == failedProviderID {
			continue
		}

		select {
		case <-ctx.Done():
			return sse.ErrClientDisconnected
		default:
		}

		fbCB := r.breakers[fbProvider.ID()]
		if !fbCB.Allow() {
			continue
		}

		log.Printf("[Router] Executing mid-stream fallback with provider %s", fbProvider.ID())
		chunkChan, errChan := fbProvider.Stream(ctx, req)

		var streamErr error
	fbLoop:
		for {
			select {
			case <-ctx.Done():
				return sse.ErrClientDisconnected

			case err, ok := <-errChan:
				if ok && err != nil {
					streamErr = err
					fbCB.RecordFailure(err)
					break fbLoop
				}

			case chunk, ok := <-chunkChan:
				if !ok {
					// Check if error occurred on errChan
					select {
					case err, ok := <-errChan:
						if ok && err != nil {
							streamErr = err
							fbCB.RecordFailure(err)
							break fbLoop
						}
					default:
					}

					fbCB.RecordSuccess()
					return sseWriter.WriteDone()
				}

				if err := sseWriter.WriteChunk(chunk); err != nil {
					return err
				}
			}
		}

		if streamErr != nil {
			log.Printf("[Router] Mid-stream fallback provider %s failed: %v", fbProvider.ID(), streamErr)
			continue
		}
	}

	return ErrAllProvidersFailed
}
