package provider

import (
	"context"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
)

// LLMProvider defines the common interface for all upstream LLM backends.
type LLMProvider interface {
	// ID returns a unique identifier for the provider (e.g. "openai", "ollama").
	ID() string
	// Stream sends a completion request and returns channels for streaming chunks and errors.
	Stream(ctx context.Context, req models.CompletionRequest) (<-chan models.StreamChunk, <-chan error)
}
