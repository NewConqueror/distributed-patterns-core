package cache

import (
	"context"
	"time"
)

// DefaultMaxDistance is the maximum cosine distance for a cache hit (0.08 corresponds to >= 92% similarity).
const DefaultMaxDistance = 0.08

// CachedItem represents a stored completion item in the vector cache.
type CachedItem struct {
	ID           string         `json:"id"`
	PromptText   string         `json:"prompt_text"`
	ResponseText string         `json:"response_text"`
	Distance     float64        `json:"distance"`
	Similarity   float64        `json:"similarity"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
}

// VectorStore abstracts vector database operations for semantic caching.
type VectorStore interface {
	// FindSimilar searches for a cached item with cosine distance less than maxDistance.
	FindSimilar(ctx context.Context, embedding []float32, maxDistance float64) (*CachedItem, error)
	// Store saves a new prompt, its vector embedding, and the completed response.
	Store(ctx context.Context, promptText string, embedding []float32, responseText string, metadata map[string]any) error
}
