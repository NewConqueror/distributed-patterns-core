package memory

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/cache"
)

type memoryRecord struct {
	id           string
	promptText   string
	embedding    []float32
	norm         float64
	responseText string
	metadata     map[string]any
	createdAt    time.Time
}

// Store is an in-memory implementation of cache.VectorStore.
type Store struct {
	mu      sync.RWMutex
	records []memoryRecord
	nextID  int64
}

// NewStore creates a new in-memory vector store.
func NewStore() *Store {
	return &Store{
		records: make([]memoryRecord, 0),
	}
}

// FindSimilar finds the closest stored item where cosine distance is below maxDistance.
func (s *Store) FindSimilar(ctx context.Context, embedding []float32, maxDistance float64) (*cache.CachedItem, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	queryNorm := computeNorm(embedding)
	if queryNorm == 0 {
		return nil, nil
	}

	var bestItem *cache.CachedItem
	bestDistance := math.MaxFloat64

	for _, rec := range s.records {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		dist := cosineDistance(embedding, queryNorm, rec.embedding, rec.norm)
		if dist < maxDistance && dist < bestDistance {
			bestDistance = dist
			bestItem = &cache.CachedItem{
				ID:           rec.id,
				PromptText:   rec.promptText,
				ResponseText: rec.responseText,
				Distance:     dist,
				Similarity:   1.0 - dist,
				Metadata:     rec.metadata,
				CreatedAt:    rec.createdAt,
			}
		}
	}

	return bestItem, nil
}

// Store saves a new prompt, embedding, and response.
func (s *Store) Store(ctx context.Context, promptText string, embedding []float32, responseText string, metadata map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	norm := computeNorm(embedding)
	s.nextID++

	rec := memoryRecord{
		id:           fmt.Sprintf("mem-%d", s.nextID),
		promptText:   promptText,
		embedding:    embedding,
		norm:         norm,
		responseText: responseText,
		metadata:     metadata,
		createdAt:    time.Now(),
	}

	s.records = append(s.records, rec)
	return nil
}

func computeNorm(v []float32) float64 {
	var sum float64
	for _, val := range v {
		sum += float64(val * val)
	}
	return math.Sqrt(sum)
}

func cosineDistance(v1 []float32, norm1 float64, v2 []float32, norm2 float64) float64 {
	if len(v1) != len(v2) || norm1 == 0 || norm2 == 0 {
		return 1.0 // maximum distance
	}

	var dot float64
	for i := range v1 {
		dot += float64(v1[i] * v2[i])
	}

	similarity := dot / (norm1 * norm2)
	// Bound to [-1.0, 1.0] to prevent floating point inaccuracies
	if similarity > 1.0 {
		similarity = 1.0
	} else if similarity < -1.0 {
		similarity = -1.0
	}

	return 1.0 - similarity
}
