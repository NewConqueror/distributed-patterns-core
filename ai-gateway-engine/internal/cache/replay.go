package cache

import (
	"context"
	"strings"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/sse"
)

// ReplayConfig holds options for replaying cached responses.
type ReplayConfig struct {
	// ChunkDelay is the simulated delay between streamed chunks (defaults to 15ms).
	ChunkDelay time.Duration
	// MaxChunkWords specifies roughly how many words per chunk to emit (defaults to 2).
	MaxChunkWords int
}

// ReplayEngine streams a cached response as simulated SSE tokens to preserve UX.
type ReplayEngine struct {
	chunkDelay    time.Duration
	maxChunkWords int
}

// NewReplayEngine initializes ReplayEngine with sensible defaults (15ms delay).
func NewReplayEngine(cfg ReplayConfig) *ReplayEngine {
	delay := cfg.ChunkDelay
	if delay <= 0 {
		delay = 15 * time.Millisecond
	}
	words := cfg.MaxChunkWords
	if words <= 0 {
		words = 2
	}

	return &ReplayEngine{
		chunkDelay:    delay,
		maxChunkWords: words,
	}
}

// Replay streams the cached item to the client through SSE.
func (r *ReplayEngine) Replay(ctx context.Context, item *CachedItem, sseWriter *sse.Writer) error {
	// 1. Send Cache Hit event metadata to the client
	err := sseWriter.WriteEvent(sse.Event{
		Event: "cache_hit",
		Data: map[string]any{
			"cached":     true,
			"similarity": item.Similarity,
			"distance":   item.Distance,
			"source":     "semantic_cache",
		},
	})
	if err != nil {
		return err
	}

	// 2. Tokenize response into words/chunks
	chunks := splitIntoChunks(item.ResponseText, r.maxChunkWords)

	// 3. Stream chunks with simulated token-by-token delay
	for i, chunkText := range chunks {
		select {
		case <-ctx.Done():
			return sse.ErrClientDisconnected
		case <-time.After(r.chunkDelay):
		}

		var finishReason string
		if i == len(chunks)-1 {
			finishReason = "stop"
		}

		chunk := models.StreamChunk{
			ID:           item.ID,
			Model:        "cached",
			DeltaContent: chunkText,
			FinishReason: finishReason,
			CreatedAt:    time.Now(),
		}

		if err := sseWriter.WriteChunk(chunk); err != nil {
			return err
		}
	}

	// 4. Send standard [DONE] marker
	return sseWriter.WriteDone()
}

// splitIntoChunks breaks a text into natural small chunks preserving whitespace.
func splitIntoChunks(text string, wordsPerChunk int) []string {
	if text == "" {
		return nil
	}

	words := strings.Fields(text)
	if len(words) == 0 {
		return []string{text}
	}

	var chunks []string
	var current strings.Builder

	count := 0
	for i, w := range words {
		if count > 0 {
			current.WriteString(" ")
		}
		current.WriteString(w)
		count++

		if count >= wordsPerChunk || i == len(words)-1 {
			if i < len(words)-1 {
				current.WriteString(" ")
			}
			chunks = append(chunks, current.String())
			current.Reset()
			count = 0
		}
	}

	return chunks
}
