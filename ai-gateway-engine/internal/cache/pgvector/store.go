package pgvector

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/cache"
)

// Store implements cache.VectorStore backed by PostgreSQL and pgvector.
type Store struct {
	db *sql.DB
}

// NewStore creates a new pgvector-backed vector store.
func NewStore(db *sql.DB) *Store {
	return &Store{
		db: db,
	}
}

// FormatVector converts []float32 to pgvector string format "[0.01, -0.02, ...]".
func FormatVector(vec []float32) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i, v := range vec {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.FormatFloat(float64(v), 'f', -1, 32))
	}
	sb.WriteByte(']')
	return sb.String()
}

// FindSimilar finds the most similar cached response where cosine distance < maxDistance.
func (s *Store) FindSimilar(ctx context.Context, embedding []float32, maxDistance float64) (*cache.CachedItem, error) {
	if len(embedding) == 0 {
		return nil, nil
	}

	vecStr := FormatVector(embedding)

	query := `
		SELECT id, prompt_text, response_text, metadata, created_at,
		       (prompt_embedding <=> $1) AS distance
		FROM semantic_cache
		WHERE (prompt_embedding <=> $1) < $2
		ORDER BY distance ASC
		LIMIT 1;
	`

	var (
		id           string
		promptText   string
		responseText string
		metadataJSON []byte
		createdAt    time.Time
		distance     float64
	)

	err := s.db.QueryRowContext(ctx, query, vecStr, maxDistance).Scan(
		&id,
		&promptText,
		&responseText,
		&metadataJSON,
		&createdAt,
		&distance,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("pgvector similarity query failed: %w", err)
	}

	var metadata map[string]any
	if len(metadataJSON) > 0 {
		_ = json.Unmarshal(metadataJSON, &metadata)
	}

	return &cache.CachedItem{
		ID:           id,
		PromptText:   promptText,
		ResponseText: responseText,
		Distance:     distance,
		Similarity:   1.0 - distance,
		Metadata:     metadata,
		CreatedAt:    createdAt,
	}, nil
}

// Store inserts a new prompt, embedding, and response into PostgreSQL.
func (s *Store) Store(ctx context.Context, promptText string, embedding []float32, responseText string, metadata map[string]any) error {
	vecStr := FormatVector(embedding)

	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		metaBytes = []byte("{}")
	}

	query := `
		INSERT INTO semantic_cache (prompt_text, prompt_embedding, response_text, metadata, created_at)
		VALUES ($1, $2, $3, $4, $5);
	`

	_, err = s.db.ExecContext(ctx, query, promptText, vecStr, responseText, metaBytes, time.Now())
	if err != nil {
		return fmt.Errorf("failed to insert semantic cache entry: %w", err)
	}

	return nil
}
