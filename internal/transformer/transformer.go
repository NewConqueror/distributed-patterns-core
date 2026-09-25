package transformer

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/model"
)

type Transformer struct {
	categoryCache *CategoryCache
	targetIndex   string
}

func NewTransformer(categoryCache *CategoryCache, targetIndex string) *Transformer {
	return &Transformer{
		categoryCache: categoryCache,
		targetIndex:   targetIndex,
	}
}

// Transform parses a Kafka message produced by Debezium and converts it to a BulkItem for Elasticsearch
func (t *Transformer) Transform(msg kafka.Message) (*model.BulkItem, error) {
	// Case 1: Tombstone Message (Value is nil or empty, produced after a DELETE when tombstones.on.delete=true)
	if len(msg.Value) == 0 {
		keyID, err := t.extractKeyID(msg.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to extract key from tombstone: %w", err)
		}
		return &model.BulkItem{
			Action:      model.ActionDelete,
			ID:          keyID,
			Index:       t.targetIndex,
			Version:     0, // Will be skipped or handled gracefully
			KafkaMsg:    msg,
			IsTombstone: true,
		}, nil
	}

	// Case 2: Standard Debezium JSON Envelope
	var envelope model.DebeziumEnvelope[model.ProductRaw]
	if err := json.Unmarshal(msg.Value, &envelope); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Debezium envelope: %w", err)
	}

	switch envelope.Op {
	case model.OpDelete:
		// Delete Operation: Extract id and version from 'before' state
		var docID string
		var version int64 = 1
		if envelope.Before != nil {
			docID = envelope.Before.ID
			if envelope.Before.Version > 0 {
				version = envelope.Before.Version
			}
		} else {
			var err error
			docID, err = t.extractKeyID(msg.Key)
			if err != nil {
				return nil, fmt.Errorf("missing before state and failed to extract key on delete: %w", err)
			}
		}

		sourceTs := envelope.Source.TsMs
		if sourceTs == 0 {
			sourceTs = envelope.TsMs
		}

		return &model.BulkItem{
			Action:     model.ActionDelete,
			ID:         docID,
			Index:      t.targetIndex,
			Version:    version,
			KafkaMsg:   msg,
			SourceTsMs: sourceTs,
		}, nil

	case model.OpCreate, model.OpUpdate, model.OpRead:
		// Index / Upsert Operation: Flatten and denormalize 'after' state
		if envelope.After == nil {
			return nil, fmt.Errorf("op '%s' requires non-nil 'after' payload", envelope.Op)
		}

		raw := envelope.After
		desc := ""
		if raw.Description != nil {
			desc = *raw.Description
		}

		// Denormalize category name from in-memory cache
		categoryName := t.categoryCache.Get(raw.CategoryID)

		doc := model.ProductSearchDocument{
			ID:           raw.ID,
			Title:        raw.Title,
			Description:  desc,
			Price:        raw.Price,
			CategoryID:   raw.CategoryID,
			CategoryName: categoryName,
			Status:       raw.Status,
			Version:      raw.Version,
			UpdatedAt:    time.Now().UTC(),
			IndexedAt:    time.Now().UTC(),
		}

		sourceTs := envelope.Source.TsMs
		if sourceTs == 0 {
			sourceTs = envelope.TsMs
		}

		return &model.BulkItem{
			Action:     model.ActionIndex,
			ID:         raw.ID,
			Index:      t.targetIndex,
			Version:    raw.Version,
			Document:   doc,
			KafkaMsg:   msg,
			SourceTsMs: sourceTs,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported Debezium op: %s", envelope.Op)
	}
}

// extractKeyID parses the Kafka key payload: either JSON {"id": "..."} or raw string
func (t *Transformer) extractKeyID(keyBytes []byte) (string, error) {
	if len(keyBytes) == 0 {
		return "", fmt.Errorf("empty Kafka key")
	}

	var parsedKey model.DebeziumKey
	if err := json.Unmarshal(keyBytes, &parsedKey); err == nil && parsedKey.ID != "" {
		return parsedKey.ID, nil
	}

	// Fallback to raw string
	return string(keyBytes), nil
}
