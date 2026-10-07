package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"

	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/model"
)

type BulkHeader struct {
	Index       string `json:"_index"`
	ID          string `json:"_id"`
	Version     *int64 `json:"version,omitempty"`
	VersionType string `json:"version_type,omitempty"`
}

type IndexActionWrap struct {
	Index BulkHeader `json:"index"`
}

type DeleteActionWrap struct {
	Delete BulkHeader `json:"delete"`
}

// BuildNDJSON formats an array of BulkItems into Elasticsearch _bulk NDJSON format with external_gte versioning
func BuildNDJSON(items []model.BulkItem) ([]byte, error) {
	var buf bytes.Buffer

	for _, item := range items {
		var versionPtr *int64
		var versionType string
		if item.Version > 0 {
			v := item.Version
			versionPtr = &v
			versionType = "external_gte" // Protects against out-of-order Kafka message replay
		}

		switch item.Action {
		case model.ActionIndex:
			header := IndexActionWrap{
				Index: BulkHeader{
					Index:       item.Index,
					ID:          item.ID,
					Version:     versionPtr,
					VersionType: versionType,
				},
			}
			headerBytes, err := json.Marshal(header)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal index header for id %s: %w", item.ID, err)
			}
			buf.Write(headerBytes)
			buf.WriteByte('\n')

			docBytes, err := json.Marshal(item.Document)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal doc for id %s: %w", item.ID, err)
			}
			buf.Write(docBytes)
			buf.WriteByte('\n')

		case model.ActionDelete:
			header := DeleteActionWrap{
				Delete: BulkHeader{
					Index:       item.Index,
					ID:          item.ID,
					Version:     versionPtr,
					VersionType: versionType,
				},
			}
			headerBytes, err := json.Marshal(header)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal delete header for id %s: %w", item.ID, err)
			}
			buf.Write(headerBytes)
			buf.WriteByte('\n')

		default:
			return nil, fmt.Errorf("unsupported action type: %s", item.Action)
		}
	}

	return buf.Bytes(), nil
}
