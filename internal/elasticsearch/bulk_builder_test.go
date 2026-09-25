package elasticsearch

import (
	"strings"
	"testing"

	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/model"
)

func TestBuildNDJSON_IndexAndDelete(t *testing.T) {
	items := []model.BulkItem{
		{
			Action:  model.ActionIndex,
			ID:      "prod-1",
			Index:   "products",
			Version: 2,
			Document: map[string]any{
				"title": "Test Product",
				"price": 49.99,
			},
		},
		{
			Action:  model.ActionDelete,
			ID:      "prod-2",
			Index:   "products",
			Version: 5,
		},
	}

	ndjson, err := BuildNDJSON(items)
	if err != nil {
		t.Fatalf("failed to build NDJSON: %v", err)
	}

	output := string(ndjson)
	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")

	// 2 items: 1 index (2 lines) + 1 delete (1 line) = 3 lines total
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines in NDJSON, got %d:\n%s", len(lines), output)
	}

	// Verify line 1 (index header with external_gte)
	if !strings.Contains(lines[0], `"version_type":"external_gte"`) || !strings.Contains(lines[0], `"version":2`) {
		t.Errorf("line 1 missing external_gte or version: %s", lines[0])
	}

	// Verify line 2 (document body)
	if !strings.Contains(lines[1], `"title":"Test Product"`) {
		t.Errorf("line 2 missing doc content: %s", lines[1])
	}

	// Verify line 3 (delete header with external_gte)
	if !strings.Contains(lines[2], `"delete"`) || !strings.Contains(lines[2], `"version":5`) {
		t.Errorf("line 3 missing delete action or version: %s", lines[2])
	}
}
