package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/model"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Ping checks if Elasticsearch cluster is accessible
func (c *Client) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/_cluster/health", nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("ping returned status: %d", resp.StatusCode)
	}
	return nil
}

// EnsureIndex creates the products index with appropriate mappings if it does not already exist
func (c *Client) EnsureIndex(ctx context.Context, indexName string) error {
	headReq, err := http.NewRequestWithContext(ctx, http.MethodHead, fmt.Sprintf("%s/%s", c.baseURL, indexName), nil)
	if err != nil {
		return err
	}

	headResp, err := c.httpClient.Do(headReq)
	if err != nil {
		return err
	}
	defer headResp.Body.Close()

	if headResp.StatusCode == http.StatusOK {
		return nil // Index exists
	}

	// Create index with mapping
	mapping := `{
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 0
		},
		"mappings": {
			"properties": {
				"id": { "type": "keyword" },
				"title": { 
					"type": "text",
					"fields": { "keyword": { "type": "keyword", "ignore_above": 256 } }
				},
				"description": { "type": "text" },
				"price": { "type": "double" },
				"category_id": { "type": "integer" },
				"category_name": { "type": "keyword" },
				"status": { "type": "keyword" },
				"version": { "type": "long" },
				"updated_at": { "type": "date" },
				"indexed_at": { "type": "date" }
			}
		}
	}`

	createReq, err := http.NewRequestWithContext(ctx, http.MethodPut, fmt.Sprintf("%s/%s", c.baseURL, indexName), bytes.NewBufferString(mapping))
	if err != nil {
		return err
	}
	createReq.Header.Set("Content-Type", "application/json")

	createResp, err := c.httpClient.Do(createReq)
	if err != nil {
		return err
	}
	defer createResp.Body.Close()

	if createResp.StatusCode >= 400 && createResp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(createResp.Body)
		return fmt.Errorf("failed to create index %s (status %d): %s", indexName, createResp.StatusCode, string(body))
	}

	log.Printf("[Elasticsearch] Index '%s' verified / created successfully", indexName)
	return nil
}

// Bulk sends an NDJSON payload to Elasticsearch /_bulk and handles item-level responses & version conflicts
func (c *Client) Bulk(ctx context.Context, ndjson []byte) (*model.BulkResponse, error) {
	if len(ndjson) == 0 {
		return &model.BulkResponse{}, nil
	}

	endpoint := fmt.Sprintf("%s/_bulk", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(ndjson))
	if err != nil {
		return nil, fmt.Errorf("failed to create bulk request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("bulk HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read bulk response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("bulk request returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	var bulkResp model.BulkResponse
	if err := json.Unmarshal(body, &bulkResp); err != nil {
		return nil, fmt.Errorf("failed to decode bulk JSON response: %w", err)
	}

	// Inspect errors
	if bulkResp.Errors {
		var fatalErrors []string
		for _, itemMap := range bulkResp.Items {
			for action, detail := range itemMap {
				if detail.Status >= 400 {
					// 409 Conflict: Expected with version_type=external_gte when stale events arrive out of order
					if detail.Status == http.StatusConflict {
						log.Printf("[ES Version Conflict] Item ID=%s (%s): %s (Fresher version already in ES, safely ignored)", detail.ID, action, detail.Error.Reason)
						continue
					}
					// 404 Not Found on Delete: Document was already deleted
					if detail.Status == http.StatusNotFound && action == "delete" {
						log.Printf("[ES Notice] Delete on ID=%s skipped (document not found in index)", detail.ID)
						continue
					}
					// Genuine fatal error
					errReason := "unknown"
					if detail.Error != nil {
						errReason = detail.Error.Reason
					}
					fatalErrors = append(fatalErrors, fmt.Sprintf("ID=%s status=%d err=%s", detail.ID, detail.Status, errReason))
				}
			}
		}

		if len(fatalErrors) > 0 {
			return &bulkResp, fmt.Errorf("bulk had %d critical errors: %s", len(fatalErrors), strings.Join(fatalErrors, "; "))
		}
	}

	return &bulkResp, nil
}
