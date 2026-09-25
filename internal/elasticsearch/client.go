package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/model"
)

// TransientError indicates a network failure, timeout, or 5xx server error that should be retried
type TransientError struct {
	StatusCode int
	Message    string
	Cause      error
}

func (e *TransientError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("transient ES error (status %d): %s (cause: %v)", e.StatusCode, e.Message, e.Cause)
	}
	return fmt.Sprintf("transient ES error (status %d): %s", e.StatusCode, e.Message)
}

func (e *TransientError) Unwrap() error {
	return e.Cause
}

func IsTransient(err error) bool {
	var tErr *TransientError
	return errors.As(err, &tErr)
}

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

// ClusterHealthResponse represents Elasticsearch /_cluster/health JSON
type ClusterHealthResponse struct {
	ClusterName string `json:"cluster_name"`
	Status      string `json:"status"` // "green", "yellow", "red"
	TimedOut    bool   `json:"timed_out"`
}

// CheckHealth queries /_cluster/health and verifies if cluster status is green or yellow
func (c *Client) CheckHealth(ctx context.Context) (status string, isHealthy bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/_cluster/health", nil)
	if err != nil {
		return "", false, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		return "", false, fmt.Errorf("cluster health returned 5xx status: %d", resp.StatusCode)
	}

	var health ClusterHealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return "", false, fmt.Errorf("failed to decode health response: %w", err)
	}

	healthy := health.Status == "green" || health.Status == "yellow"
	return health.Status, healthy, nil
}

// Ping checks if Elasticsearch cluster is accessible
func (c *Client) Ping(ctx context.Context) error {
	_, healthy, err := c.CheckHealth(ctx)
	if err != nil {
		return err
	}
	if !healthy {
		return fmt.Errorf("cluster is not healthy (status red)")
	}
	return nil
}

// EnsureIndex creates the products index with appropriate mappings if it does not already exist
func (c *Client) EnsureIndex(ctx context.Context, indexName string) error {
	headReq, err := http.NewRequestWithContext(ctx, http.MethodHead, fmt.Sprintf("%s/%s", c.baseURL, indexName), nil)
	if err != nil {
		return &TransientError{Cause: err, Message: "failed to check index existence"}
	}

	headResp, err := c.httpClient.Do(headReq)
	if err != nil {
		return &TransientError{Cause: err, Message: "connection error checking index existence"}
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
		return &TransientError{Cause: err, Message: "connection error during index creation"}
	}
	defer createResp.Body.Close()

	if createResp.StatusCode >= 500 {
		return &TransientError{StatusCode: createResp.StatusCode, Message: "index creation returned server error"}
	}

	if createResp.StatusCode >= 400 && createResp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(createResp.Body)
		return fmt.Errorf("failed to create index %s (status %d): %s", indexName, createResp.StatusCode, string(body))
	}

	log.Printf("[Elasticsearch] Index '%s' verified / created successfully", indexName)
	return nil
}

// BulkResult holds the overall response and specific failed items (e.g. mapping mismatch)
type BulkResult struct {
	Response     *model.BulkResponse
	PermanentMap map[string]string // ID -> Error reason (for DLQ routing)
}

// Bulk sends an NDJSON payload to Elasticsearch /_bulk and handles transient errors vs permanent item errors
func (c *Client) Bulk(ctx context.Context, ndjson []byte) (*BulkResult, error) {
	if len(ndjson) == 0 {
		return &BulkResult{Response: &model.BulkResponse{}, PermanentMap: make(map[string]string)}, nil
	}

	endpoint := fmt.Sprintf("%s/_bulk", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(ndjson))
	if err != nil {
		return nil, fmt.Errorf("failed to create bulk request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Network drop, timeout, or connection refused -> Transient
		return nil, &TransientError{
			StatusCode: 0,
			Message:    "network failure communicating with Elasticsearch",
			Cause:      err,
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &TransientError{
			StatusCode: resp.StatusCode,
			Message:    "failed to read response body",
			Cause:      err,
		}
	}

	// 5xx Server Error or 429 Too Many Requests -> Transient Error (Circuit Breaker must trip)
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		return nil, &TransientError{
			StatusCode: resp.StatusCode,
			Message:    fmt.Sprintf("Elasticsearch bulk returned server status %d: %s", resp.StatusCode, string(body)),
		}
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("bulk request failed with HTTP %d: %s", resp.StatusCode, string(body))
	}

	var bulkResp model.BulkResponse
	if err := json.Unmarshal(body, &bulkResp); err != nil {
		return nil, fmt.Errorf("failed to decode bulk JSON response: %w", err)
	}

	result := &BulkResult{
		Response:     &bulkResp,
		PermanentMap: make(map[string]string),
	}

	// Inspect item-level errors
	if bulkResp.Errors {
		for _, itemMap := range bulkResp.Items {
			for action, detail := range itemMap {
				if detail.Status >= 400 {
					// 409 Conflict: Expected with version_type=external_gte when stale events arrive out of order
					if detail.Status == http.StatusConflict {
						log.Printf("[ES Version Conflict] Item ID=%s (%s): %s (Fresher version already in ES, safely ignored)", detail.ID, detail.ID, detail.Error.Reason)
						continue
					}
					// 404 Not Found on Delete: Document was already deleted
					if detail.Status == http.StatusNotFound && action == "delete" {
						log.Printf("[ES Notice] Delete on ID=%s skipped (document not found in index)", detail.ID)
						continue
					}
					// Permanent item-level error (e.g., 400 mapper_parsing_exception)
					errReason := "unknown error"
					if detail.Error != nil {
						errReason = fmt.Sprintf("[%s] %s", detail.Error.Type, detail.Error.Reason)
					}
					result.PermanentMap[detail.ID] = errReason
					log.Printf("[ES Item Error] Permanent failure for ID=%s status=%d err=%s", detail.ID, detail.Status, errReason)
				}
			}
		}
	}

	return result, nil
}
