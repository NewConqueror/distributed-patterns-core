package model

import (
	"github.com/segmentio/kafka-go"
)

type ActionType string

const (
	ActionIndex  ActionType = "index"
	ActionDelete ActionType = "delete"
)

// BulkItem represents a single operation in a bulk request to Elasticsearch
type BulkItem struct {
	Action      ActionType
	ID          string
	Index       string
	Version     int64
	Document    any
	KafkaMsg    kafka.Message
	IsTombstone bool
}

// BulkResponse represents the response from Elasticsearch _bulk API
type BulkResponse struct {
	Took   int                                 `json:"took"`
	Errors bool                                `json:"errors"`
	Items  []map[string]BulkItemResponseDetail `json:"items"`
}

// BulkItemResponseDetail holds status and possible error for each bulk item
type BulkItemResponseDetail struct {
	Index   string         `json:"_index"`
	ID      string         `json:"_id"`
	Status  int            `json:"status"`
	Version int64          `json:"_version"`
	Result  string         `json:"result"`
	Error   *BulkItemError `json:"error,omitempty"`
}

type BulkItemError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}
