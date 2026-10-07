package model

import (
	"time"
)

// ProductRaw represents the raw PostgreSQL products table row as emitted by Debezium
type ProductRaw struct {
	ID          string  `json:"id"`
	Title       string  `json:"title"`
	Description *string `json:"description"`
	Price       float64 `json:"price"`
	CategoryID  int     `json:"category_id"`
	Status      string  `json:"status"`
	Version     int64   `json:"version"`
	CreatedAt   any     `json:"created_at"`
	UpdatedAt   any     `json:"updated_at"`
}

// ProductSearchDocument represents the denormalized, flattened document for Elasticsearch
type ProductSearchDocument struct {
	ID           string    `json:"id"`
	Title        string    `json:"title"`
	Description  string    `json:"description"`
	Price        float64   `json:"price"`
	CategoryID   int       `json:"category_id"`
	CategoryName string    `json:"category_name"` // Denormalized field
	Status       string    `json:"status"`
	Version      int64     `json:"version"`
	UpdatedAt    time.Time `json:"updated_at"`
	IndexedAt    time.Time `json:"indexed_at"`
}
