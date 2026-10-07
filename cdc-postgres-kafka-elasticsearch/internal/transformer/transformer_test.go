package transformer

import (
	"testing"

	"github.com/segmentio/kafka-go"

	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/model"
)

func TestTransformer_Transform_Create(t *testing.T) {
	cache := NewCategoryCache()
	cache.Set(101, "Mice & Pointers")
	tr := NewTransformer(cache, "products")

	rawMsg := []byte(`{
		"before": null,
		"after": {
			"id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
			"title": "Logitech MX Master 3S",
			"description": "Ergonomic wireless mouse",
			"price": 99.99,
			"category_id": 101,
			"status": "ACTIVE",
			"version": 1
		},
		"source": { "table": "products" },
		"op": "c",
		"ts_ms": 1727264100000
	}`)

	kafkaMsg := kafka.Message{
		Key:   []byte(`{"id":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}`),
		Value: rawMsg,
	}

	item, err := tr.Transform(kafkaMsg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if item.Action != model.ActionIndex {
		t.Errorf("expected ActionIndex, got %s", item.Action)
	}

	if item.ID != "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" {
		t.Errorf("expected ID a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11, got %s", item.ID)
	}

	doc, ok := item.Document.(model.ProductSearchDocument)
	if !ok {
		t.Fatalf("expected ProductSearchDocument, got %T", item.Document)
	}

	if doc.CategoryName != "Mice & Pointers" {
		t.Errorf("expected CategoryName 'Mice & Pointers', got '%s'", doc.CategoryName)
	}

	if doc.Version != 1 {
		t.Errorf("expected Version 1, got %d", doc.Version)
	}
}

func TestTransformer_Transform_Delete(t *testing.T) {
	cache := NewCategoryCache()
	tr := NewTransformer(cache, "products")

	rawMsg := []byte(`{
		"before": {
			"id": "b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22",
			"title": "Keychron Q1 Pro",
			"price": 199.50,
			"category_id": 102,
			"status": "ACTIVE",
			"version": 3
		},
		"after": null,
		"op": "d",
		"ts_ms": 1727264200000
	}`)

	kafkaMsg := kafka.Message{
		Key:   []byte(`{"id":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22"}`),
		Value: rawMsg,
	}

	item, err := tr.Transform(kafkaMsg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if item.Action != model.ActionDelete {
		t.Errorf("expected ActionDelete, got %s", item.Action)
	}

	if item.ID != "b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22" {
		t.Errorf("expected ID b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22, got %s", item.ID)
	}

	if item.Version != 3 {
		t.Errorf("expected Version 3, got %d", item.Version)
	}
}

func TestTransformer_Transform_Tombstone(t *testing.T) {
	cache := NewCategoryCache()
	tr := NewTransformer(cache, "products")

	kafkaMsg := kafka.Message{
		Key:   []byte(`{"id":"b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22"}`),
		Value: nil, // Tombstone
	}

	item, err := tr.Transform(kafkaMsg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !item.IsTombstone {
		t.Errorf("expected IsTombstone to be true")
	}

	if item.ID != "b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22" {
		t.Errorf("expected ID b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22, got %s", item.ID)
	}
}
