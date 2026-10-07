package tool

import (
	"context"
	"strings"
	"testing"
)

func TestRegistry_RegisterAndGet(t *testing.T) {
	reg := NewRegistry()

	orderTool := Tool{
		Definition: Definition{
			Name:        "fetch_order_status",
			Description: "Fetches order status by order ID",
			ReadOnly:    true,
			Parameters: JSONSchema{
				Type:     TypeObject,
				Required: []string{"order_id"},
				Properties: map[string]JSONSchema{
					"order_id": {Type: TypeString, Description: "Unique order identifier"},
				},
			},
		},
		Handler: func(ctx context.Context, args map[string]any) (any, error) {
			return map[string]string{"status": "shipped"}, nil
		},
	}

	err := reg.Register(orderTool)
	if err != nil {
		t.Fatalf("unexpected error registering tool: %v", err)
	}

	// Duplicate registration should error
	err = reg.Register(orderTool)
	if err == nil {
		t.Fatal("expected error on duplicate tool registration, got nil")
	}

	retrieved, exists := reg.Get("fetch_order_status")
	if !exists {
		t.Fatal("expected tool to exist in registry")
	}
	if retrieved.Definition.Name != "fetch_order_status" {
		t.Fatalf("expected tool name 'fetch_order_status', got %s", retrieved.Definition.Name)
	}

	// OpenAI tools format
	openAITools := reg.ToOpenAITools()
	if len(openAITools) != 1 {
		t.Fatalf("expected 1 OpenAI tool, got %d", len(openAITools))
	}
	fnMap := openAITools[0]["function"].(map[string]any)
	if fnMap["name"] != "fetch_order_status" {
		t.Fatalf("expected function name 'fetch_order_status', got %v", fnMap["name"])
	}
}

func TestValidator_RequiredFieldMissing(t *testing.T) {
	schema := JSONSchema{
		Type:     TypeObject,
		Required: []string{"order_id", "user_id"},
		Properties: map[string]JSONSchema{
			"order_id": {Type: TypeString},
			"user_id":  {Type: TypeString},
		},
	}

	args := map[string]any{
		"order_id": "ORD-12345",
	}

	err := Validate(schema, args)
	if err == nil {
		t.Fatal("expected validation error for missing user_id, got nil")
	}

	if !strings.Contains(err.Error(), "user_id") || !strings.Contains(err.Error(), "Şema hatası") {
		t.Fatalf("expected error message to mention 'user_id' and 'Şema hatası', got: %v", err)
	}
}

func TestValidator_TypeMismatch(t *testing.T) {
	schema := JSONSchema{
		Type: TypeObject,
		Properties: map[string]JSONSchema{
			"count":    {Type: TypeInteger},
			"active":   {Type: TypeBoolean},
			"username": {Type: TypeString},
		},
	}

	// Invalid integer (passed string)
	err := Validate(schema, map[string]any{"count": "not-an-integer"})
	if err == nil || !strings.Contains(err.Error(), "count") {
		t.Fatalf("expected type mismatch for count, got: %v", err)
	}

	// Invalid integer (passed float with decimal)
	err = Validate(schema, map[string]any{"count": 12.34})
	if err == nil || !strings.Contains(err.Error(), "tam sayı") {
		t.Fatalf("expected float error for count, got: %v", err)
	}

	// Invalid boolean (passed string)
	err = Validate(schema, map[string]any{"active": "true"})
	if err == nil || !strings.Contains(err.Error(), "boolean") {
		t.Fatalf("expected boolean mismatch error, got: %v", err)
	}
}

func TestValidator_EnumCheck(t *testing.T) {
	schema := JSONSchema{
		Type: TypeObject,
		Properties: map[string]JSONSchema{
			"status": {
				Type: TypeString,
				Enum: []any{"pending", "processing", "completed"},
			},
		},
	}

	// Valid enum
	err := Validate(schema, map[string]any{"status": "completed"})
	if err != nil {
		t.Fatalf("unexpected error for valid enum: %v", err)
	}

	// Invalid enum
	err = Validate(schema, map[string]any{"status": "cancelled"})
	if err == nil || !strings.Contains(err.Error(), "geçerli değil") {
		t.Fatalf("expected enum error, got: %v", err)
	}
}
