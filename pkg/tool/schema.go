package tool

// SchemaType defines primitive JSON Schema data types.
type SchemaType string

const (
	TypeObject  SchemaType = "object"
	TypeString  SchemaType = "string"
	TypeNumber  SchemaType = "number"
	TypeInteger SchemaType = "integer"
	TypeBoolean SchemaType = "boolean"
	TypeArray   SchemaType = "array"
)

// JSONSchema defines a standard JSON Schema descriptor for tool arguments.
type JSONSchema struct {
	Type        SchemaType            `json:"type"`
	Description string                `json:"description,omitempty"`
	Properties  map[string]JSONSchema `json:"properties,omitempty"`
	Required    []string              `json:"required,omitempty"`
	Enum        []any                 `json:"enum,omitempty"`
	Items       *JSONSchema           `json:"items,omitempty"`
}
