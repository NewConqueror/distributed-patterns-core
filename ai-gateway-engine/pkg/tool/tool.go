package tool

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrToolAlreadyRegistered is returned when registering a duplicate tool name.
	ErrToolAlreadyRegistered = errors.New("tool already registered")
	// ErrToolNotFound is returned when looking up a nonexistent tool.
	ErrToolNotFound = errors.New("tool not found")
)

// Definition describes an executable tool for LLMs and clients.
type Definition struct {
	Name                string     `json:"name"`
	Description         string     `json:"description"`
	Parameters          JSONSchema `json:"parameters"`
	ReadOnly            bool       `json:"read_only"`
	RequiredPermissions []string   `json:"required_permissions,omitempty"`
}

// Handler executes the business logic of a tool with validated arguments.
type Handler func(ctx context.Context, args map[string]any) (any, error)

// Tool bundles a Definition with its executable Handler.
type Tool struct {
	Definition Definition
	Handler    Handler
}

// Registry manages the set of available tools in a thread-safe manner.
type Registry struct {
	mu    sync.RWMutex
	tools map[string]Tool
}

// NewRegistry initializes an empty Tool Registry.
func NewRegistry() *Registry {
	return &Registry{
		tools: make(map[string]Tool),
	}
}

// Register registers a tool in the registry. Returns error if name is duplicate.
func (r *Registry) Register(t Tool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if t.Definition.Name == "" {
		return errors.New("tool name cannot be empty")
	}

	if _, exists := r.tools[t.Definition.Name]; exists {
		return fmt.Errorf("%w: %s", ErrToolAlreadyRegistered, t.Definition.Name)
	}

	r.tools[t.Definition.Name] = t
	return nil
}

// Get retrieves a tool by name.
func (r *Registry) Get(name string) (Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	t, exists := r.tools[name]
	return t, exists
}

// List returns all registered tool definitions.
func (r *Registry) List() []Definition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	defs := make([]Definition, 0, len(r.tools))
	for _, t := range r.tools {
		defs = append(defs, t.Definition)
	}
	return defs
}

// ToOpenAITools converts registered tools into the standard OpenAI function calling format.
func (r *Registry) ToOpenAITools() []map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tools := make([]map[string]any, 0, len(r.tools))
	for _, t := range r.tools {
		tools = append(tools, map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        t.Definition.Name,
				"description": t.Definition.Description,
				"parameters":  t.Definition.Parameters,
			},
		})
	}
	return tools
}
