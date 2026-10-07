package mcp

import (
	"context"
	"fmt"
	"sync"
)

// PromptGenerator produces parameterized prompt messages.
type PromptGenerator func(ctx context.Context, args map[string]string) ([]PromptMessage, error)

type promptEntry struct {
	info      PromptInfo
	generator PromptGenerator
}

// PromptManager manages reusable prompt templates conforming to MCP prompt specs.
type PromptManager struct {
	mu      sync.RWMutex
	prompts map[string]promptEntry
}

// NewPromptManager creates an empty PromptManager.
func NewPromptManager() *PromptManager {
	return &PromptManager{
		prompts: make(map[string]promptEntry),
	}
}

// Register registers a prompt template and its generator.
func (m *PromptManager) Register(info PromptInfo, generator PromptGenerator) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if info.Name == "" {
		return fmt.Errorf("prompt name cannot be empty")
	}

	m.prompts[info.Name] = promptEntry{
		info:      info,
		generator: generator,
	}
	return nil
}

// List returns descriptions for all registered prompts.
func (m *PromptManager) List() []PromptInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	list := make([]PromptInfo, 0, len(m.prompts))
	for _, p := range m.prompts {
		list = append(list, p.info)
	}
	return list
}

// Get evaluates a prompt by name with supplied arguments.
func (m *PromptManager) Get(ctx context.Context, name string, args map[string]string) (*PromptGetResult, error) {
	m.mu.RLock()
	entry, exists := m.prompts[name]
	m.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("prompt not found: %s", name)
	}

	messages, err := entry.generator(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("failed generating prompt %s: %w", name, err)
	}

	return &PromptGetResult{
		Description: entry.info.Description,
		Messages:    messages,
	}, nil
}
