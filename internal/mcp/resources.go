package mcp

import (
	"context"
	"fmt"
	"sync"
)

// ResourceReader reads content for a given resource URI.
type ResourceReader func(ctx context.Context, uri string) (string, error)

type resourceEntry struct {
	info   ResourceInfo
	reader ResourceReader
}

// ResourceManager manages dynamic context resources that models can read.
type ResourceManager struct {
	mu        sync.RWMutex
	resources map[string]resourceEntry
}

// NewResourceManager creates an empty ResourceManager.
func NewResourceManager() *ResourceManager {
	return &ResourceManager{
		resources: make(map[string]resourceEntry),
	}
}

// Register adds a dynamic resource provider.
func (m *ResourceManager) Register(info ResourceInfo, reader ResourceReader) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if info.URI == "" {
		return fmt.Errorf("resource URI cannot be empty")
	}

	m.resources[info.URI] = resourceEntry{
		info:   info,
		reader: reader,
	}
	return nil
}

// List returns descriptors for all registered resources.
func (m *ResourceManager) List() []ResourceInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	list := make([]ResourceInfo, 0, len(m.resources))
	for _, r := range m.resources {
		list = append(list, r.info)
	}
	return list
}

// Read fetches dynamic content for the requested resource URI.
func (m *ResourceManager) Read(ctx context.Context, uri string) (*ResourceContent, error) {
	m.mu.RLock()
	entry, exists := m.resources[uri]
	m.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("resource not found: %s", uri)
	}

	text, err := entry.reader(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("error reading resource %s: %w", uri, err)
	}

	return &ResourceContent{
		URI:      uri,
		MimeType: entry.info.MimeType,
		Text:     text,
	}, nil
}
