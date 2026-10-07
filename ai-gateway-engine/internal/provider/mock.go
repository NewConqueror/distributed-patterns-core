package provider

import (
	"context"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
)

// MockProvider provides a controllable LLMProvider implementation for testing.
type MockProvider struct {
	id              string
	chunks          []models.StreamChunk
	chunkDelay      time.Duration
	initialErr      error
	midStreamErr    error
	failAfterChunks int
}

// MockProviderOption configures MockProvider.
type MockProviderOption func(*MockProvider)

func WithMockChunks(chunks []models.StreamChunk) MockProviderOption {
	return func(m *MockProvider) {
		m.chunks = chunks
	}
}

func WithMockInitialError(err error) MockProviderOption {
	return func(m *MockProvider) {
		m.initialErr = err
	}
}

func WithMockMidStreamError(err error, afterChunks int) MockProviderOption {
	return func(m *MockProvider) {
		m.midStreamErr = err
		m.failAfterChunks = afterChunks
	}
}

func WithMockDelay(d time.Duration) MockProviderOption {
	return func(m *MockProvider) {
		m.chunkDelay = d
	}
}

func NewMockProvider(id string, opts ...MockProviderOption) *MockProvider {
	m := &MockProvider{
		id: id,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *MockProvider) ID() string {
	return m.id
}

func (m *MockProvider) Stream(ctx context.Context, req models.CompletionRequest) (<-chan models.StreamChunk, <-chan error) {
	chunkChan := make(chan models.StreamChunk, 16)
	errChan := make(chan error, 1)

	go func() {
		defer close(chunkChan)
		defer close(errChan)

		if m.initialErr != nil {
			select {
			case errChan <- m.initialErr:
			case <-ctx.Done():
			}
			return
		}

		for i, ch := range m.chunks {
			if m.chunkDelay > 0 {
				select {
				case <-time.After(m.chunkDelay):
				case <-ctx.Done():
					return
				}
			}

			if m.midStreamErr != nil && i >= m.failAfterChunks {
				select {
				case errChan <- m.midStreamErr:
				case <-ctx.Done():
				}
				return
			}

			select {
			case chunkChan <- ch:
			case <-ctx.Done():
				return
			}
		}
	}()

	return chunkChan, errChan
}
