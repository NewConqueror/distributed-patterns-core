package models

import (
	"fmt"
	"time"
)

// Role defines the role of a message sender.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// Message represents a chat completion message.
type Message struct {
	Role       Role   `json:"role"`
	Content    string `json:"content"`
	Name       string `json:"name,omitempty"`
	ToolCallID string `json:"tool_call_id,omitempty"`
}

// CompletionRequest represents the normalized chat completion request.
type CompletionRequest struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Temperature float32   `json:"temperature,omitempty"`
	TopP        float32   `json:"top_p,omitempty"`
	MaxTokens   int       `json:"max_tokens,omitempty"`
	Stream      bool      `json:"stream"`
	Stop        []string  `json:"stop,omitempty"`
}

// StreamChunk represents a single streaming token or delta from upstream.
type StreamChunk struct {
	ID           string         `json:"id,omitempty"`
	Model        string         `json:"model,omitempty"`
	DeltaContent string         `json:"delta_content"`
	FinishReason string         `json:"finish_reason,omitempty"`
	Usage        *UsageMetadata `json:"usage,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
}

// UsageMetadata records token consumption.
type UsageMetadata struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// UpstreamError represents an error returned by an upstream LLM provider.
type UpstreamError struct {
	StatusCode int    `json:"status_code"`
	Provider   string `json:"provider"`
	Message    string `json:"message"`
	Retryable  bool   `json:"retryable"`
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("[%s] HTTP %d: %s (retryable: %v)", e.Provider, e.StatusCode, e.Message, e.Retryable)
}
