package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
)

// OpenAIProvider implements LLMProvider for OpenAI and OpenAI-compatible APIs.
type OpenAIProvider struct {
	id         string
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

// OpenAIConfig holds configuration for OpenAIProvider.
type OpenAIConfig struct {
	ID         string
	APIKey     string
	BaseURL    string
	HTTPClient *http.Client
}

// NewOpenAIProvider creates a new instance of OpenAIProvider.
func NewOpenAIProvider(cfg OpenAIConfig) *OpenAIProvider {
	id := cfg.ID
	if id == "" {
		id = "openai"
	}
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	baseURL = strings.TrimRight(baseURL, "/")

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 0, // Streaming relies on context cancellation and idle timeouts
		}
	}

	return &OpenAIProvider{
		id:         id,
		apiKey:     cfg.APIKey,
		baseURL:    baseURL,
		httpClient: httpClient,
	}
}

func (p *OpenAIProvider) ID() string {
	return p.id
}

type openAIChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIChatRequest struct {
	Model       string              `json:"model"`
	Messages    []openAIChatMessage `json:"messages"`
	Temperature float32             `json:"temperature,omitempty"`
	TopP        float32             `json:"top_p,omitempty"`
	MaxTokens   int                 `json:"max_tokens,omitempty"`
	Stream      bool                `json:"stream"`
}

type openAIStreamChunkChoice struct {
	Index int `json:"index"`
	Delta struct {
		Role    string `json:"role,omitempty"`
		Content string `json:"content,omitempty"`
	} `json:"delta"`
	FinishReason *string `json:"finish_reason,omitempty"`
}

type openAIStreamChunkResponse struct {
	ID      string                    `json:"id"`
	Model   string                    `json:"model"`
	Choices []openAIStreamChunkChoice `json:"choices"`
}

func (p *OpenAIProvider) Stream(ctx context.Context, req models.CompletionRequest) (<-chan models.StreamChunk, <-chan error) {
	chunkChan := make(chan models.StreamChunk, 32)
	errChan := make(chan error, 1)

	go func() {
		defer close(chunkChan)
		defer close(errChan)

		// 1. Prepare payload
		apiMessages := make([]openAIChatMessage, 0, len(req.Messages))
		for _, m := range req.Messages {
			apiMessages = append(apiMessages, openAIChatMessage{
				Role:    string(m.Role),
				Content: m.Content,
			})
		}

		payload := openAIChatRequest{
			Model:       req.Model,
			Messages:    apiMessages,
			Temperature: req.Temperature,
			TopP:        req.TopP,
			MaxTokens:   req.MaxTokens,
			Stream:      true,
		}

		bodyBytes, err := json.Marshal(payload)
		if err != nil {
			select {
			case errChan <- fmt.Errorf("failed to marshal request: %w", err):
			case <-ctx.Done():
			}
			return
		}

		url := fmt.Sprintf("%s/chat/completions", p.baseURL)
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
		if err != nil {
			select {
			case errChan <- fmt.Errorf("failed to create http request: %w", err):
			case <-ctx.Done():
			}
			return
		}

		httpReq.Header.Set("Content-Type", "application/json")
		if p.apiKey != "" {
			httpReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", p.apiKey))
		}

		// 2. Execute HTTP Request
		resp, err := p.httpClient.Do(httpReq)
		if err != nil {
			select {
			case errChan <- &models.UpstreamError{
				StatusCode: http.StatusServiceUnavailable,
				Provider:   p.id,
				Message:    fmt.Sprintf("network error: %v", err),
				Retryable:  true,
			}:
			case <-ctx.Done():
			}
			return
		}
		defer resp.Body.Close()

		// 3. Handle non-200 responses
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
			select {
			case errChan <- &models.UpstreamError{
				StatusCode: resp.StatusCode,
				Provider:   p.id,
				Message:    string(body),
				Retryable:  retryable,
			}:
			case <-ctx.Done():
			}
			return
		}

		// 4. Stream and parse SSE chunks
		reader := bufio.NewReader(resp.Body)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			line, err := reader.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					return
				}
				select {
				case errChan <- &models.UpstreamError{
					StatusCode: http.StatusBadGateway,
					Provider:   p.id,
					Message:    fmt.Sprintf("error reading stream: %v", err),
					Retryable:  true,
				}:
				case <-ctx.Done():
				}
				return
			}

			lineStr := strings.TrimSpace(string(line))
			if lineStr == "" {
				continue
			}

			if !strings.HasPrefix(lineStr, "data:") {
				continue
			}

			data := strings.TrimSpace(strings.TrimPrefix(lineStr, "data:"))
			if data == "[DONE]" {
				return
			}

			var chunkResp openAIStreamChunkResponse
			if err := json.Unmarshal([]byte(data), &chunkResp); err != nil {
				// Some proxies might send keep-alive or comment in data, skip malformed line
				continue
			}

			if len(chunkResp.Choices) == 0 {
				continue
			}

			choice := chunkResp.Choices[0]
			var finishReason string
			if choice.FinishReason != nil {
				finishReason = *choice.FinishReason
			}

			chunk := models.StreamChunk{
				ID:           chunkResp.ID,
				Model:        chunkResp.Model,
				DeltaContent: choice.Delta.Content,
				FinishReason: finishReason,
				CreatedAt:    time.Now(),
			}

			select {
			case chunkChan <- chunk:
			case <-ctx.Done():
				return
			}
		}
	}()

	return chunkChan, errChan
}
