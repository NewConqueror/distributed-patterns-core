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

// OllamaProvider implements LLMProvider for local Ollama instances.
type OllamaProvider struct {
	id         string
	baseURL    string
	httpClient *http.Client
}

// OllamaConfig holds configuration for OllamaProvider.
type OllamaConfig struct {
	ID         string
	BaseURL    string
	HTTPClient *http.Client
}

// NewOllamaProvider creates a new instance of OllamaProvider.
func NewOllamaProvider(cfg OllamaConfig) *OllamaProvider {
	id := cfg.ID
	if id == "" {
		id = "ollama"
	}
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = "http://localhost:11434"
	}
	baseURL = strings.TrimRight(baseURL, "/")

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 0,
		}
	}

	return &OllamaProvider{
		id:         id,
		baseURL:    baseURL,
		httpClient: httpClient,
	}
}

func (p *OllamaProvider) ID() string {
	return p.id
}

type ollamaChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ollamaOptions struct {
	Temperature float32 `json:"temperature,omitempty"`
	TopP        float32 `json:"top_p,omitempty"`
	NumPredict  int     `json:"num_predict,omitempty"`
}

type ollamaChatRequest struct {
	Model    string              `json:"model"`
	Messages []ollamaChatMessage `json:"messages"`
	Stream   bool                `json:"stream"`
	Options  *ollamaOptions      `json:"options,omitempty"`
}

type ollamaChatResponse struct {
	Model     string            `json:"model"`
	CreatedAt string            `json:"created_at"`
	Message   ollamaChatMessage `json:"message"`
	Done      bool              `json:"done"`
	DoneReason string           `json:"done_reason,omitempty"`
	PromptEvalCount int         `json:"prompt_eval_count,omitempty"`
	EvalCount       int         `json:"eval_count,omitempty"`
}

func (p *OllamaProvider) Stream(ctx context.Context, req models.CompletionRequest) (<-chan models.StreamChunk, <-chan error) {
	chunkChan := make(chan models.StreamChunk, 32)
	errChan := make(chan error, 1)

	go func() {
		defer close(chunkChan)
		defer close(errChan)

		// 1. Prepare payload
		apiMessages := make([]ollamaChatMessage, 0, len(req.Messages))
		for _, m := range req.Messages {
			apiMessages = append(apiMessages, ollamaChatMessage{
				Role:    string(m.Role),
				Content: m.Content,
			})
		}

		var opts *ollamaOptions
		if req.Temperature > 0 || req.TopP > 0 || req.MaxTokens > 0 {
			opts = &ollamaOptions{
				Temperature: req.Temperature,
				TopP:        req.TopP,
				NumPredict:  req.MaxTokens,
			}
		}

		payload := ollamaChatRequest{
			Model:    req.Model,
			Messages: apiMessages,
			Stream:   true,
			Options:  opts,
		}

		bodyBytes, err := json.Marshal(payload)
		if err != nil {
			select {
			case errChan <- fmt.Errorf("failed to marshal ollama request: %w", err):
			case <-ctx.Done():
			}
			return
		}

		url := fmt.Sprintf("%s/api/chat", p.baseURL)
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
		if err != nil {
			select {
			case errChan <- fmt.Errorf("failed to create http request: %w", err):
			case <-ctx.Done():
			}
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")

		// 2. Execute HTTP Request
		resp, err := p.httpClient.Do(httpReq)
		if err != nil {
			select {
			case errChan <- &models.UpstreamError{
				StatusCode: http.StatusServiceUnavailable,
				Provider:   p.id,
				Message:    fmt.Sprintf("ollama network error: %v", err),
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

		// 4. Stream and parse NDJSON lines
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
					Message:    fmt.Sprintf("error reading ollama stream: %v", err),
					Retryable:  true,
				}:
				case <-ctx.Done():
				}
				return
			}

			lineTrimmed := bytes.TrimSpace(line)
			if len(lineTrimmed) == 0 {
				continue
			}

			var streamResp ollamaChatResponse
			if err := json.Unmarshal(lineTrimmed, &streamResp); err != nil {
				continue
			}

			var usage *models.UsageMetadata
			if streamResp.Done && (streamResp.PromptEvalCount > 0 || streamResp.EvalCount > 0) {
				usage = &models.UsageMetadata{
					PromptTokens:     streamResp.PromptEvalCount,
					CompletionTokens: streamResp.EvalCount,
					TotalTokens:      streamResp.PromptEvalCount + streamResp.EvalCount,
				}
			}

			chunk := models.StreamChunk{
				Model:        streamResp.Model,
				DeltaContent: streamResp.Message.Content,
				FinishReason: streamResp.DoneReason,
				Usage:        usage,
				CreatedAt:    time.Now(),
			}

			select {
			case chunkChan <- chunk:
			case <-ctx.Done():
				return
			}

			if streamResp.Done {
				return
			}
		}
	}()

	return chunkChan, errChan
}
