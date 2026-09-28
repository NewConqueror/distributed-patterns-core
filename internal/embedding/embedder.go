package embedding

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/normalizer"
)

// Embedder generates vector embeddings for given input text.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
	Dimension() int
}

// OpenAIEmbedderConfig holds configuration for OpenAI embeddings API.
type OpenAIEmbedderConfig struct {
	APIKey     string
	BaseURL    string
	Model      string
	HTTPClient *http.Client
}

// OpenAIEmbedder implements Embedder using OpenAI's /v1/embeddings endpoint.
type OpenAIEmbedder struct {
	apiKey     string
	baseURL    string
	model      string
	dimension  int
	httpClient *http.Client
}

// NewOpenAIEmbedder creates a new OpenAIEmbedder.
func NewOpenAIEmbedder(cfg OpenAIEmbedderConfig) *OpenAIEmbedder {
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	baseURL = strings.TrimRight(baseURL, "/")

	model := cfg.Model
	dim := 1536
	if model == "" {
		model = "text-embedding-3-small"
		dim = 1536
	} else if model == "text-embedding-3-large" {
		dim = 3072
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	return &OpenAIEmbedder{
		apiKey:     cfg.APIKey,
		baseURL:    baseURL,
		model:      model,
		dimension:  dim,
		httpClient: httpClient,
	}
}

func (e *OpenAIEmbedder) Dimension() int {
	return e.dimension
}

type openAIEmbeddingRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

type openAIEmbeddingData struct {
	Embedding []float32 `json:"embedding"`
	Index     int       `json:"index"`
}

type openAIEmbeddingResponse struct {
	Data  []openAIEmbeddingData `json:"data"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// Embed normalizes input text and requests vector embedding from OpenAI.
func (e *OpenAIEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	cleanText := normalizer.Normalize(text)
	if cleanText == "" {
		return nil, errors.New("cannot embed empty text")
	}

	payload := openAIEmbeddingRequest{
		Model: e.model,
		Input: cleanText,
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal embedding request: %w", err)
	}

	reqURL := fmt.Sprintf("%s/embeddings", e.baseURL)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if e.apiKey != "" {
		httpReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", e.apiKey))
	}

	resp, err := e.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("embedding request failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embedding API returned status %d: %s", resp.StatusCode, string(respBytes))
	}

	var embResp openAIEmbeddingResponse
	if err := json.Unmarshal(respBytes, &embResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal embedding response: %w", err)
	}

	if embResp.Error != nil && embResp.Error.Message != "" {
		return nil, fmt.Errorf("embedding API error: %s", embResp.Error.Message)
	}

	if len(embResp.Data) == 0 || len(embResp.Data[0].Embedding) == 0 {
		return nil, errors.New("received empty embedding data from API")
	}

	return embResp.Data[0].Embedding, nil
}

// MockEmbedder provides deterministic or configurable embeddings for testing.
type MockEmbedder struct {
	DimensionVal int
	CustomVector []float32
	Err          error
}

func NewMockEmbedder(dim int) *MockEmbedder {
	if dim <= 0 {
		dim = 1536
	}
	return &MockEmbedder{
		DimensionVal: dim,
	}
}

func (m *MockEmbedder) Dimension() int {
	return m.DimensionVal
}

// Embed generates a deterministic normalized vector based on SHA-256 hash of the text.
// Similar texts produce identical hashes if normalized identically.
func (m *MockEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	if len(m.CustomVector) > 0 {
		return m.CustomVector, nil
	}

	cleanText := normalizer.Normalize(text)
	if cleanText == "" {
		return nil, errors.New("cannot embed empty text")
	}

	vec := make([]float32, m.DimensionVal)
	hash := sha256.Sum256([]byte(cleanText))

	var sumSq float64
	for i := 0; i < m.DimensionVal; i++ {
		// Seed pseudo-random deterministic floats from hash
		idx := (i * 4) % (len(hash) - 4)
		seed := binary.LittleEndian.Uint32(hash[idx : idx+4])
		val := (float32(seed%10000) / 5000.0) - 1.0 // between -1.0 and 1.0
		vec[i] = val
		sumSq += float64(val * val)
	}

	// Normalize vector (L2 norm)
	norm := float32(math.Sqrt(sumSq))
	if norm > 0 {
		for i := range vec {
			vec[i] /= norm
		}
	}

	return vec, nil
}
