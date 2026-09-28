package tokenguard

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
)

// Middleware enforces rate limits and token budgets before requests reach LLM or cache.
type Middleware struct {
	limiter RateLimiter
}

// NewMiddleware creates a new rate limiting middleware.
func NewMiddleware(limiter RateLimiter) *Middleware {
	return &Middleware{
		limiter: limiter,
	}
}

// Handler wraps an http.Handler with rate limiting and budget enforcement.
func (m *Middleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only enforce budget on chat completion endpoints
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			next.ServeHTTP(w, r)
			return
		}

		// 1. Extract identification key (API key or Client IP)
		key := extractClientKey(r)

		// 2. Peek into request body to estimate tokens without consuming the body
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusBadRequest)
			return
		}
		// Restore body for downstream handlers
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

		var req models.CompletionRequest
		if err := json.Unmarshal(bodyBytes, &req); err != nil {
			// If JSON is malformed, downstream handler will return bad request
			next.ServeHTTP(w, r)
			return
		}

		estimatedTokens := EstimatePromptTokens(req.Messages)
		if req.MaxTokens > 0 {
			estimatedTokens += req.MaxTokens
		}

		// 3. Evaluate Budget with RateLimiter
		res, err := m.limiter.Allow(r.Context(), key, estimatedTokens)
		if err != nil {
			// Fail open on rate limiter technical failure or log error
			next.ServeHTTP(w, r)
			return
		}

		// 4. Set Standard RateLimit Headers
		resetSec := int(math.Ceil(res.ResetAfter.Seconds()))
		if resetSec <= 0 {
			resetSec = 1
		}

		w.Header().Set("X-RateLimit-Limit-RPM", strconv.Itoa(res.LimitRPM))
		w.Header().Set("X-RateLimit-Remaining-RPM", strconv.Itoa(res.RemainingRPM))
		w.Header().Set("X-RateLimit-Limit-TPM", strconv.Itoa(res.LimitTPM))
		w.Header().Set("X-RateLimit-Remaining-TPM", strconv.Itoa(res.RemainingTPM))
		w.Header().Set("X-RateLimit-Reset", strconv.Itoa(resetSec))

		// 5. If Budget Exceeded, Block with HTTP 429
		if !res.Allowed {
			w.Header().Set("Retry-After", strconv.Itoa(resetSec))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)

			errMsg := fmt.Sprintf("Rate limit exceeded for %s budget", strings.ToUpper(res.ViolatedBudget))
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"message":             errMsg,
					"type":                "rate_limit_exceeded",
					"violated_budget":     res.ViolatedBudget,
					"retry_after_seconds": resetSec,
				},
			})
			return
		}

		next.ServeHTTP(w, r)
	})
}

func extractClientKey(r *http.Request) string {
	// 1. Authorization: Bearer <token>
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		token := strings.TrimPrefix(auth, "Bearer ")
		if token != "" {
			return "token:" + token
		}
	}

	// 2. X-API-Key header
	if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
		return "key:" + apiKey
	}

	// 3. Client IP address
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		ip = r.RemoteAddr
	}
	if ip == "" {
		ip = "unknown"
	}
	return "ip:" + ip
}
