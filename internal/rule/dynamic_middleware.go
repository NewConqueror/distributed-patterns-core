package rule

import (
	"math"
	"net/http"
	"strconv"
	"strings"

	"distributed-api-gateway/internal/limiter"
	"distributed-api-gateway/internal/proxy"
)

// DynamicMiddleware integrates dynamic rule evaluation with rate limiting.
type DynamicMiddleware struct {
	reloader *Reloader
	limiter  limiter.DynamicLimiter
}

// NewDynamicMiddleware constructs a dynamic rule-driven rate limiting middleware.
func NewDynamicMiddleware(reloader *Reloader, lim limiter.DynamicLimiter) *DynamicMiddleware {
	return &DynamicMiddleware{
		reloader: reloader,
		limiter:  lim,
	}
}

// Handler returns standard http.Handler wrapping rate limit rule evaluation.
func (dm *DynamicMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 1. Extract request and client metadata
		clientIP := proxy.ExtractClientIP(r)
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			// Extract bearer token as fallback API key
			authHeader := r.Header.Get("Authorization")
			if strings.HasPrefix(authHeader, "Bearer ") {
				apiKey = strings.TrimPrefix(authHeader, "Bearer ")
			}
		}
		userID := r.Header.Get("X-User-ID")

		// 2. Resolve active rule via lock-free atomic engine pointer
		engine := dm.reloader.Engine()
		matchReq := NewMatchRequest(r, apiKey, clientIP, userID)
		matchedRule := engine.Match(matchReq)

		// 3. Evaluate rate limit with rule-specific limits
		counterKey := matchedRule.CounterKey(matchReq)
		res, err := dm.limiter.AllowRule(r.Context(), counterKey, matchedRule.Capacity, matchedRule.Window, matchedRule.Rate)
		if err != nil {
			http.Error(w, "Rate limit evaluation error", http.StatusInternalServerError)
			return
		}

		// 4. Inject standard rate limiting headers
		w.Header().Set("X-RateLimit-Limit", strconv.FormatInt(res.Limit, 10))
		w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(res.Remaining, 10))
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(res.ResetTime.Unix(), 10))
		w.Header().Set("X-RateLimit-Scope", matchedRule.Scope)

		// 5. Rate limit exceeded: reject immediately with 429
		if !res.Allowed {
			retryAfterSec := int64(math.Ceil(res.RetryAfter.Seconds()))
			if retryAfterSec < 1 {
				retryAfterSec = 1
			}

			w.Header().Set("Retry-After", strconv.FormatInt(retryAfterSec, 10))
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"Too Many Requests","message":"Rate limit exceeded. Please retry later."}` + "\n"))
			return
		}

		next.ServeHTTP(w, r)
	})
}
