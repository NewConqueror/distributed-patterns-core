package limiter

import (
	"math"
	"net/http"
	"strconv"

	"distributed-api-gateway/internal/proxy"
)

// KeyExtractor extracts a unique rate-limiting key from an incoming HTTP request.
type KeyExtractor func(r *http.Request) string

// IPKeyExtractor extracts client IP using proxy IP resolution (X-Forwarded-For / RemoteAddr).
func IPKeyExtractor() KeyExtractor {
	return func(r *http.Request) string {
		return proxy.ExtractClientIP(r)
	}
}

// HeaderKeyExtractor extracts client identifier from a custom header (e.g. "X-API-Key").
func HeaderKeyExtractor(headerName string, fallback KeyExtractor) KeyExtractor {
	return func(r *http.Request) string {
		if val := r.Header.Get(headerName); val != "" {
			return val
		}
		if fallback != nil {
			return fallback(r)
		}
		return ""
	}
}

// Middleware enforces rate limiting on incoming HTTP requests.
type Middleware struct {
	limiter      Limiter
	keyExtractor KeyExtractor
}

// NewMiddleware constructs an HTTP rate-limiting middleware.
func NewMiddleware(limiter Limiter, keyExtractor KeyExtractor) *Middleware {
	if keyExtractor == nil {
		keyExtractor = IPKeyExtractor()
	}
	return &Middleware{
		limiter:      limiter,
		keyExtractor: keyExtractor,
	}
}

// Handler returns standard http.Handler middleware.
func (m *Middleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := m.keyExtractor(r)

		res, err := m.limiter.Allow(r.Context(), key)
		if err != nil {
			http.Error(w, "Rate limit evaluation error", http.StatusInternalServerError)
			return
		}

		// Inject standard headers on all responses
		w.Header().Set("X-RateLimit-Limit", strconv.FormatInt(res.Limit, 10))
		w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(res.Remaining, 10))
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(res.ResetTime.Unix(), 10))

		// If rate limit exceeded: reject immediately without draining or buffering request body
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
