package limiter

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMiddleware_AllowAndHeaders(t *testing.T) {
	memLim := NewMemoryLimiter(MemoryConfig{
		Rate:      10,
		Capacity:  5,
		NumShards: 2,
	})
	defer memLim.Close()

	mw := NewMiddleware(memLim, IPKeyExtractor())

	nextCalled := false
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	handler := mw.Handler(nextHandler)

	req := httptest.NewRequest(http.MethodGet, "/resource", nil)
	req.RemoteAddr = "10.10.10.1:12345"
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if !nextCalled {
		t.Fatal("expected next handler to be called")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	// Verify standard headers
	if rec.Header().Get("X-RateLimit-Limit") != "5" {
		t.Errorf("expected X-RateLimit-Limit 5, got %s", rec.Header().Get("X-RateLimit-Limit"))
	}
	if rec.Header().Get("X-RateLimit-Remaining") != "4" {
		t.Errorf("expected X-RateLimit-Remaining 4, got %s", rec.Header().Get("X-RateLimit-Remaining"))
	}
	if rec.Header().Get("X-RateLimit-Reset") == "" {
		t.Errorf("expected X-RateLimit-Reset to be present")
	}
}

func TestMiddleware_RejectAnd429(t *testing.T) {
	memLim := NewMemoryLimiter(MemoryConfig{
		Rate:      1, // 1 token per second
		Capacity:  1, // Only 1 burst
		NumShards: 2,
	})
	defer memLim.Close()

	mw := NewMiddleware(memLim, HeaderKeyExtractor("X-API-Key", IPKeyExtractor()))

	nextCalledCount := 0
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalledCount++
		w.WriteHeader(http.StatusOK)
	})

	handler := mw.Handler(nextHandler)

	// 1st request with API key: should pass
	req1 := httptest.NewRequest(http.MethodGet, "/api/data", nil)
	req1.Header.Set("X-API-Key", "api-secret-key-1")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 1st request to succeed, got %d", rec1.Code)
	}

	// 2nd immediate request with same API key: must return 429
	req2 := httptest.NewRequest(http.MethodPost, "/api/data", strings.NewReader(`{"large":"body"}`))
	req2.Header.Set("X-API-Key", "api-secret-key-1")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 2nd request to return 429 Too Many Requests, got %d", rec2.Code)
	}
	if nextCalledCount != 1 {
		t.Errorf("next handler was called %d times, expected only 1", nextCalledCount)
	}

	// Check 429 response headers
	if rec2.Header().Get("X-RateLimit-Remaining") != "0" {
		t.Errorf("expected remaining 0, got %s", rec2.Header().Get("X-RateLimit-Remaining"))
	}
	retryAfter := rec2.Header().Get("Retry-After")
	if retryAfter == "" || retryAfter == "0" {
		t.Errorf("expected positive Retry-After header, got %q", retryAfter)
	}
	if !strings.Contains(rec2.Body.String(), "Too Many Requests") {
		t.Errorf("expected body to contain error message, got %s", rec2.Body.String())
	}
}

func TestMiddleware_DistinctKeys(t *testing.T) {
	memLim := NewMemoryLimiter(MemoryConfig{
		Rate:      1,
		Capacity:  1,
		NumShards: 2,
	})
	defer memLim.Close()

	mw := NewMiddleware(memLim, HeaderKeyExtractor("X-API-Key", nil))
	handler := mw.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Client A: exhaust quota
	reqA := httptest.NewRequest(http.MethodGet, "/", nil)
	reqA.Header.Set("X-API-Key", "client-A")
	recA := httptest.NewRecorder()
	handler.ServeHTTP(recA, reqA)
	if recA.Code != http.StatusOK {
		t.Fatalf("expected 200 for client A, got %d", recA.Code)
	}

	// Client B: should NOT be blocked by client A
	reqB := httptest.NewRequest(http.MethodGet, "/", nil)
	reqB.Header.Set("X-API-Key", "client-B")
	recB := httptest.NewRecorder()
	handler.ServeHTTP(recB, reqB)
	if recB.Code != http.StatusOK {
		t.Fatalf("client B should be independent and allowed, got %d", recB.Code)
	}
}
