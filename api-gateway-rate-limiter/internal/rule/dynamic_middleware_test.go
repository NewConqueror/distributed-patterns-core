package rule

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"distributed-api-gateway/internal/limiter"
)

func TestDynamicMiddleware_EndpointIsolationAndHotReload(t *testing.T) {
	initialYAML := `
global:
  capacity: 2
  rate: 1.0
  window: 60s
endpoints:
  - path: "/api/v1/checkout"
    method: "POST"
    capacity: 1
    rate: 0.1
    window: 60s
`

	reloader, err := NewReloader([]byte(initialYAML), ReloaderConfig{})
	if err != nil {
		t.Fatalf("failed to create reloader: %v", err)
	}
	defer reloader.Close()

	memLim := limiter.NewMemoryLimiter(limiter.DefaultMemoryConfig())
	defer memLim.Close()

	mw := NewDynamicMiddleware(reloader, memLim)

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	handler := mw.Handler(nextHandler)

	// 1. Endpoint /api/v1/checkout (POST): capacity is 1
	reqCheckout1 := httptest.NewRequest(http.MethodPost, "/api/v1/checkout", nil)
	recCheckout1 := httptest.NewRecorder()
	handler.ServeHTTP(recCheckout1, reqCheckout1)

	if recCheckout1.Code != http.StatusOK {
		t.Fatalf("expected 1st checkout request 200, got %d", recCheckout1.Code)
	}
	if recCheckout1.Header().Get("X-RateLimit-Limit") != "1" {
		t.Errorf("expected limit 1, got %s", recCheckout1.Header().Get("X-RateLimit-Limit"))
	}
	if recCheckout1.Header().Get("X-RateLimit-Scope") != "endpoint" {
		t.Errorf("expected scope endpoint, got %s", recCheckout1.Header().Get("X-RateLimit-Scope"))
	}

	// 2nd checkout request immediately: capacity is exhausted -> must return 429
	reqCheckout2 := httptest.NewRequest(http.MethodPost, "/api/v1/checkout", nil)
	recCheckout2 := httptest.NewRecorder()
	handler.ServeHTTP(recCheckout2, reqCheckout2)

	if recCheckout2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 2nd checkout request 429, got %d", recCheckout2.Code)
	}

	// 2. Different endpoint /api/v1/items: falls back to global rule (capacity 2)
	reqItems1 := httptest.NewRequest(http.MethodGet, "/api/v1/items", nil)
	recItems1 := httptest.NewRecorder()
	handler.ServeHTTP(recItems1, reqItems1)

	if recItems1.Code != http.StatusOK {
		t.Fatalf("expected items request to succeed, got %d", recItems1.Code)
	}
	if recItems1.Header().Get("X-RateLimit-Limit") != "2" {
		t.Errorf("expected limit 2, got %s", recItems1.Header().Get("X-RateLimit-Limit"))
	}
	if recItems1.Header().Get("X-RateLimit-Scope") != "global" {
		t.Errorf("expected scope global, got %s", recItems1.Header().Get("X-RateLimit-Scope"))
	}

	// 3. ZERO-DOWNTIME HOT-RELOAD: Increase checkout capacity to 10
	updatedYAML := `
global:
  capacity: 2
  rate: 1.0
  window: 60s
endpoints:
  - path: "/api/v1/checkout"
    method: "POST"
    capacity: 10
    rate: 1.0
    window: 60s
`
	err = reloader.ReloadFromBytes([]byte(updatedYAML))
	if err != nil {
		t.Fatalf("hot-reload failed: %v", err)
	}

	// 4. Send 3rd checkout request after hot-reload: should immediately succeed with limit 10!
	reqCheckout3 := httptest.NewRequest(http.MethodPost, "/api/v1/checkout", nil)
	recCheckout3 := httptest.NewRecorder()
	handler.ServeHTTP(recCheckout3, reqCheckout3)

	if recCheckout3.Code != http.StatusOK {
		t.Fatalf("expected checkout request after hot-reload to succeed, got %d", recCheckout3.Code)
	}
	if recCheckout3.Header().Get("X-RateLimit-Limit") != "10" {
		t.Errorf("expected limit 10 after hot-reload, got %s", recCheckout3.Header().Get("X-RateLimit-Limit"))
	}
}

func TestDynamicMiddleware_ClientAPIKeyAuth(t *testing.T) {
	yamlConfig := `
global:
  capacity: 5
  window: 60s
clients:
  - id: "partner-x"
    api_key: "secret-key-xyz"
    capacity: 500
    window: 60s
`
	reloader, _ := NewReloader([]byte(yamlConfig), ReloaderConfig{})
	defer reloader.Close()

	memLim := limiter.NewMemoryLimiter(limiter.DefaultMemoryConfig())
	defer memLim.Close()

	mw := NewDynamicMiddleware(reloader, memLim)
	handler := mw.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Request with Bearer token matching api_key
	req := httptest.NewRequest(http.MethodGet, "/anything", nil)
	req.Header.Set("Authorization", "Bearer secret-key-xyz")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if rec.Header().Get("X-RateLimit-Limit") != "500" {
		t.Errorf("expected limit 500 for authenticated client, got %s", rec.Header().Get("X-RateLimit-Limit"))
	}
	if rec.Header().Get("X-RateLimit-Scope") != "client_global" {
		t.Errorf("expected scope client_global, got %s", rec.Header().Get("X-RateLimit-Scope"))
	}
}
