package tokenguard

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
)

func TestEstimatePromptTokens(t *testing.T) {
	messages := []models.Message{
		{Role: models.RoleSystem, Content: "You are an AI assistant."},
		{Role: models.RoleUser, Content: "Hello world!"},
	}

	tokens := EstimatePromptTokens(messages)
	// System: 3 framing + ~7 tokens
	// User: 3 framing + ~3 tokens
	// Assistant prime: 3 framing
	// Total roughly ~19
	if tokens < 15 || tokens > 30 {
		t.Fatalf("expected estimated tokens between 15 and 30, got %d", tokens)
	}
}

func TestMemorySlidingWindowLimiter_RPM_And_TPM(t *testing.T) {
	limiter := NewMemorySlidingWindowLimiter(LimiterConfig{
		MaxRPM:         3,
		MaxTPM:         100,
		WindowDuration: 50 * time.Millisecond,
	})

	ctx := context.Background()

	// 1. First 3 requests with 20 tokens each -> Allowed
	for i := 0; i < 3; i++ {
		res, err := limiter.Allow(ctx, "user-1", 20)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !res.Allowed {
			t.Fatalf("expected request %d to be allowed", i+1)
		}
	}

	// 2. 4th request exceeds RPM limit (MaxRPM=3)
	resRPM, err := limiter.Allow(ctx, "user-1", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resRPM.Allowed {
		t.Fatal("expected 4th request to exceed RPM, but was allowed")
	}
	if resRPM.ViolatedBudget != "rpm" {
		t.Fatalf("expected violated budget 'rpm', got %s", resRPM.ViolatedBudget)
	}

	// 3. Test TPM limit for a different user
	resTPM, err := limiter.Allow(ctx, "user-2", 150) // MaxTPM=100
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resTPM.Allowed {
		t.Fatal("expected request exceeding MaxTPM to be rejected")
	}
	if resTPM.ViolatedBudget != "tpm" {
		t.Fatalf("expected violated budget 'tpm', got %s", resTPM.ViolatedBudget)
	}

	// 4. Wait for window to slide (cooldown)
	time.Sleep(60 * time.Millisecond)

	// user-1 should be allowed again
	resAfterCooldown, err := limiter.Allow(ctx, "user-1", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resAfterCooldown.Allowed {
		t.Fatal("expected request to be allowed after sliding window cleared")
	}
}

func TestMiddleware_BudgetEnforcer(t *testing.T) {
	limiter := NewMemorySlidingWindowLimiter(LimiterConfig{
		MaxRPM:         2,
		MaxTPM:         1000,
		WindowDuration: time.Minute,
	})

	mw := NewMiddleware(limiter)

	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	handler := mw.Handler(dummyHandler)

	bodyBytes, _ := json.Marshal(models.CompletionRequest{
		Model: "gpt-4o",
		Messages: []models.Message{
			{Role: models.RoleUser, Content: "Hello"},
		},
	})

	// 1st request -> 200 OK
	req1 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(bodyBytes))
	req1.Header.Set("Authorization", "Bearer test-user-key")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec1.Code)
	}
	if rec1.Header().Get("X-RateLimit-Limit-RPM") != "2" {
		t.Errorf("expected limit RPM header 2, got %s", rec1.Header().Get("X-RateLimit-Limit-RPM"))
	}
	if rec1.Header().Get("X-RateLimit-Remaining-RPM") != "1" {
		t.Errorf("expected remaining RPM header 1, got %s", rec1.Header().Get("X-RateLimit-Remaining-RPM"))
	}

	// 2nd request -> 200 OK
	req2 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(bodyBytes))
	req2.Header.Set("Authorization", "Bearer test-user-key")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for 2nd request, got %d", rec2.Code)
	}

	// 3rd request -> 429 Too Many Requests
	req3 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(bodyBytes))
	req3.Header.Set("Authorization", "Bearer test-user-key")
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", rec3.Code)
	}
	if rec3.Header().Get("Retry-After") == "" {
		t.Errorf("expected Retry-After header on 429 response")
	}
	if rec3.Header().Get("X-RateLimit-Reset") == "" {
		t.Errorf("expected X-RateLimit-Reset header on 429 response")
	}
}
