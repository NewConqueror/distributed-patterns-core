package orchestrator

import (
	"context"
	"net/http"
	"testing"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
)

func TestCircuitBreaker_Tripping(t *testing.T) {
	cb := NewCircuitBreaker("test-provider", CircuitBreakerConfig{
		WindowSize:       5,
		FailureThreshold: 3,
		SuccessThreshold: 2,
		OpenCooldown:     50 * time.Millisecond,
	})

	if cb.State() != StateClosed {
		t.Fatalf("expected initial state CLOSED, got %s", cb.State())
	}
	if !cb.Allow() {
		t.Fatal("expected Allow() to be true in CLOSED state")
	}

	err429 := &models.UpstreamError{StatusCode: http.StatusTooManyRequests, Provider: "test"}
	err500 := &models.UpstreamError{StatusCode: http.StatusInternalServerError, Provider: "test"}

	// 1st failure
	cb.RecordFailure(err429)
	if cb.State() != StateClosed {
		t.Fatalf("expected state CLOSED after 1 failure, got %s", cb.State())
	}

	// 2nd failure
	cb.RecordFailure(context.DeadlineExceeded)
	if cb.State() != StateClosed {
		t.Fatalf("expected state CLOSED after 2 failures, got %s", cb.State())
	}

	// 3rd failure: should trip to OPEN
	cb.RecordFailure(err500)
	if cb.State() != StateOpen {
		t.Fatalf("expected state OPEN after 3 failures, got %s", cb.State())
	}
	if cb.Allow() {
		t.Fatal("expected Allow() to be false in OPEN state before cooldown")
	}

	// Wait for cooldown
	time.Sleep(60 * time.Millisecond)

	// State should transition to HALF-OPEN on State() or Allow()
	if !cb.Allow() {
		t.Fatal("expected Allow() to be true in HALF-OPEN state (trial)")
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected state HALF-OPEN, got %s", cb.State())
	}

	// First probe success
	cb.RecordSuccess()
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected state still HALF-OPEN after 1 success, got %s", cb.State())
	}

	// Second probe success -> should close
	cb.RecordSuccess()
	if cb.State() != StateClosed {
		t.Fatalf("expected state CLOSED after 2 successes, got %s", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenFailure(t *testing.T) {
	cb := NewCircuitBreaker("test-provider", CircuitBreakerConfig{
		WindowSize:       5,
		FailureThreshold: 2,
		SuccessThreshold: 2,
		OpenCooldown:     30 * time.Millisecond,
	})

	// Trip to OPEN
	cb.RecordFailure(&models.UpstreamError{StatusCode: 503})
	cb.RecordFailure(&models.UpstreamError{StatusCode: 503})
	if cb.State() != StateOpen {
		t.Fatalf("expected OPEN state, got %s", cb.State())
	}

	// Wait for cooldown
	time.Sleep(40 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("expected trial request allowed in Half-Open")
	}

	// Trial probe fails -> immediately OPEN again
	cb.RecordFailure(&models.UpstreamError{StatusCode: 500})
	if cb.State() != StateOpen {
		t.Fatalf("expected state OPEN after failed probe, got %s", cb.State())
	}
}
