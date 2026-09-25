package circuitbreaker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type mockHealthChecker struct {
	attempts atomic.Int32
	failRuns int32
}

func (m *mockHealthChecker) CheckHealth(ctx context.Context) (string, bool, error) {
	att := m.attempts.Add(1)
	if att <= m.failRuns {
		return "red", false, errors.New("cluster unhealthy")
	}
	return "green", true, nil
}

func TestCircuitBreaker_StateTransitionsAndRecovery(t *testing.T) {
	mockChecker := &mockHealthChecker{failRuns: 2}
	cb := NewCircuitBreaker(mockChecker, 10*time.Millisecond, 50*time.Millisecond)

	if cb.GetState() != StateClosed {
		t.Errorf("expected initial state CLOSED, got %s", cb.GetState())
	}

	if cb.IsOpen() {
		t.Errorf("expected IsOpen to be false")
	}

	// Trip circuit breaker
	cb.Trip(errors.New("simulated 503 service unavailable"))

	if !cb.IsOpen() {
		t.Errorf("expected IsOpen to be true after trip")
	}

	// Wait for background probe to recover (mock fails 2 times, then succeeds on 3rd attempt)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := cb.WaitIfOpen(ctx)
	if err != nil {
		t.Fatalf("expected WaitIfOpen to succeed after recovery, got error: %v", err)
	}

	if cb.GetState() != StateClosed {
		t.Errorf("expected state to be CLOSED after recovery, got %s", cb.GetState())
	}

	if mockChecker.attempts.Load() < 3 {
		t.Errorf("expected at least 3 probe attempts, got %d", mockChecker.attempts.Load())
	}
}

func TestCircuitBreaker_WaitIfOpen_ContextCancel(t *testing.T) {
	mockChecker := &mockHealthChecker{failRuns: 100} // never recovers in this test
	cb := NewCircuitBreaker(mockChecker, 50*time.Millisecond, 100*time.Millisecond)

	cb.Trip(errors.New("down"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := cb.WaitIfOpen(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}
