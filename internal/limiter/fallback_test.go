package limiter

import (
	"context"
	"errors"
	"testing"
	"time"
)

type mockFailingLimiter struct {
	failCount int
}

func (m *mockFailingLimiter) Allow(ctx context.Context, key string) (*Result, error) {
	m.failCount++
	return nil, errors.New("redis connection refused")
}

func (m *mockFailingLimiter) Close() error {
	return nil
}

type mockWorkingLimiter struct {
	allowed bool
}

func (m *mockWorkingLimiter) Allow(ctx context.Context, key string) (*Result, error) {
	return &Result{
		Allowed:   m.allowed,
		Limit:     50,
		Remaining: 49,
		ResetTime: time.Now().Add(time.Minute),
	}, nil
}

func (m *mockWorkingLimiter) Close() error {
	return nil
}

func TestFallbackLimiter_PrimarySuccess(t *testing.T) {
	primary := &mockWorkingLimiter{allowed: true}
	fallback := &mockWorkingLimiter{allowed: false}

	fl := NewFallbackLimiter(FallbackLimiterConfig{
		Primary:  primary,
		Fallback: fallback,
		Strategy: FailClosed,
	})
	defer fl.Close()

	res, err := fl.Allow(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Allowed {
		t.Fatal("expected request to be allowed by primary")
	}
	if fl.FallbackInvocations() != 0 {
		t.Errorf("expected 0 fallback invocations, got %d", fl.FallbackInvocations())
	}
}

func TestFallbackLimiter_FallbackToMemory(t *testing.T) {
	primary := &mockFailingLimiter{}
	fallback := NewMemoryLimiter(MemoryConfig{
		Rate:      10,
		Capacity:  2,
		NumShards: 2,
	})
	defer fallback.Close()

	fl := NewFallbackLimiter(FallbackLimiterConfig{
		Primary:           primary,
		Fallback:          fallback,
		Strategy:          FailClosed,
		ThresholdFailures: 1,
		Cooldown:          100 * time.Millisecond,
	})
	defer fl.Close()

	ctx := context.Background()

	// 1st request: primary fails, fallback allows
	res1, err := fl.Allow(ctx, "user-test")
	if err != nil {
		t.Fatalf("expected fallback to handle request without error: %v", err)
	}
	if !res1.Allowed {
		t.Fatal("expected fallback to allow 1st request")
	}

	// 2nd request: primary is bypassed due to circuit open, fallback allows
	res2, err := fl.Allow(ctx, "user-test")
	if err != nil {
		t.Fatalf("expected fallback to handle request without error: %v", err)
	}
	if !res2.Allowed {
		t.Fatal("expected fallback to allow 2nd request")
	}

	// 3rd request: capacity 2 is exhausted on fallback, request rejected
	res3, err := fl.Allow(ctx, "user-test")
	if err != nil {
		t.Fatalf("expected rate limit rejection without error: %v", err)
	}
	if res3.Allowed {
		t.Fatal("expected fallback to reject 3rd request after capacity exhaustion")
	}

	if fl.FallbackInvocations() < 2 {
		t.Errorf("expected at least 2 fallback invocations, got %d", fl.FallbackInvocations())
	}
}

func TestFallbackLimiter_FailOpen(t *testing.T) {
	primary := &mockFailingLimiter{}
	var fallback Limiter = nil // Both unavailable

	fl := NewFallbackLimiter(FallbackLimiterConfig{
		Primary:  primary,
		Fallback: fallback,
		Strategy: FailOpen,
	})
	defer fl.Close()

	res, err := fl.Allow(context.Background(), "user-x")
	if err != nil {
		t.Fatalf("fail-open should not return error: %v", err)
	}
	if !res.Allowed {
		t.Fatal("fail-open must allow request when infrastructure fails")
	}
}

func TestFallbackLimiter_FailClosed(t *testing.T) {
	primary := &mockFailingLimiter{}
	var fallback Limiter = nil

	fl := NewFallbackLimiter(FallbackLimiterConfig{
		Primary:  primary,
		Fallback: fallback,
		Strategy: FailClosed,
	})
	defer fl.Close()

	res, err := fl.Allow(context.Background(), "user-y")
	if err == nil {
		t.Fatal("fail-closed must return error when infrastructure fails")
	}
	if res.Allowed {
		t.Fatal("fail-closed must reject request when infrastructure fails")
	}
}
