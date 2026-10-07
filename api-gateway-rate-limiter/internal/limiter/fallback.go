package limiter

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// FallbackStrategy controls behavior when both primary and secondary rate limiters fail.
type FallbackStrategy int

const (
	// FailOpen permits incoming requests if rate limiting infrastructure is completely unavailable.
	FailOpen FallbackStrategy = iota
	// FailClosed blocks incoming requests if rate limiting infrastructure is completely unavailable.
	FailClosed
)

// FallbackLimiterConfig specifies parameters for resilient multi-tier rate limiting.
type FallbackLimiterConfig struct {
	Primary           Limiter          // Distributed Redis limiter
	Fallback          Limiter          // Local in-memory limiter
	Strategy          FallbackStrategy // Fail-Open or Fail-Closed on catastrophic failure
	Cooldown          time.Duration    // Time to stay in fallback mode before re-probing primary
	ThresholdFailures int64            // Consecutive failures before circuit trips to fallback
}

// FallbackLimiter wraps a primary distributed limiter with an automatic local in-memory fallback.
// If Redis becomes unreachable or times out, traffic smoothly fails over to the local in-memory
// token bucket to prevent cascading gateway outages.
type FallbackLimiter struct {
	cfg FallbackLimiterConfig

	consecutiveErrors atomic.Int64
	circuitOpenUntil  atomic.Int64 // Unix nano timestamp until which circuit is OPEN

	mu            sync.RWMutex
	fallbackCount atomic.Uint64
}

// NewFallbackLimiter instantiates a resilient fallback-enabled rate limiter.
func NewFallbackLimiter(cfg FallbackLimiterConfig) *FallbackLimiter {
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 5 * time.Second
	}
	if cfg.ThresholdFailures <= 0 {
		cfg.ThresholdFailures = 2
	}

	return &FallbackLimiter{
		cfg: cfg,
	}
}

// Allow executes rate limit evaluation with automatic fallback protection.
func (f *FallbackLimiter) Allow(ctx context.Context, key string) (*Result, error) {
	return f.AllowRule(ctx, key, 0, 0, 0)
}

// AllowRule executes rate limit evaluation with dynamic parameters and fallback protection.
func (f *FallbackLimiter) AllowRule(ctx context.Context, key string, capacity int64, window time.Duration, rate float64) (*Result, error) {
	nowNano := time.Now().UnixNano()
	openUntil := f.circuitOpenUntil.Load()

	// 1. If circuit is OPEN (cooling down after Redis outage), bypass primary to avoid latency spikes
	if nowNano < openUntil {
		return f.execFallback(ctx, key, capacity, window, rate, nil)
	}

	// 2. Attempt evaluation on Primary (Distributed Redis)
	var res *Result
	var err error
	if dyn, ok := f.cfg.Primary.(DynamicLimiter); ok && capacity > 0 {
		res, err = dyn.AllowRule(ctx, key, capacity, window, rate)
	} else {
		res, err = f.cfg.Primary.Allow(ctx, key)
	}

	if err == nil {
		// Successful primary call: reset consecutive error count
		f.consecutiveErrors.Store(0)
		return res, nil
	}

	// 3. Primary failed: increment failure count and evaluate circuit trip
	failCount := f.consecutiveErrors.Add(1)
	if failCount >= f.cfg.ThresholdFailures {
		f.circuitOpenUntil.Store(time.Now().Add(f.cfg.Cooldown).UnixNano())
	}

	// 4. Failover to local in-memory fallback
	return f.execFallback(ctx, key, capacity, window, rate, err)
}

func (f *FallbackLimiter) execFallback(ctx context.Context, key string, capacity int64, window time.Duration, rate float64, primaryErr error) (*Result, error) {
	f.fallbackCount.Add(1)

	if f.cfg.Fallback != nil {
		var res *Result
		var err error
		if dyn, ok := f.cfg.Fallback.(DynamicLimiter); ok && capacity > 0 {
			res, err = dyn.AllowRule(ctx, key, capacity, window, rate)
		} else {
			res, err = f.cfg.Fallback.Allow(ctx, key)
		}
		if err == nil {
			return res, nil
		}
	}

	// 5. Catastrophic case: both primary and fallback failed
	if f.cfg.Strategy == FailOpen {
		return &Result{
			Allowed:    true,
			Limit:      0,
			Remaining:  0,
			ResetTime:  time.Now(),
			RetryAfter: 0,
		}, nil
	}

	// FailClosed
	return &Result{
		Allowed:    false,
		Limit:      0,
		Remaining:  0,
		ResetTime:  time.Now().Add(f.cfg.Cooldown),
		RetryAfter: f.cfg.Cooldown,
	}, primaryErr
}

// FallbackInvocations returns how many times fallback was triggered.
func (f *FallbackLimiter) FallbackInvocations() uint64 {
	return f.fallbackCount.Load()
}

// Close releases resources held by both limiters.
func (f *FallbackLimiter) Close() error {
	var err1, err2 error
	if f.cfg.Primary != nil {
		err1 = f.cfg.Primary.Close()
	}
	if f.cfg.Fallback != nil {
		err2 = f.cfg.Fallback.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}
