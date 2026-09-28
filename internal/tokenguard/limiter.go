package tokenguard

import (
	"context"
	"sync"
	"time"
)

// RateLimitResult encapsulates rate limit evaluation metrics.
type RateLimitResult struct {
	Allowed        bool          `json:"allowed"`
	CurrentRPM     int           `json:"current_rpm"`
	RemainingRPM   int           `json:"remaining_rpm"`
	LimitRPM       int           `json:"limit_rpm"`
	CurrentTPM     int           `json:"current_tpm"`
	RemainingTPM   int           `json:"remaining_tpm"`
	LimitTPM       int           `json:"limit_tpm"`
	ResetAfter     time.Duration `json:"reset_after"`
	ViolatedBudget string        `json:"violated_budget,omitempty"` // "rpm" or "tpm"
}

// RateLimiter evaluates RPM and TPM limits for clients.
type RateLimiter interface {
	Allow(ctx context.Context, key string, estimatedTokens int) (*RateLimitResult, error)
}

// LimiterConfig specifies rate limit quotas.
type LimiterConfig struct {
	// MaxRPM is the maximum requests per minute allowed (e.g., 60).
	MaxRPM int
	// MaxTPM is the maximum tokens per minute allowed (e.g., 40000).
	MaxTPM int
	// WindowDuration is the sliding window size (defaults to 1 minute).
	WindowDuration time.Duration
}

type windowEntry struct {
	timestamp time.Time
	tokens    int
}

// MemorySlidingWindowLimiter tracks sliding window token and request usage in memory.
type MemorySlidingWindowLimiter struct {
	mu     sync.Mutex
	cfg    LimiterConfig
	keys   map[string][]windowEntry
	window time.Duration
}

// NewMemorySlidingWindowLimiter creates an in-memory sliding window rate limiter.
func NewMemorySlidingWindowLimiter(cfg LimiterConfig) *MemorySlidingWindowLimiter {
	window := cfg.WindowDuration
	if window <= 0 {
		window = time.Minute
	}
	if cfg.MaxRPM <= 0 {
		cfg.MaxRPM = 60
	}
	if cfg.MaxTPM <= 0 {
		cfg.MaxTPM = 40000
	}

	return &MemorySlidingWindowLimiter{
		cfg:    cfg,
		keys:   make(map[string][]windowEntry),
		window: window,
	}
}

// Allow checks if the request fits within the RPM and TPM limits.
func (l *MemorySlidingWindowLimiter) Allow(ctx context.Context, key string, estimatedTokens int) (*RateLimitResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-l.window)

	// 1. Clean up entries outside sliding window
	entries := l.keys[key]
	valid := entries[:0]
	currentRPM := 0
	currentTPM := 0
	var oldestTimestamp time.Time

	for _, e := range entries {
		if e.timestamp.After(cutoff) {
			valid = append(valid, e)
			currentRPM++
			currentTPM += e.tokens
			if oldestTimestamp.IsZero() || e.timestamp.Before(oldestTimestamp) {
				oldestTimestamp = e.timestamp
			}
		}
	}
	l.keys[key] = valid

	resetAfter := time.Duration(0)
	if !oldestTimestamp.IsZero() {
		// Reset is when the oldest entry leaves the sliding window
		resetAfter = oldestTimestamp.Add(l.window).Sub(now)
		if resetAfter < 0 {
			resetAfter = 0
		}
	} else {
		resetAfter = l.window
	}

	res := &RateLimitResult{
		CurrentRPM:   currentRPM,
		LimitRPM:     l.cfg.MaxRPM,
		RemainingRPM: l.cfg.MaxRPM - currentRPM,
		CurrentTPM:   currentTPM,
		LimitTPM:     l.cfg.MaxTPM,
		RemainingTPM: l.cfg.MaxTPM - currentTPM,
		ResetAfter:   resetAfter,
	}
	if res.RemainingRPM < 0 {
		res.RemainingRPM = 0
	}
	if res.RemainingTPM < 0 {
		res.RemainingTPM = 0
	}

	// 2. Check RPM budget
	if currentRPM+1 > l.cfg.MaxRPM {
		res.Allowed = false
		res.ViolatedBudget = "rpm"
		return res, nil
	}

	// 3. Check TPM budget
	if currentTPM+estimatedTokens > l.cfg.MaxTPM {
		res.Allowed = false
		res.ViolatedBudget = "tpm"
		return res, nil
	}

	// 4. Request approved: record usage
	l.keys[key] = append(valid, windowEntry{
		timestamp: now,
		tokens:    estimatedTokens,
	})

	res.Allowed = true
	res.CurrentRPM++
	res.RemainingRPM--
	res.CurrentTPM += estimatedTokens
	res.RemainingTPM -= estimatedTokens

	return res, nil
}
