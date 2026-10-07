package limiter

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed sliding_window.lua
var slidingWindowLua string

// RedisConfig defines settings for distributed sliding window rate limiting.
type RedisConfig struct {
	Client    redis.UniversalClient
	Limit     int64
	Window    time.Duration
	KeyPrefix string
}

// RedisSlidingWindowLimiter implements the Limiter interface using an atomic Redis Lua script.
type RedisSlidingWindowLimiter struct {
	client redis.UniversalClient
	cfg    RedisConfig
	script *redis.Script
}

// NewRedisSlidingWindowLimiter constructs a distributed sliding window rate limiter.
func NewRedisSlidingWindowLimiter(cfg RedisConfig) *RedisSlidingWindowLimiter {
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "ratelimit"
	}
	if cfg.Window <= 0 {
		cfg.Window = time.Minute
	}
	if cfg.Limit <= 0 {
		cfg.Limit = 100
	}

	return &RedisSlidingWindowLimiter{
		client: cfg.Client,
		cfg:    cfg,
		script: redis.NewScript(slidingWindowLua),
	}
}

// Allow executes the sliding window evaluation in Redis using default configuration limits.
func (r *RedisSlidingWindowLimiter) Allow(ctx context.Context, key string) (*Result, error) {
	return r.AllowRule(ctx, key, r.cfg.Limit, r.cfg.Window, 0)
}

// AllowRule executes the sliding window evaluation in Redis with dynamic per-rule capacity and window duration.
func (r *RedisSlidingWindowLimiter) AllowRule(ctx context.Context, key string, limit int64, window time.Duration, _ float64) (*Result, error) {
	if limit <= 0 {
		limit = r.cfg.Limit
	}
	if window <= 0 {
		window = r.cfg.Window
	}

	// Use Redis hash tag {...} around key to ensure compatibility with Redis Cluster
	redisKey := fmt.Sprintf("%s:{%s}", r.cfg.KeyPrefix, key)

	nowMs := time.Now().UnixMilli()
	windowMs := window.Milliseconds()
	if windowMs < 1 {
		windowMs = 1
	}

	rawRes, err := r.script.Run(ctx, r.client, []string{redisKey}, limit, windowMs, nowMs, 1).Slice()
	if err != nil {
		return nil, fmt.Errorf("redis sliding window script execution failed: %w", err)
	}

	if len(rawRes) < 4 {
		return nil, fmt.Errorf("unexpected script result format: %v", rawRes)
	}

	allowedVal, _ := rawRes[0].(int64)
	remainingVal, _ := rawRes[1].(int64)
	resetMsVal, _ := rawRes[2].(int64)
	retryAfterMsVal, _ := rawRes[3].(int64)

	resetTime := time.UnixMilli(resetMsVal)
	retryAfter := time.Duration(retryAfterMsVal) * time.Millisecond

	return &Result{
		Allowed:    allowedVal == 1,
		Limit:      limit,
		Remaining:  remainingVal,
		ResetTime:  resetTime,
		RetryAfter: retryAfter,
	}, nil
}

// Close closes the underlying Redis client connection.
func (r *RedisSlidingWindowLimiter) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}
