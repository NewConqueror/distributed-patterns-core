package tokenguard

import (
	"context"
	"fmt"
	"time"
)

// RedisClient defines the minimal interface needed for Redis rate limiting.
type RedisClient interface {
	Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)
}

// RedisSlidingWindowLimiter implements RateLimiter using Redis and Lua script for atomic sliding windows.
type RedisSlidingWindowLimiter struct {
	client RedisClient
	cfg    LimiterConfig
}

// NewRedisSlidingWindowLimiter creates a new RedisSlidingWindowLimiter.
func NewRedisSlidingWindowLimiter(client RedisClient, cfg LimiterConfig) *RedisSlidingWindowLimiter {
	if cfg.MaxRPM <= 0 {
		cfg.MaxRPM = 60
	}
	if cfg.MaxTPM <= 0 {
		cfg.MaxTPM = 40000
	}
	if cfg.WindowDuration <= 0 {
		cfg.WindowDuration = time.Minute
	}

	return &RedisSlidingWindowLimiter{
		client: client,
		cfg:    cfg,
	}
}

// Lua script for atomic sliding window check across RPM and TPM
const slidingWindowLuaScript = `
local keyRPM = KEYS[1]
local keyTPM = KEYS[2]

local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local maxRPM = tonumber(ARGV[3])
local maxTPM = tonumber(ARGV[4])
local tokens = tonumber(ARGV[5])

local clearBefore = now - window

-- 1. Remove expired entries
redis.call('ZREMRANGEBYSCORE', keyRPM, '-inf', clearBefore)
redis.call('ZREMRANGEBYSCORE', keyTPM, '-inf', clearBefore)

-- 2. Count current RPM
local currentRPM = redis.call('ZCARD', keyRPM)
if currentRPM + 1 > maxRPM then
    return {0, "rpm", currentRPM, 0}
end

-- 3. Calculate current TPM by summing scores/members
local tpmEntries = redis.call('ZRANGE', keyTPM, 0, -1, 'WITHSCORES')
local currentTPM = 0
for i = 1, #tpmEntries, 2 do
    local member = tpmEntries[i]
    -- member format: "<uuid>:<tokens>"
    local colonIdx = string.find(member, ":")
    if colonIdx then
        local tCount = tonumber(string.sub(member, colonIdx + 1))
        if tCount then
            currentTPM = currentTPM + tCount
        end
    end
end

if currentTPM + tokens > maxTPM then
    return {0, "tpm", currentRPM, currentTPM}
end

-- 4. Approved: Record this request
local reqId = tostring(now) .. ":" .. tostring(math.random(1000, 9999))
redis.call('ZADD', keyRPM, now, reqId)
redis.call('ZADD', keyTPM, now, reqId .. ":" .. tostring(tokens))

-- Expire keys slightly after window
redis.call('EXPIRE', keyRPM, math.ceil(window / 1000) + 10)
redis.call('EXPIRE', keyTPM, math.ceil(window / 1000) + 10)

return {1, "ok", currentRPM + 1, currentTPM + tokens}
`

// Allow atomically checks and records request and token usage in Redis.
func (r *RedisSlidingWindowLimiter) Allow(ctx context.Context, key string, estimatedTokens int) (*RateLimitResult, error) {
	if r.client == nil {
		return &RateLimitResult{Allowed: true}, nil
	}

	keyRPM := fmt.Sprintf("ratelimit:%s:rpm", key)
	keyTPM := fmt.Sprintf("ratelimit:%s:tpm", key)

	nowMs := time.Now().UnixMilli()
	windowMs := r.cfg.WindowDuration.Milliseconds()

	res, err := r.client.Eval(ctx, slidingWindowLuaScript, []string{keyRPM, keyTPM},
		nowMs,
		windowMs,
		r.cfg.MaxRPM,
		r.cfg.MaxTPM,
		estimatedTokens,
	)
	if err != nil {
		return nil, fmt.Errorf("redis rate limiter eval failed: %w", err)
	}

	resSlice, ok := res.([]any)
	if !ok || len(resSlice) < 4 {
		return nil, fmt.Errorf("unexpected lua script response format: %v", res)
	}

	allowed := resSlice[0].(int64) == 1
	statusStr := fmt.Sprintf("%v", resSlice[1])
	currentRPM := int(resSlice[2].(int64))
	currentTPM := int(resSlice[3].(int64))

	remainingRPM := r.cfg.MaxRPM - currentRPM
	if remainingRPM < 0 {
		remainingRPM = 0
	}
	remainingTPM := r.cfg.MaxTPM - currentTPM
	if remainingTPM < 0 {
		remainingTPM = 0
	}

	result := &RateLimitResult{
		Allowed:      allowed,
		CurrentRPM:   currentRPM,
		RemainingRPM: remainingRPM,
		LimitRPM:     r.cfg.MaxRPM,
		CurrentTPM:   currentTPM,
		RemainingTPM: remainingTPM,
		LimitTPM:     r.cfg.MaxTPM,
		ResetAfter:   r.cfg.WindowDuration,
	}

	if !allowed {
		result.ViolatedBudget = statusStr
	}

	return result, nil
}
