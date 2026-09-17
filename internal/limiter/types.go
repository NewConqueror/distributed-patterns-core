package limiter

import (
	"context"
	"time"
)

// Result encapsulates the evaluation outcome of a rate limiting check.
type Result struct {
	Allowed    bool          // True if the request is permitted
	Limit      int64         // Configured limit/capacity
	Remaining  int64         // Remaining quota available
	ResetTime  time.Time     // Unix timestamp or time when the quota resets/replenishes
	RetryAfter time.Duration // Required wait duration when Allowed is false
}

// Limiter defines the universal contract for all rate limiter implementations
// (In-Memory, Distributed Redis, Fallback).
type Limiter interface {
	// Allow evaluates whether an incoming request with the specified key is allowed.
	Allow(ctx context.Context, key string) (*Result, error)
	// Close gracefully terminates background resources (e.g. cleanup workers, connections).
	Close() error
}

// MemoryConfig defines the operational parameters for an In-Memory rate limiter.
type MemoryConfig struct {
	Rate            float64       // Replenishment rate (tokens per second)
	Capacity        int64         // Maximum burst capacity
	TTL             time.Duration // Time-to-live for inactive client keys
	CleanupInterval time.Duration // Periodic interval for the garbage collection worker
	NumShards       int           // Number of shards for fine-grained striped locking (power of 2)
}

// DefaultMemoryConfig provides battle-tested production defaults.
func DefaultMemoryConfig() MemoryConfig {
	return MemoryConfig{
		Rate:            10.0,             // 10 requests per second
		Capacity:        20,               // Burst up to 20
		TTL:             3 * time.Minute,  // Purge keys idle for 3 minutes
		CleanupInterval: 30 * time.Second, // Run cleanup every 30s
		NumShards:       64,               // 64 shards reduces mutex contention by ~98%
	}
}
