package limiter

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
)

type tokenBucket struct {
	capacity   int64
	tokens     float64
	lastRefill time.Time
	lastSeen   time.Time
}

type memoryShard struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
}

// MemoryLimiter implements Limiter using an in-memory sharded Token Bucket algorithm.
// It avoids global mutex contention by striping keys across independent shards.
// Expired inactive keys are automatically reclaimed by a background cleanup worker.
type MemoryLimiter struct {
	cfg     MemoryConfig
	shards  []*memoryShard
	mask    uint64
	stopCh  chan struct{}
	closeWg sync.WaitGroup
}

// NewMemoryLimiter instantiates a high-performance in-memory rate limiter.
func NewMemoryLimiter(cfg MemoryConfig) *MemoryLimiter {
	if cfg.NumShards <= 0 {
		cfg.NumShards = 64
	}
	// Round up NumShards to the nearest power of 2 for fast bitwise masking
	powerOf2 := 1
	for powerOf2 < cfg.NumShards {
		powerOf2 <<= 1
	}
	cfg.NumShards = powerOf2

	shards := make([]*memoryShard, cfg.NumShards)
	for i := 0; i < cfg.NumShards; i++ {
		shards[i] = &memoryShard{
			buckets: make(map[string]*tokenBucket),
		}
	}

	ml := &MemoryLimiter{
		cfg:    cfg,
		shards: shards,
		mask:   uint64(cfg.NumShards - 1),
		stopCh: make(chan struct{}),
	}

	if cfg.CleanupInterval > 0 && cfg.TTL > 0 {
		ml.closeWg.Add(1)
		go ml.startCleanupWorker()
	}

	return ml
}

func (m *MemoryLimiter) getShard(key string) *memoryShard {
	h := xxhash.Sum64String(key)
	return m.shards[h&m.mask]
}

// Allow executes the Token Bucket rate limit check using the default configured limits.
func (m *MemoryLimiter) Allow(ctx context.Context, key string) (*Result, error) {
	return m.AllowRule(ctx, key, m.cfg.Capacity, 0, m.cfg.Rate)
}

// AllowRule executes the Token Bucket rate limit check with custom per-rule capacity and rate.
func (m *MemoryLimiter) AllowRule(ctx context.Context, key string, capacity int64, _ time.Duration, rate float64) (*Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if capacity <= 0 {
		capacity = m.cfg.Capacity
	}
	if rate <= 0 {
		rate = m.cfg.Rate
	}

	shard := m.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	now := time.Now()
	b, exists := shard.buckets[key]
	if !exists {
		b = &tokenBucket{
			capacity:   capacity,
			tokens:     float64(capacity),
			lastRefill: now,
			lastSeen:   now,
		}
		shard.buckets[key] = b
	} else if b.capacity != capacity {
		// Handle dynamic rule hot-reload capacity change
		if capacity > b.capacity {
			b.tokens += float64(capacity - b.capacity)
		} else if b.tokens > float64(capacity) {
			b.tokens = float64(capacity)
		}
		b.capacity = capacity
	}

	// Calculate token replenishment
	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed > 0 {
		b.tokens = math.Min(float64(capacity), b.tokens+(elapsed*rate))
		b.lastRefill = now
	}
	b.lastSeen = now

	// Check if at least 1 token is available
	if b.tokens >= 1.0 {
		b.tokens -= 1.0

		// Estimate when bucket reaches full capacity
		var resetTime time.Time
		if rate > 0 {
			remainingToFull := float64(capacity) - b.tokens
			resetTime = now.Add(time.Duration((remainingToFull / rate) * float64(time.Second)))
		} else {
			resetTime = now
		}

		return &Result{
			Allowed:    true,
			Limit:      capacity,
			Remaining:  int64(b.tokens),
			ResetTime:  resetTime,
			RetryAfter: 0,
		}, nil
	}

	// Reject request: compute Retry-After until 1 token is generated
	var retryAfter time.Duration
	if rate > 0 {
		needed := 1.0 - b.tokens
		retryAfter = time.Duration((needed / rate) * float64(time.Second))
		if retryAfter <= 0 {
			retryAfter = time.Millisecond
		}
	}

	return &Result{
		Allowed:    false,
		Limit:      capacity,
		Remaining:  0,
		ResetTime:  now.Add(retryAfter),
		RetryAfter: retryAfter,
	}, nil
}

// startCleanupWorker sweeps expired buckets across shards to reclaim memory.
func (m *MemoryLimiter) startCleanupWorker() {
	defer m.closeWg.Done()

	ticker := time.NewTicker(m.cfg.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.cleanupExpired()
		}
	}
}

func (m *MemoryLimiter) cleanupExpired() {
	now := time.Now()
	ttl := m.cfg.TTL

	for _, shard := range m.shards {
		shard.mu.Lock()
		for key, b := range shard.buckets {
			if now.Sub(b.lastSeen) > ttl {
				delete(shard.buckets, key)
			}
		}
		shard.mu.Unlock()
	}
}

// KeyCount returns the total number of active keys stored across all shards.
func (m *MemoryLimiter) KeyCount() int {
	total := 0
	for _, shard := range m.shards {
		shard.mu.Lock()
		total += len(shard.buckets)
		shard.mu.Unlock()
	}
	return total
}

// Close terminates the background cleanup routine and waits for completion.
func (m *MemoryLimiter) Close() error {
	select {
	case <-m.stopCh:
		// Already closed
		return nil
	default:
		close(m.stopCh)
	}
	m.closeWg.Wait()
	return nil
}
