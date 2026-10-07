package cache

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// MultiTierCache orchestrates multi-tier caching (L1 In-Memory -> L2 Redis).
type MultiTierCache struct {
	l1          *L1Cache
	l2          *L2Cache
	positiveTTL time.Duration
	negativeTTL time.Duration
}

// NewMultiTierCache initializes the multi-tier caching layer.
func NewMultiTierCache(l1 *L1Cache, l2 *L2Cache) *MultiTierCache {
	return &MultiTierCache{
		l1:          l1,
		l2:          l2,
		positiveTTL: DefaultPositiveTTL,
		negativeTTL: DefaultNegativeTTL,
	}
}

// SetTTLs configures custom TTLs for positive and negative cache entries.
func (c *MultiTierCache) SetTTLs(positiveTTL, negativeTTL time.Duration) {
	if positiveTTL > 0 {
		c.positiveTTL = positiveTTL
	}
	if negativeTTL > 0 {
		c.negativeTTL = negativeTTL
	}
}

// Get checks L1, then L2, returning (allowed, found, error).
func (c *MultiTierCache) Get(
	ctx context.Context,
	tenantID uuid.UUID,
	subject, permission, object string,
) (bool, bool, error) {
	// 1. Retrieve current tenant version for cache snapshot consistency
	version := int64(1)
	if c.l2 != nil {
		v, err := c.l2.GetTenantVersion(ctx, tenantID)
		if err == nil && v > 0 {
			version = v
		}
	}

	l1Key := BuildL1Key(tenantID, version, subject, permission, object)

	// 2. Query L1 (Process In-Memory LRU) - ~100ns latency
	if c.l1 != nil {
		if allowed, found := c.l1.Get(l1Key); found {
			return allowed, true, nil
		}
	}

	// 3. Query L2 (Distributed Redis) - ~1-2ms latency
	if c.l2 != nil {
		redisKey := BuildRedisKey(tenantID, version, subject, permission, object)
		allowed, found, err := c.l2.Get(ctx, redisKey)
		if err != nil {
			// On Redis error, gracefully fall through to DB
			return false, false, nil
		}
		if found {
			// Backfill L1 cache
			if c.l1 != nil {
				ttl := c.positiveTTL
				if !allowed {
					ttl = c.negativeTTL
				}
				c.l1.Set(l1Key, allowed, version, ttl)
			}
			return allowed, true, nil
		}
	}

	// Cache miss across all tiers
	return false, false, nil
}

// Set writes the check result into both L2 Redis and L1 In-Memory with respective TTLs.
func (c *MultiTierCache) Set(
	ctx context.Context,
	tenantID uuid.UUID,
	subject, permission, object string,
	allowed bool,
) error {
	version := int64(1)
	if c.l2 != nil {
		v, err := c.l2.GetTenantVersion(ctx, tenantID)
		if err == nil && v > 0 {
			version = v
		}
	}

	ttl := c.positiveTTL
	if !allowed {
		ttl = c.negativeTTL
	}

	// 1. Write to L2 Redis with hash-tag
	if c.l2 != nil {
		redisKey := BuildRedisKey(tenantID, version, subject, permission, object)
		_ = c.l2.Set(ctx, redisKey, allowed, ttl)
	}

	// 2. Write to L1 In-Memory LRU
	if c.l1 != nil {
		l1Key := BuildL1Key(tenantID, version, subject, permission, object)
		c.l1.Set(l1Key, allowed, version, ttl)
	}

	return nil
}
