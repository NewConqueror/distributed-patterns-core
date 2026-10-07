package cache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// L2Cache manages distributed caching over Redis.
// Typical latency: ~1-2 milliseconds.
type L2Cache struct {
	client *redis.Client
}

// NewL2Cache creates a new L2 distributed Redis cache wrapper.
func NewL2Cache(client *redis.Client) *L2Cache {
	return &L2Cache{client: client}
}

// Get queries Redis for the check decision.
// Returns (allowed, found, error).
func (c *L2Cache) Get(ctx context.Context, key string) (bool, bool, error) {
	val, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, false, nil // Cache miss
		}
		return false, false, fmt.Errorf("redis get error: %w", err)
	}

	allowed, err := strconv.ParseBool(val)
	if err != nil {
		return false, false, fmt.Errorf("malformed cached boolean in redis: %w", err)
	}

	return allowed, true, nil
}

// Set stores the boolean decision in Redis with the given TTL.
// Positive checks receive a long TTL (e.g. 10m), while negative checks receive a short TTL (e.g. 30s).
func (c *L2Cache) Set(ctx context.Context, key string, allowed bool, ttl time.Duration) error {
	val := strconv.FormatBool(allowed)
	if err := c.client.Set(ctx, key, val, ttl).Err(); err != nil {
		return fmt.Errorf("redis set error: %w", err)
	}
	return nil
}

// GetTenantVersion retrieves the current epoch version for the tenant.
// If the key does not exist, it defaults to version 1.
func (c *L2Cache) GetTenantVersion(ctx context.Context, tenantID uuid.UUID) (int64, error) {
	versionKey := BuildTenantVersionKey(tenantID)
	val, err := c.client.Get(ctx, versionKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// Initialize version at 1 if not present
			_ = c.client.SetNX(ctx, versionKey, 1, 0).Err()
			return 1, nil
		}
		return 1, fmt.Errorf("failed to fetch tenant version: %w", err)
	}

	version, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 1, nil
	}
	return version, nil
}

// IncrementTenantVersion atomically increments the tenant version counter.
// This executes the "Tenant Ephemeral Snapshot" (Zookies) invalidation pattern:
// With a single atomic INCR, all existing cached check keys for this tenant
// become instantly invalid at O(1) time complexity.
func (c *L2Cache) IncrementTenantVersion(ctx context.Context, tenantID uuid.UUID) (int64, error) {
	versionKey := BuildTenantVersionKey(tenantID)
	newVersion, err := c.client.Incr(ctx, versionKey).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to increment tenant version: %w", err)
	}
	return newVersion, nil
}
