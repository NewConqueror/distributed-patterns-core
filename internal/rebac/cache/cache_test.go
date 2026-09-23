package cache

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashTagKeyFormat(t *testing.T) {
	tenantID := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	version := int64(3)

	redisKey := BuildRedisKey(tenantID, version, "user:123", "viewer", "document:doc-456")

	// 1. Must start with {tenant_id} for Redis Cluster hash tagging
	expectedTag := "{" + tenantID.String() + "}"
	assert.True(t, strings.HasPrefix(redisKey, expectedTag), "Key must start with {tenant_id} hash-tag")

	// 2. Must contain version
	assert.Contains(t, redisKey, ":v3:rebac:check:")

	// 3. Different inputs must yield different SHA-256 hashes
	redisKey2 := BuildRedisKey(tenantID, version, "user:999", "viewer", "document:doc-456")
	assert.NotEqual(t, redisKey, redisKey2)

	// 4. Different versions must yield different keys (O(1) Zookies invalidation guarantee)
	redisKeyNextVer := BuildRedisKey(tenantID, version+1, "user:123", "viewer", "document:doc-456")
	assert.NotEqual(t, redisKey, redisKeyNextVer)
}

func TestL1CacheOperations(t *testing.T) {
	cache, err := NewL1Cache(100)
	require.NoError(t, err)

	key := "test-key"

	// 1. Miss initially
	_, found := cache.Get(key)
	assert.False(t, found)

	// 2. Set with positive decision
	cache.Set(key, true, 1, 1*time.Hour)
	allowed, found := cache.Get(key)
	assert.True(t, found)
	assert.True(t, allowed)

	// 3. Set with negative decision
	negKey := "neg-key"
	cache.Set(negKey, false, 1, 1*time.Hour)
	allowed, found = cache.Get(negKey)
	assert.True(t, found)
	assert.False(t, allowed)

	// 4. Test TTL expiration
	expKey := "exp-key"
	cache.Set(expKey, true, 1, 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	_, found = cache.Get(expKey)
	assert.False(t, found, "Expired key should not be found")

	// 5. Purge clears everything
	cache.Purge()
	assert.Equal(t, 0, cache.Len())
}

func TestMultiTierCacheWithL1Only(t *testing.T) {
	l1, err := NewL1Cache(100)
	require.NoError(t, err)

	multiTier := NewMultiTierCache(l1, nil)
	tenantID := uuid.New()
	ctx := context.Background()

	// 1. Miss initially
	allowed, hit, err := multiTier.Get(ctx, tenantID, "user:alice", "viewer", "document:doc-1")
	require.NoError(t, err)
	assert.False(t, hit)
	assert.False(t, allowed)

	// 2. Set positive
	err = multiTier.Set(ctx, tenantID, "user:alice", "viewer", "document:doc-1", true)
	require.NoError(t, err)

	// 3. Hit from L1
	allowed, hit, err = multiTier.Get(ctx, tenantID, "user:alice", "viewer", "document:doc-1")
	require.NoError(t, err)
	assert.True(t, hit)
	assert.True(t, allowed)
}
