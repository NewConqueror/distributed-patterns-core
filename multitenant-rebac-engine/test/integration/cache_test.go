package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/database"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/cache"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/engine"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/model"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/repository"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/service"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestRedis connects to Redis for integration tests, skipping gracefully if unavailable.
func setupTestRedis(t *testing.T) *redis.Client {
	if os.Getenv("CI") != "" && os.Getenv("REDIS_HOST") == "" {
		t.Skip("Skipping Redis tests in CI environment without Redis")
	}

	cfg := cache.DefaultRedisConfig()
	client, err := cache.NewRedisClient(context.Background(), cfg)
	if err != nil {
		t.Skipf("Skipping integration test: Redis is not reachable at %s:%d (%v)", cfg.Host, cfg.Port, err)
	}
	return client
}

func TestMultiTierCachingAndInvalidation(t *testing.T) {
	pool := setupTestPool(t)
	defer pool.Close()

	redisClient := setupTestRedis(t)
	defer redisClient.Close()

	ctx := context.Background()
	tenantID := createTestTenant(t, pool, "Caching-Test-Tenant")

	// 1. Initialize Cache Components
	l1Cache, err := cache.NewL1Cache(1000)
	require.NoError(t, err)

	l2Cache := cache.NewL2Cache(redisClient)
	invalidationBus := cache.NewInvalidationBus(redisClient, l1Cache, l2Cache)
	invalidationBus.StartSubscriber(ctx)

	multiTier := cache.NewMultiTierCache(l1Cache, l2Cache)
	multiTier.SetTTLs(5*time.Minute, 10*time.Second)

	tupleRepo := repository.NewTupleRepository()
	rawEngine := engine.NewSQLCheckEngine()
	cachedEngine := cache.NewCachedCheckEngine(rawEngine, multiTier)

	tupleService := service.NewTupleService(pool, tupleRepo, invalidationBus)

	// 2. Seed initial relation: Alice is viewer of doc-100
	aliceTuple := &model.Tuple{
		TenantID:    tenantID,
		EntityType:  "document",
		EntityID:    "doc-100",
		Relation:    "viewer",
		SubjectType: "user",
		SubjectID:   "alice",
	}

	err = tupleService.WriteTuple(ctx, aliceTuple)
	require.NoError(t, err)

	checkReq := engine.CheckRequest{
		TenantID:    tenantID,
		EntityType:  "document",
		EntityID:    "doc-100",
		Relation:    "viewer",
		SubjectType: "user",
		SubjectID:   "alice",
	}

	// =========================================================================
	// Test 1: Cold Check (Cache Miss -> DB Traversal -> Cache Fill)
	// =========================================================================
	var allowed bool
	startCold := time.Now()
	err = database.WithinTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		var checkErr error
		allowed, checkErr = cachedEngine.Check(ctx, tx, checkReq)
		return checkErr
	})
	coldDuration := time.Since(startCold)
	require.NoError(t, err)
	assert.True(t, allowed)
	t.Logf("Cold Check Duration (Miss -> DB -> L2 -> L1): %v", coldDuration)

	// =========================================================================
	// Test 2: Warm Check (L1 In-Memory Hit: sub-microsecond latency)
	// =========================================================================
	startWarmL1 := time.Now()
	err = database.WithinTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		var checkErr error
		allowed, checkErr = cachedEngine.Check(ctx, tx, checkReq)
		return checkErr
	})
	warmL1Duration := time.Since(startWarmL1)
	require.NoError(t, err)
	assert.True(t, allowed)
	t.Logf("L1 Cache Hit Duration: %v", warmL1Duration)
	assert.True(t, warmL1Duration < coldDuration, "L1 cache hit should be significantly faster than cold DB check")

	// =========================================================================
	// Test 3: L2 Redis Hit (Simulate L1 flush, verify L2 hits and backfills L1)
	// =========================================================================
	l1Cache.Purge() // Wipe local in-memory cache to force L2 query
	assert.Equal(t, 0, l1Cache.Len())

	startWarmL2 := time.Now()
	err = database.WithinTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		var checkErr error
		allowed, checkErr = cachedEngine.Check(ctx, tx, checkReq)
		return checkErr
	})
	warmL2Duration := time.Since(startWarmL2)
	require.NoError(t, err)
	assert.True(t, allowed)
	t.Logf("L2 Redis Hit Duration: %v", warmL2Duration)
	// Verify L1 was backfilled from L2
	assert.Greater(t, l1Cache.Len(), 0, "L1 cache should be automatically backfilled from L2")

	// =========================================================================
	// Test 4: O(1) Ephemeral Snapshot Invalidation on Tuple Delete
	// =========================================================================
	// Delete Alice's tuple -> TupleService automatically calls InvalidateTenant
	err = tupleService.DeleteTuple(ctx, aliceTuple)
	require.NoError(t, err)

	// Wait briefly for Pub/Sub propagation across Goroutines
	time.Sleep(50 * time.Millisecond)

	// After invalidation, the cache key version is incremented.
	// Check MUST bypass old cache and re-evaluate DB, returning FALSE!
	err = database.WithinTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		var checkErr error
		allowed, checkErr = cachedEngine.Check(ctx, tx, checkReq)
		return checkErr
	})
	require.NoError(t, err)
	assert.False(t, allowed, "After tuple deletion and invalidation, check MUST return false")

	// =========================================================================
	// Test 5: Negative Caching Verification
	// =========================================================================
	// The negative result (allowed = false) is now cached with NegativeTTL
	startNegCache := time.Now()
	err = database.WithinTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		var checkErr error
		allowed, checkErr = cachedEngine.Check(ctx, tx, checkReq)
		return checkErr
	})
	negCacheDuration := time.Since(startNegCache)
	require.NoError(t, err)
	assert.False(t, allowed)
	t.Logf("Negative Cache Hit Duration: %v", negCacheDuration)
}
