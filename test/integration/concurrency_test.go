package integration

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/database"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/document"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/cache"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/engine"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRaceCondition_TenantLeak proves that under extreme concurrent load (100 goroutines),
// reusing connections from the shared pool NEVER causes cross-tenant context leakage.
// RLS physically guarantees that no goroutine can observe another tenant's rows.
func TestRaceCondition_TenantLeak(t *testing.T) {
	pool := setupTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	repo := document.NewRepository()

	const tenantCount = 10
	tenants := make([]uuid.UUID, tenantCount)

	// 1. Seed 10 distinct tenants with private documents
	for i := 0; i < tenantCount; i++ {
		tID := createTestTenant(t, pool, fmt.Sprintf("Race-Tenant-%d", i))
		tenants[i] = tID

		doc := &document.Document{
			TenantID: tID,
			Title:    fmt.Sprintf("Secret-Data-Tenant-%d", i),
			Content:  fmt.Sprintf("Top-Secret content strictly isolated to tenant %d", i),
		}

		err := database.WithinTenantTx(ctx, pool, tID, func(tx pgx.Tx) error {
			return repo.Create(ctx, tx, doc)
		})
		require.NoError(t, err)
	}

	t.Logf("Seeded %d tenants with private documents. Commencing 100 concurrent workers...", tenantCount)

	// 2. Launch 100 concurrent workers hitting the connection pool randomly
	const workerCount = 100
	const iterationsPerWorker = 50

	var totalSuccessfulChecks atomic.Int64
	var totalCrossTenantLeaks atomic.Int64

	var wg sync.WaitGroup
	wg.Add(workerCount)

	startBarrier := make(chan struct{})

	for w := 0; w < workerCount; w++ {
		go func(workerID int) {
			defer wg.Done()
			<-startBarrier // Synchronize all 100 workers to start at the exact same millisecond

			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for i := 0; i < iterationsPerWorker; i++ {
				// Pick a random tenant
				chosenIdx := r.Intn(tenantCount)
				expectedTenantID := tenants[chosenIdx]

				err := database.WithinTenantTx(ctx, pool, expectedTenantID, func(tx pgx.Tx) error {
					// 1. Verify session variable is set correctly inside transaction
					var sessionTenant string
					if err := tx.QueryRow(ctx, "SELECT current_setting('app.current_tenant_id', true);").Scan(&sessionTenant); err != nil {
						return err
					}
					if sessionTenant != expectedTenantID.String() {
						totalCrossTenantLeaks.Add(1)
						return fmt.Errorf("LEAK! Expected session tenant %s, got %s", expectedTenantID, sessionTenant)
					}

					// 2. Query documents (List has NO 'WHERE tenant_id = ?' clause!)
					docs, err := repo.List(ctx, tx)
					if err != nil {
						return err
					}

					// 3. Mathematically assert zero cross-tenant documents
					for _, d := range docs {
						if d.TenantID != expectedTenantID {
							totalCrossTenantLeaks.Add(1)
							return fmt.Errorf("CRITICAL SECURITY BREACH! Tenant %s saw document of tenant %s",
								expectedTenantID, d.TenantID)
						}
					}

					totalSuccessfulChecks.Add(1)
					return nil
				})

				if err != nil {
					t.Errorf("Worker %d iteration %d error: %v", workerID, i, err)
					return
				}
			}
		}(w)
	}

	// Release all workers simultaneously
	close(startBarrier)
	wg.Wait()

	t.Logf("Completed %d concurrent queries across %d workers.", totalSuccessfulChecks.Load(), workerCount)
	assert.Equal(t, int64(0), totalCrossTenantLeaks.Load(), "MUST have ZERO cross-tenant data leaks under extreme race conditions!")
	assert.Equal(t, int64(workerCount*iterationsPerWorker), totalSuccessfulChecks.Load())
}

// countingMockEngine counts how many times the underlying database traversal is executed.
type countingMockEngine struct {
	calls atomic.Int64
}

func (m *countingMockEngine) Check(ctx context.Context, tx pgx.Tx, req engine.CheckRequest) (bool, error) {
	m.calls.Add(1)
	// Simulate non-trivial database query latency
	time.Sleep(10 * time.Millisecond)
	return true, nil
}

// TestThunderingHerd_Singleflight proves that singleflight collapses 500 simultaneous
// concurrent requests on a cache miss into EXACTLY 1 database query.
func TestThunderingHerd_Singleflight(t *testing.T) {
	l1Cache, err := cache.NewL1Cache(100)
	require.NoError(t, err)

	multiTier := cache.NewMultiTierCache(l1Cache, nil)
	mockDB := &countingMockEngine{}
	cachedEngine := cache.NewCachedCheckEngine(mockDB, multiTier)

	tenantID := uuid.New()
	req := engine.CheckRequest{
		TenantID:    tenantID,
		EntityType:  "document",
		EntityID:    "thundering-doc-1",
		Relation:    "viewer",
		SubjectType: "user",
		SubjectID:   "alice",
	}

	const concurrentCallers = 500
	var wg sync.WaitGroup
	wg.Add(concurrentCallers)

	startBarrier := make(chan struct{})
	ctx := context.Background()

	var successCount atomic.Int64

	for i := 0; i < concurrentCallers; i++ {
		go func() {
			defer wg.Done()
			<-startBarrier // Synchronize all 500 callers to fire simultaneously

			allowed, err := cachedEngine.Check(ctx, nil, req)
			if err == nil && allowed {
				successCount.Add(1)
			}
		}()
	}

	// Unleash all 500 callers simultaneously
	close(startBarrier)
	wg.Wait()

	assert.Equal(t, int64(concurrentCallers), successCount.Load(), "All 500 concurrent callers must succeed")

	// Singleflight Guarantee: Exactly 1 database query must be executed!
	dbCalls := mockDB.calls.Load()
	t.Logf("Thundering Herd Result: 500 simultaneous requests resulted in %d database query!", dbCalls)
	assert.Equal(t, int64(1), dbCalls, "Singleflight must collapse 500 requests into exactly 1 database call")
}
