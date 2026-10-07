package cache

import (
	"context"
	"fmt"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/engine"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/model"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/singleflight"
)

// CachedCheckEngine decorates an underlying CheckEngine with MultiTierCache
// and Thundering Herd protection via singleflight.
type CachedCheckEngine struct {
	underlying engine.CheckEngine
	cache      *MultiTierCache
	sfg        singleflight.Group
}

// NewCachedCheckEngine wraps an existing check engine (SQL CTE or In-Memory) with caching and singleflight.
func NewCachedCheckEngine(underlying engine.CheckEngine, cache *MultiTierCache) engine.CheckEngine {
	return &CachedCheckEngine{
		underlying: underlying,
		cache:      cache,
	}
}

// Check first checks L1, then L2. On a cache miss, it collapses concurrent duplicate
// requests into a single database query using singleflight (Thundering Herd protection).
func (e *CachedCheckEngine) Check(ctx context.Context, tx pgx.Tx, req engine.CheckRequest) (bool, error) {
	subject := model.FormatEntity(req.SubjectType, req.SubjectID)
	object := model.FormatEntity(req.EntityType, req.EntityID)
	permission := req.Relation

	// 1. Check Multi-Tier Cache (L1 LRU -> L2 Redis)
	if e.cache != nil {
		if allowed, hit, err := e.cache.Get(ctx, req.TenantID, subject, permission, object); err == nil && hit {
			return allowed, nil
		}
	}

	// 2. Thundering Herd Protection:
	// If 500 concurrent goroutines query this exact permission on a cache miss,
	// singleflight collapses them into exactly 1 underlying database call.
	flightKey := fmt.Sprintf("%s:%s#%s@%s", req.TenantID, object, permission, subject)
	result, err, _ := e.sfg.Do(flightKey, func() (any, error) {
		// Double check cache in case a preceding singleflight call just populated it
		if e.cache != nil {
			if allowed, hit, cacheErr := e.cache.Get(ctx, req.TenantID, subject, permission, object); cacheErr == nil && hit {
				return allowed, nil
			}
		}

		// Execute graph traversal in database
		allowed, dbErr := e.underlying.Check(ctx, tx, req)
		if dbErr != nil {
			return false, dbErr
		}

		// Backfill L1 and L2 caches
		if e.cache != nil {
			_ = e.cache.Set(ctx, req.TenantID, subject, permission, object, allowed)
		}

		return allowed, nil
	})

	if err != nil {
		return false, err
	}

	return result.(bool), nil
}
