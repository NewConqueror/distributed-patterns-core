package cache

import (
	"context"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/engine"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/model"

	"github.com/jackc/pgx/v5"
)

// CachedCheckEngine decorates an underlying CheckEngine with MultiTierCache.
type CachedCheckEngine struct {
	underlying engine.CheckEngine
	cache      *MultiTierCache
}

// NewCachedCheckEngine wraps an existing check engine (SQL CTE or In-Memory) with caching.
func NewCachedCheckEngine(underlying engine.CheckEngine, cache *MultiTierCache) engine.CheckEngine {
	return &CachedCheckEngine{
		underlying: underlying,
		cache:      cache,
	}
}

// Check first checks L1, then L2, and only executes the underlying database graph
// traversal on a complete cache miss.
func (e *CachedCheckEngine) Check(ctx context.Context, tx pgx.Tx, req engine.CheckRequest) (bool, error) {
	subject := model.FormatEntity(req.SubjectType, req.SubjectID)
	object := model.FormatEntity(req.EntityType, req.EntityID)
	permission := req.Relation

	// 1. Check Multi-Tier Cache
	if e.cache != nil {
		if allowed, hit, err := e.cache.Get(ctx, req.TenantID, subject, permission, object); err == nil && hit {
			return allowed, nil
		}
	}

	// 2. Cache Miss: Execute graph traversal
	allowed, err := e.underlying.Check(ctx, tx, req)
	if err != nil {
		return false, err
	}

	// 3. Write result to L1 and L2 caches
	if e.cache != nil {
		_ = e.cache.Set(ctx, req.TenantID, subject, permission, object, allowed)
	}

	return allowed, nil
}
