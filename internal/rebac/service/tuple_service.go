package service

import (
	"context"
	"fmt"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/database"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/cache"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/model"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/repository"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TupleService handles writes and deletes with transactional safety and automatic cache invalidation.
type TupleService struct {
	pool            *pgxpool.Pool
	repo            repository.TupleRepository
	invalidationBus *cache.InvalidationBus
}

func NewTupleService(
	pool *pgxpool.Pool,
	repo repository.TupleRepository,
	bus *cache.InvalidationBus,
) *TupleService {
	return &TupleService{
		pool:            pool,
		repo:            repo,
		invalidationBus: bus,
	}
}

// WriteTuple commits a relation tuple to PostgreSQL within a tenant transaction
// and immediately triggers cache invalidation across the distributed cluster.
func (s *TupleService) WriteTuple(ctx context.Context, tuple *model.Tuple) error {
	err := database.WithinTenantTx(ctx, s.pool, tuple.TenantID, func(tx pgx.Tx) error {
		return s.repo.WriteTuple(ctx, tx, tuple)
	})
	if err != nil {
		return fmt.Errorf("failed to write tuple in transaction: %w", err)
	}

	// Transaction committed successfully -> Invalidate cache
	if s.invalidationBus != nil {
		affectedEntity := model.FormatEntity(tuple.EntityType, tuple.EntityID)
		_ = s.invalidationBus.InvalidateTenant(ctx, tuple.TenantID, affectedEntity)
	}

	return nil
}

// DeleteTuple removes a relation tuple and invalidates the tenant's cache.
func (s *TupleService) DeleteTuple(ctx context.Context, tuple *model.Tuple) error {
	err := database.WithinTenantTx(ctx, s.pool, tuple.TenantID, func(tx pgx.Tx) error {
		return s.repo.DeleteTuple(ctx, tx, tuple)
	})
	if err != nil {
		return fmt.Errorf("failed to delete tuple in transaction: %w", err)
	}

	// Transaction committed successfully -> Invalidate cache
	if s.invalidationBus != nil {
		affectedEntity := model.FormatEntity(tuple.EntityType, tuple.EntityID)
		_ = s.invalidationBus.InvalidateTenant(ctx, tuple.TenantID, affectedEntity)
	}

	return nil
}
