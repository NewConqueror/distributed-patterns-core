package database

import (
	"context"
	"fmt"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/tenant"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// WithinTenantTx executes the provided callback within a database transaction scoped
// to the given tenantID using PostgreSQL's transaction-local configuration (`is_local = true`).
//
// Critical Architecture Note:
// 1. We NEVER use global "SET app.current_tenant_id = ..." because pgxpool reuses
//    connections across requests. A global SET would cause cross-tenant context leakage.
// 2. PostgreSQL's "SET LOCAL" syntax does NOT support parameterized queries ($1).
// 3. We use "SELECT set_config('app.current_tenant_id', $1, true);" where the third parameter
//    is_local=true ensures the setting is automatically discarded as soon as the transaction
//    ends (either on Commit or Rollback).
func WithinTenantTx(ctx context.Context, pool *pgxpool.Pool, tenantID uuid.UUID, fn func(tx pgx.Tx) error) error {
	if tenantID == uuid.Nil {
		return tenant.ErrInvalidTenantID
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Defer rollback in case of error or panic. If committed, Rollback is a no-op.
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// Scope tenant context strictly to this transaction (equivalent to SET LOCAL)
	_, err = tx.Exec(ctx, "SELECT set_config('app.current_tenant_id', $1, true);", tenantID.String())
	if err != nil {
		return fmt.Errorf("failed to set local tenant context in transaction: %w", err)
	}

	// Execute business logic with the scoped transaction
	if err := fn(tx); err != nil {
		return err
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit tenant transaction: %w", err)
	}

	return nil
}

// WithinTenant automatically extracts the tenant ID from the Go context and delegates
// to WithinTenantTx.
func WithinTenant(ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx) error) error {
	tenantID, err := tenant.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("cannot execute tenant transaction: %w", err)
	}
	return WithinTenantTx(ctx, pool, tenantID, fn)
}
