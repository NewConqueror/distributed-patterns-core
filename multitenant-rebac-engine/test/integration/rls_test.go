package integration

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/database"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/document"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/tenant"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestPool creates a test connection pool using app_user credentials.
// It is critical to test using a non-superuser role (like app_user) because PostgreSQL
// superusers bypass RLS by design.
func setupTestPool(t *testing.T) *pgxpool.Pool {
	cfg := database.DefaultConfig()

	if host := os.Getenv("DB_HOST"); host != "" {
		cfg.Host = host
	}
	if portStr := os.Getenv("DB_PORT"); portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil {
			cfg.Port = p
		}
	}
	if user := os.Getenv("DB_USER"); user != "" {
		cfg.User = user
	}
	if pass := os.Getenv("DB_PASSWORD"); pass != "" {
		cfg.Password = pass
	}
	if db := os.Getenv("DB_NAME"); db != "" {
		cfg.Database = db
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := database.NewPool(ctx, cfg)
	if err != nil {
		t.Skipf("Skipping integration test: PostgreSQL is not reachable at %s:%d (%v)", cfg.Host, cfg.Port, err)
	}

	return pool
}

// createTestTenant creates a tenant entry in the tenants table.
func createTestTenant(t *testing.T, pool *pgxpool.Pool, name string) uuid.UUID {
	ctx := context.Background()
	var tenantID uuid.UUID
	err := pool.QueryRow(ctx, "INSERT INTO tenants (name) VALUES ($1) RETURNING id", name).Scan(&tenantID)
	require.NoError(t, err, "Failed to create test tenant")
	return tenantID
}

func TestContextLeakageAndRLS(t *testing.T) {
	pool := setupTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	repo := document.NewRepository()

	// 1. Setup two isolated tenants
	tenantA := createTestTenant(t, pool, "Tenant-Alpha")
	tenantB := createTestTenant(t, pool, "Tenant-Beta")

	t.Logf("Created Tenant A: %s", tenantA)
	t.Logf("Created Tenant B: %s", tenantB)

	// =========================================================================
	// Scenario 1: Tenant A creates doc-1. Verify doc-1 is persisted.
	// =========================================================================
	docA := &document.Document{
		TenantID: tenantA,
		Title:    "Confidential Alpha Plan",
		Content:  "Secret data of Tenant Alpha",
	}

	err := database.WithinTenantTx(ctx, pool, tenantA, func(tx pgx.Tx) error {
		return repo.Create(ctx, tx, docA)
	})
	require.NoError(t, err, "Tenant A should be able to create their document")
	assert.NotEqual(t, uuid.Nil, docA.ID)

	// =========================================================================
	// Scenario 2: Tenant B queries documents from the exact same connection pool.
	// RLS must filter it out; the count MUST be 0.
	// =========================================================================
	var docsTenantB []document.Document
	err = database.WithinTenantTx(ctx, pool, tenantB, func(tx pgx.Tx) error {
		var listErr error
		// Note: List() executes "SELECT ... FROM documents" without any manual WHERE tenant_id = ?
		docsTenantB, listErr = repo.List(ctx, tx)
		return listErr
	})
	require.NoError(t, err)
	assert.Empty(t, docsTenantB, "Tenant B MUST NOT see Tenant A's documents (Expected 0 rows)")

	// Also verify that Tenant B cannot access docA by its direct ID
	err = database.WithinTenantTx(ctx, pool, tenantB, func(tx pgx.Tx) error {
		_, getErr := repo.GetByID(ctx, tx, docA.ID)
		return getErr
	})
	assert.ErrorIs(t, err, document.ErrDocumentNotFound, "Tenant B MUST receive not found when querying Tenant A's document ID")

	// =========================================================================
	// Scenario 3: Tenant B tries to forge an INSERT with Tenant A's tenant_id.
	// The RLS WITH CHECK policy MUST abort the transaction with check_violation (SQLSTATE 44000).
	// =========================================================================
	forgedDoc := &document.Document{
		TenantID: tenantA, // Maliciously forged tenant ID
		Title:    "Poisoned Doc by Tenant B",
		Content:  "Attempting cross-tenant data injection",
	}

	err = database.WithinTenantTx(ctx, pool, tenantB, func(tx pgx.Tx) error {
		return repo.Create(ctx, tx, forgedDoc)
	})
	require.Error(t, err, "Inserting document with mismatched tenant ID MUST be rejected by RLS")

	// Validate that the error code is PostgreSQL's check_violation (44000)
	var pgErr *pgconn.PgError
	if assert.ErrorAs(t, err, &pgErr) {
		assert.Equal(t, "44000", pgErr.Code, "Expected PostgreSQL check_violation (44000) error code")
		t.Logf("Successfully captured RLS WITH CHECK violation: Code=%s, Message=%s", pgErr.Code, pgErr.Message)
	}

	// =========================================================================
	// Scenario 4: Connection Pool Sanitation (Context Leakage Check)
	// After WithinTenantTx finishes, verify that the session setting is NOT leaked
	// to subsequent operations on a raw connection from the pool.
	// =========================================================================
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()

	var currentTenantSetting string
	err = conn.QueryRow(ctx, "SELECT current_setting('app.current_tenant_id', true);").Scan(&currentTenantSetting)
	require.NoError(t, err)
	assert.Equal(t, "", currentTenantSetting, "Connection pool connection MUST NOT leak app.current_tenant_id outside transaction!")
}

func TestContextHelpers(t *testing.T) {
	ctx := context.Background()

	// 1. Missing context
	_, err := tenant.FromContext(ctx)
	assert.ErrorIs(t, err, tenant.ErrTenantNotFoundInContext)

	// 2. Valid context
	expectedID := uuid.New()
	ctxWithTenant := tenant.WithTenantID(ctx, expectedID)
	retrievedID, err := tenant.FromContext(ctxWithTenant)
	require.NoError(t, err)
	assert.Equal(t, expectedID, retrievedID)
}
