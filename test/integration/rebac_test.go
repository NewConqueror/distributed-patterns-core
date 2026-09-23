package integration

import (
	"context"
	"testing"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/database"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/engine"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/grpc"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/model"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/repository"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/schema"
	rebacv1 "Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/proto/rebac/v1"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReBACEngines(t *testing.T) {
	pool := setupTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	tenantID := createTestTenant(t, pool, "ReBAC-Test-Tenant")

	tupleRepo := repository.NewTupleRepository()
	sqlEngine := engine.NewSQLCheckEngine()
	memEngine := engine.NewMemoryCheckEngine(schema.DefaultSchema(), tupleRepo)

	// Helper to write tuples within tenant
	writeTuple := func(entityType, entityID, relation, subjectType, subjectID string, subjectRel *string) {
		tup := &model.Tuple{
			TenantID:        tenantID,
			EntityType:      entityType,
			EntityID:        entityID,
			Relation:        relation,
			SubjectType:     subjectType,
			SubjectID:       subjectID,
			SubjectRelation: subjectRel,
		}
		err := database.WithinTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
			return tupleRepo.WriteTuple(ctx, tx, tup)
		})
		require.NoError(t, err, "Failed to seed tuple: %v", tup)
	}

	memberRel := "member"

	// 1. Seed Relationship Graph:
	// - Alice is a member of workspace "engineering"
	// - Folder "backend" parent is workspace "engineering"
	// - Document "architecture" parent is folder "backend"
	// - Bob is direct owner of Document "architecture"
	writeTuple("workspace", "engineering", "member", "user", "alice", nil)
	writeTuple("folder", "backend", "parent", "workspace", "engineering", nil)
	writeTuple("folder", "backend", "viewer", "workspace", "engineering", &memberRel)
	writeTuple("document", "architecture", "parent", "folder", "backend", nil)
	writeTuple("document", "architecture", "owner", "user", "bob", nil)

	type testCase struct {
		name        string
		req         engine.CheckRequest
		expectedAns bool
	}

	testCases := []testCase{
		{
			name: "Direct relation: Bob is direct owner of architecture doc",
			req: engine.CheckRequest{
				TenantID: tenantID, EntityType: "document", EntityID: "architecture",
				Relation: "owner", SubjectType: "user", SubjectID: "bob",
			},
			expectedAns: true,
		},
		{
			name: "Direct relation negative: Alice is NOT direct owner of architecture doc",
			req: engine.CheckRequest{
				TenantID: tenantID, EntityType: "document", EntityID: "architecture",
				Relation: "owner", SubjectType: "user", SubjectID: "alice",
			},
			expectedAns: false,
		},
		{
			name: "Computed userset: Bob (owner) has editor access on architecture doc",
			req: engine.CheckRequest{
				TenantID: tenantID, EntityType: "document", EntityID: "architecture",
				Relation: "editor", SubjectType: "user", SubjectID: "bob",
			},
			expectedAns: true,
		},
		{
			name: "Computed userset: Bob (owner) has viewer access on architecture doc",
			req: engine.CheckRequest{
				TenantID: tenantID, EntityType: "document", EntityID: "architecture",
				Relation: "viewer", SubjectType: "user", SubjectID: "bob",
			},
			expectedAns: true,
		},
		{
			name: "Transitive hierarchy: Alice (member of workspace) has viewer on architecture doc",
			req: engine.CheckRequest{
				TenantID: tenantID, EntityType: "document", EntityID: "architecture",
				Relation: "viewer", SubjectType: "user", SubjectID: "alice",
			},
			expectedAns: true,
		},
		{
			name: "Negative access: Charlie (external user) has NO access to architecture doc",
			req: engine.CheckRequest{
				TenantID: tenantID, EntityType: "document", EntityID: "architecture",
				Relation: "viewer", SubjectType: "user", SubjectID: "charlie",
			},
			expectedAns: false,
		},
	}

	// Run all test cases against BOTH engines (PostgreSQL Recursive CTE & Go In-Memory BFS)
	for _, tc := range testCases {
		t.Run("SQL_CTE_"+tc.name, func(t *testing.T) {
			var allowed bool
			err := database.WithinTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
				var checkErr error
				allowed, checkErr = sqlEngine.Check(ctx, tx, tc.req)
				return checkErr
			})
			require.NoError(t, err)
			assert.Equal(t, tc.expectedAns, allowed)
		})

		t.Run("In_Memory_BFS_"+tc.name, func(t *testing.T) {
			var allowed bool
			err := database.WithinTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
				var checkErr error
				allowed, checkErr = memEngine.Check(ctx, tx, tc.req)
				return checkErr
			})
			require.NoError(t, err)
			assert.Equal(t, tc.expectedAns, allowed)
		})
	}

	// =========================================================================
	// Cycle Detection Test: Circular parent relationship A -> B -> A
	// =========================================================================
	t.Run("Cycle_Detection_Prevents_Infinite_Loop", func(t *testing.T) {
		cycleTenant := createTestTenant(t, pool, "Cycle-Test-Tenant")

		// Create cycle: folder:loop-a#parent@folder:loop-b AND folder:loop-b#parent@folder:loop-a
		cycleTupleA := &model.Tuple{
			TenantID: cycleTenant, EntityType: "folder", EntityID: "loop-a",
			Relation: "parent", SubjectType: "folder", SubjectID: "loop-b",
		}
		cycleTupleB := &model.Tuple{
			TenantID: cycleTenant, EntityType: "folder", EntityID: "loop-b",
			Relation: "parent", SubjectType: "folder", SubjectID: "loop-a",
		}

		err := database.WithinTenantTx(ctx, pool, cycleTenant, func(tx pgx.Tx) error {
			if err := tupleRepo.WriteTuple(ctx, tx, cycleTupleA); err != nil {
				return err
			}
			return tupleRepo.WriteTuple(ctx, tx, cycleTupleB)
		})
		require.NoError(t, err)

		req := engine.CheckRequest{
			TenantID:    cycleTenant,
			EntityType:  "folder",
			EntityID:    "loop-a",
			Relation:    "viewer",
			SubjectType: "user",
			SubjectID:   "random-user",
		}

		// Verify SQL CTE does not hang / infinite loop
		err = database.WithinTenantTx(ctx, pool, cycleTenant, func(tx pgx.Tx) error {
			allowed, checkErr := sqlEngine.Check(ctx, tx, req)
			assert.NoError(t, checkErr)
			assert.False(t, allowed)
			return nil
		})
		require.NoError(t, err)

		// Verify In-Memory BFS also safely terminates with cycle
		err = database.WithinTenantTx(ctx, pool, cycleTenant, func(tx pgx.Tx) error {
			allowed, checkErr := memEngine.Check(ctx, tx, req)
			assert.NoError(t, checkErr)
			assert.False(t, allowed)
			return nil
		})
		require.NoError(t, err)
	})

	// =========================================================================
	// gRPC Check API Test
	// =========================================================================
	t.Run("gRPC_Check_Service", func(t *testing.T) {
		grpcServer := grpc.NewServer(pool, sqlEngine)

		// Check Alice's viewer access via gRPC
		resp, err := grpcServer.Check(ctx, &rebacv1.CheckRequest{
			TenantId:   tenantID.String(),
			Object:     "document:architecture",
			Subject:    "user:alice",
			Permission: "viewer",
		})
		require.NoError(t, err)
		assert.True(t, resp.Allowed)

		// Check Charlie's viewer access via gRPC
		resp, err = grpcServer.Check(ctx, &rebacv1.CheckRequest{
			TenantId:   tenantID.String(),
			Object:     "document:architecture",
			Subject:    "user:charlie",
			Permission: "viewer",
		})
		require.NoError(t, err)
		assert.False(t, resp.Allowed)

		// Check Invalid Request (missing tenant)
		_, err = grpcServer.Check(ctx, &rebacv1.CheckRequest{
			TenantId:   "",
			Object:     "document:architecture",
			Subject:    "user:alice",
			Permission: "viewer",
		})
		assert.Error(t, err)
	})
}

func BenchmarkEngines(b *testing.B) {
	// Helper to setup pool for benchmark
	cfg := database.DefaultConfig()
	pool, err := database.NewPool(context.Background(), cfg)
	if err != nil {
		b.Skipf("Skipping benchmark: DB not reachable: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()
	tenantID := uuid.New()
	_, _ = pool.Exec(ctx, "INSERT INTO tenants (id, name) VALUES ($1, $2)", tenantID, "Bench-Tenant")

	tupleRepo := repository.NewTupleRepository()
	sqlEngine := engine.NewSQLCheckEngine()
	memEngine := engine.NewMemoryCheckEngine(schema.DefaultSchema(), tupleRepo)

	// Seed hierarchy
	_ = database.WithinTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
		_ = tupleRepo.WriteTuple(ctx, tx, &model.Tuple{
			TenantID: tenantID, EntityType: "folder", EntityID: "bench-folder",
			Relation: "viewer", SubjectType: "user", SubjectID: "alice",
		})
		_ = tupleRepo.WriteTuple(ctx, tx, &model.Tuple{
			TenantID: tenantID, EntityType: "document", EntityID: "bench-doc",
			Relation: "parent", SubjectType: "folder", SubjectID: "bench-folder",
		})
		return nil
	})

	req := engine.CheckRequest{
		TenantID: tenantID, EntityType: "document", EntityID: "bench-doc",
		Relation: "viewer", SubjectType: "user", SubjectID: "alice",
	}

	b.Run("PostgreSQL_Recursive_CTE", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = database.WithinTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
				_, err := sqlEngine.Check(ctx, tx, req)
				return err
			})
		}
	})

	b.Run("Go_In_Memory_BFS", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = database.WithinTenantTx(ctx, pool, tenantID, func(tx pgx.Tx) error {
				_, err := memEngine.Check(ctx, tx, req)
				return err
			})
		}
	})
}

