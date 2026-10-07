package grpc

import (
	"context"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/database"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/engine"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/model"
	rebacv1 "Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/proto/rebac/v1"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	rebacv1.UnimplementedAuthorizationServiceServer
	pool   *pgxpool.Pool
	engine engine.CheckEngine
}

// NewServer creates a new gRPC authorization service server instance.
func NewServer(pool *pgxpool.Pool, checkEngine engine.CheckEngine) *Server {
	return &Server{
		pool:   pool,
		engine: checkEngine,
	}
}

// Check evaluates whether a subject has a relation on an object within a tenant boundary.
func (s *Server) Check(ctx context.Context, req *rebacv1.CheckRequest) (*rebacv1.CheckResponse, error) {
	if req.TenantId == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}

	tenantUUID, err := uuid.Parse(req.TenantId)
	if err != nil || tenantUUID == uuid.Nil {
		return nil, status.Error(codes.InvalidArgument, "invalid tenant_id UUID format")
	}

	if req.Object == "" {
		return nil, status.Error(codes.InvalidArgument, "object is required (e.g. document:doc-1)")
	}

	if req.Subject == "" {
		return nil, status.Error(codes.InvalidArgument, "subject is required (e.g. user:u-123)")
	}

	if req.Permission == "" {
		return nil, status.Error(codes.InvalidArgument, "permission is required (e.g. viewer)")
	}

	entityType, entityID, err := model.ParseEntity(req.Object)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "malformed object: %v", err)
	}

	subjectType, subjectID, _, err := model.ParseSubject(req.Subject)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "malformed subject: %v", err)
	}

	checkReq := engine.CheckRequest{
		TenantID:    tenantUUID,
		EntityType:  entityType,
		EntityID:    entityID,
		Relation:    req.Permission,
		SubjectType: subjectType,
		SubjectID:   subjectID,
	}

	var allowed bool
	// Execute authorization graph check strictly within the tenant's transaction boundary!
	err = database.WithinTenantTx(ctx, s.pool, tenantUUID, func(tx pgx.Tx) error {
		var checkErr error
		allowed, checkErr = s.engine.Check(ctx, tx, checkReq)
		return checkErr
	})

	if err != nil {
		return nil, status.Errorf(codes.Internal, "authorization check error: %v", err)
	}

	return &rebacv1.CheckResponse{
		Allowed: allowed,
	}, nil
}
