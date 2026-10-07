package engine

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// CheckRequest encapsulates the parameters for an authorization check:
// "Does subject (e.g. user:u-123) have permission/relation (e.g. viewer) on entity (e.g. document:doc-1)?"
type CheckRequest struct {
	TenantID    uuid.UUID `json:"tenant_id"`
	EntityType  string    `json:"entity_type"`
	EntityID    string    `json:"entity_id"`
	Relation    string    `json:"relation"`
	SubjectType string    `json:"subject_type"`
	SubjectID   string    `json:"subject_id"`
}

// CheckEngine defines the interface for ReBAC graph solvers.
type CheckEngine interface {
	Check(ctx context.Context, tx pgx.Tx, req CheckRequest) (bool, error)
}
