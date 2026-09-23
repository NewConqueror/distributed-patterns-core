package repository

import (
	"context"
	"errors"
	"fmt"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/model"

	"github.com/jackc/pgx/v5"
)

type TupleRepository interface {
	WriteTuple(ctx context.Context, tx pgx.Tx, tuple *model.Tuple) error
	DeleteTuple(ctx context.Context, tx pgx.Tx, tuple *model.Tuple) error
	ListTuplesByEntity(ctx context.Context, tx pgx.Tx, entityType, entityID string) ([]model.Tuple, error)
	ListAllForEntity(ctx context.Context, tx pgx.Tx, entityType, entityID string) ([]model.Tuple, error)
	FindDirectRelation(ctx context.Context, tx pgx.Tx, entityType, entityID, relation, subjectType, subjectID string) (bool, error)
	ListAllTuples(ctx context.Context, tx pgx.Tx) ([]model.Tuple, error)
}

type pgTupleRepository struct{}

func NewTupleRepository() TupleRepository {
	return &pgTupleRepository{}
}

// WriteTuple inserts or ignores an existing tuple. RLS verifies tenant_id matches app.current_tenant_id.
func (r *pgTupleRepository) WriteTuple(ctx context.Context, tx pgx.Tx, tuple *model.Tuple) error {
	query := `
		INSERT INTO tuples (
			tenant_id, entity_type, entity_id, relation, subject_type, subject_id, subject_relation
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (tenant_id, entity_type, entity_id, relation, subject_type, subject_id, COALESCE(subject_relation, ''))
		DO NOTHING
		RETURNING id, created_at;
	`
	err := tx.QueryRow(
		ctx, query,
		tuple.TenantID,
		tuple.EntityType,
		tuple.EntityID,
		tuple.Relation,
		tuple.SubjectType,
		tuple.SubjectID,
		tuple.SubjectRelation,
	).Scan(&tuple.ID, &tuple.CreatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Already exists (DO NOTHING triggered)
			return nil
		}
		return fmt.Errorf("failed to write relation tuple: %w", err)
	}

	return nil
}

// DeleteTuple removes a relation tuple.
func (r *pgTupleRepository) DeleteTuple(ctx context.Context, tx pgx.Tx, tuple *model.Tuple) error {
	var query string
	var args []any

	if tuple.SubjectRelation == nil {
		query = `
			DELETE FROM tuples
			WHERE entity_type = $1 AND entity_id = $2 AND relation = $3 
			  AND subject_type = $4 AND subject_id = $5 AND subject_relation IS NULL;
		`
		args = []any{tuple.EntityType, tuple.EntityID, tuple.Relation, tuple.SubjectType, tuple.SubjectID}
	} else {
		query = `
			DELETE FROM tuples
			WHERE entity_type = $1 AND entity_id = $2 AND relation = $3 
			  AND subject_type = $4 AND subject_id = $5 AND subject_relation = $6;
		`
		args = []any{tuple.EntityType, tuple.EntityID, tuple.Relation, tuple.SubjectType, tuple.SubjectID, *tuple.SubjectRelation}
	}

	_, err := tx.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to delete tuple: %w", err)
	}
	return nil
}

// FindDirectRelation checks if a direct relation exists between an entity and subject.
func (r *pgTupleRepository) FindDirectRelation(
	ctx context.Context,
	tx pgx.Tx,
	entityType, entityID, relation, subjectType, subjectID string,
) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT 1 FROM tuples
			WHERE entity_type = $1
			  AND entity_id = $2
			  AND relation = $3
			  AND subject_type = $4
			  AND subject_id = $5
			  AND subject_relation IS NULL
		);
	`
	var exists bool
	err := tx.QueryRow(ctx, query, entityType, entityID, relation, subjectType, subjectID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check direct relation: %w", err)
	}
	return exists, nil
}

// ListTuplesByEntity retrieves all tuples for an entity filtered by relation.
func (r *pgTupleRepository) ListTuplesByEntity(
	ctx context.Context,
	tx pgx.Tx,
	entityType, entityID string,
) ([]model.Tuple, error) {
	return r.ListAllForEntity(ctx, tx, entityType, entityID)
}

// ListAllForEntity retrieves all tuples where entity matches (e.g. document:doc-1)
func (r *pgTupleRepository) ListAllForEntity(
	ctx context.Context,
	tx pgx.Tx,
	entityType, entityID string,
) ([]model.Tuple, error) {
	query := `
		SELECT id, tenant_id, entity_type, entity_id, relation, subject_type, subject_id, subject_relation, created_at
		FROM tuples
		WHERE entity_type = $1 AND entity_id = $2
		ORDER BY created_at ASC;
	`
	return r.scanTuples(ctx, tx, query, entityType, entityID)
}

// ListAllTuples retrieves all tuples visible to the current tenant.
func (r *pgTupleRepository) ListAllTuples(ctx context.Context, tx pgx.Tx) ([]model.Tuple, error) {
	query := `
		SELECT id, tenant_id, entity_type, entity_id, relation, subject_type, subject_id, subject_relation, created_at
		FROM tuples
		ORDER BY created_at ASC;
	`
	return r.scanTuples(ctx, tx, query)
}

func (r *pgTupleRepository) scanTuples(ctx context.Context, tx pgx.Tx, query string, args ...any) ([]model.Tuple, error) {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query tuples error: %w", err)
	}
	defer rows.Close()

	var result []model.Tuple
	for rows.Next() {
		var t model.Tuple
		if err := rows.Scan(
			&t.ID,
			&t.TenantID,
			&t.EntityType,
			&t.EntityID,
			&t.Relation,
			&t.SubjectType,
			&t.SubjectID,
			&t.SubjectRelation,
			&t.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan tuple row error: %w", err)
		}
		result = append(result, t)
	}

	return result, rows.Err()
}
