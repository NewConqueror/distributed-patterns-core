package document

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	ErrDocumentNotFound = errors.New("document not found")
)

type Repository interface {
	Create(ctx context.Context, tx pgx.Tx, doc *Document) error
	List(ctx context.Context, tx pgx.Tx) ([]Document, error)
	GetByID(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*Document, error)
	Delete(ctx context.Context, tx pgx.Tx, id uuid.UUID) error
}

type pgRepository struct{}

func NewRepository() Repository {
	return &pgRepository{}
}

// Create inserts a document into the database.
// If the provided doc.TenantID does not match the session's app.current_tenant_id,
// PostgreSQL RLS WITH CHECK policy will physically reject the insert.
func (r *pgRepository) Create(ctx context.Context, tx pgx.Tx, doc *Document) error {
	query := `
		INSERT INTO documents (tenant_id, owner_id, title, content)
		VALUES ($1, $2, $3, $4)
		RETURNING id, created_at, updated_at;
	`
	err := tx.QueryRow(ctx, query, doc.TenantID, doc.OwnerID, doc.Title, doc.Content).
		Scan(&doc.ID, &doc.CreatedAt, &doc.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to insert document: %w", err)
	}
	return nil
}

// List returns all documents visible to the current tenant.
// Notice: There is NO 'WHERE tenant_id = ?' clause needed here!
// PostgreSQL RLS automatically and physically filters rows at the engine level.
func (r *pgRepository) List(ctx context.Context, tx pgx.Tx) ([]Document, error) {
	query := `
		SELECT id, tenant_id, owner_id, title, content, created_at, updated_at
		FROM documents
		ORDER BY created_at DESC;
	`
	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query documents: %w", err)
	}
	defer rows.Close()

	var docs []Document
	for rows.Next() {
		var doc Document
		if err := rows.Scan(
			&doc.ID,
			&doc.TenantID,
			&doc.OwnerID,
			&doc.Title,
			&doc.Content,
			&doc.CreatedAt,
			&doc.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan document row: %w", err)
		}
		docs = append(docs, doc)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return docs, nil
}

// GetByID fetches a specific document by its ID.
// Even if an attacker guesses another tenant's document ID, RLS hides it completely.
func (r *pgRepository) GetByID(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*Document, error) {
	query := `
		SELECT id, tenant_id, owner_id, title, content, created_at, updated_at
		FROM documents
		WHERE id = $1;
	`
	var doc Document
	err := tx.QueryRow(ctx, query, id).Scan(
		&doc.ID,
		&doc.TenantID,
		&doc.OwnerID,
		&doc.Title,
		&doc.Content,
		&doc.CreatedAt,
		&doc.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrDocumentNotFound
		}
		return nil, fmt.Errorf("failed to query document by id: %w", err)
	}

	return &doc, nil
}

// Delete removes a document by ID. RLS ensures only documents belonging to the current tenant can be deleted.
func (r *pgRepository) Delete(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	query := `DELETE FROM documents WHERE id = $1;`
	tag, err := tx.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete document: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrDocumentNotFound
	}
	return nil
}
