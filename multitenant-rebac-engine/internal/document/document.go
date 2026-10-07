package document

import (
	"time"

	"github.com/google/uuid"
)

// Document represents a protected multi-tenant document entity.
type Document struct {
	ID        uuid.UUID  `json:"id"`
	TenantID  uuid.UUID  `json:"tenant_id"`
	OwnerID   *uuid.UUID `json:"owner_id,omitempty"`
	Title     string     `json:"title"`
	Content   string     `json:"content"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// CreateDocumentRequest represents the payload for creating a new document.
type CreateDocumentRequest struct {
	Title   string     `json:"title"`
	Content string     `json:"content"`
	OwnerID *uuid.UUID `json:"owner_id,omitempty"`
}
