package tenant

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrTenantNotFoundInContext = errors.New("tenant ID not found in context")
	ErrInvalidTenantID         = errors.New("invalid tenant ID")
)

type contextKey struct{}

var tenantContextKey = contextKey{}

// Tenant represents a tenant entity.
type Tenant struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// WithTenantID places the tenant UUID into the context.
func WithTenantID(ctx context.Context, id uuid.UUID) context.Context {
	return context.WithValue(ctx, tenantContextKey, id)
}

// FromContext extracts the tenant UUID from the context.
func FromContext(ctx context.Context) (uuid.UUID, error) {
	val := ctx.Value(tenantContextKey)
	if val == nil {
		return uuid.Nil, ErrTenantNotFoundInContext
	}
	id, ok := val.(uuid.UUID)
	if !ok || id == uuid.Nil {
		return uuid.Nil, ErrInvalidTenantID
	}
	return id, nil
}
