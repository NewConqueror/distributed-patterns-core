package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/database"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/document"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/tenant"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DocumentHandler struct {
	pool *pgxpool.Pool
	repo document.Repository
}

func NewDocumentHandler(pool *pgxpool.Pool, repo document.Repository) *DocumentHandler {
	return &DocumentHandler{
		pool: pool,
		repo: repo,
	}
}

// Create handles document creation within the tenant's transaction boundary.
func (h *DocumentHandler) Create(w http.ResponseWriter, r *http.Request) {
	tenantID, err := tenant.FromContext(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	var req document.CreateDocumentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	doc := &document.Document{
		TenantID: tenantID,
		OwnerID:  req.OwnerID,
		Title:    req.Title,
		Content:  req.Content,
	}

	err = database.WithinTenant(r.Context(), h.pool, func(tx pgx.Tx) error {
		return h.repo.Create(r.Context(), tx, doc)
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(doc)
}

// List handles retrieving documents scoped strictly to the current tenant.
func (h *DocumentHandler) List(w http.ResponseWriter, r *http.Request) {
	var docs []document.Document

	err := database.WithinTenant(r.Context(), h.pool, func(tx pgx.Tx) error {
		var listErr error
		docs, listErr = h.repo.List(r.Context(), tx)
		return listErr
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if docs == nil {
		docs = []document.Document{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(docs)
}

// GetByID handles retrieving a single document by its ID.
func (h *DocumentHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	docID, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid document ID", http.StatusBadRequest)
		return
	}

	var doc *document.Document
	err = database.WithinTenant(r.Context(), h.pool, func(tx pgx.Tx) error {
		var getErr error
		doc, getErr = h.repo.GetByID(r.Context(), tx, docID)
		return getErr
	})
	if err != nil {
		if errors.Is(err, document.ErrDocumentNotFound) {
			http.Error(w, "document not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(doc)
}
