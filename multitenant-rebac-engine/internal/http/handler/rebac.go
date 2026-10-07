package handler

import (
	"encoding/json"
	"net/http"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/database"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/engine"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ReBACHandler struct {
	pool         *pgxpool.Pool
	cachedEngine engine.CheckEngine
	rawEngine    engine.CheckEngine
}

func NewReBACHandler(pool *pgxpool.Pool, cachedEngine, rawEngine engine.CheckEngine) *ReBACHandler {
	return &ReBACHandler{
		pool:         pool,
		cachedEngine: cachedEngine,
		rawEngine:    rawEngine,
	}
}

type CheckHTTPPayload struct {
	TenantID   string `json:"tenant_id"`
	Subject    string `json:"subject"`
	Permission string `json:"permission"`
	Object     string `json:"object"`
}

type CheckHTTPResponse struct {
	Allowed bool `json:"allowed"`
}

// Check evaluates an authorization query over HTTP.
// If header 'X-Bypass-Cache: true' is sent, it forces a cold run directly against PostgreSQL RLS.
func (h *ReBACHandler) Check(w http.ResponseWriter, r *http.Request) {
	var payload CheckHTTPPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid JSON payload", http.StatusBadRequest)
		return
	}

	tenantUUID, err := uuid.Parse(payload.TenantID)
	if err != nil || tenantUUID == uuid.Nil {
		http.Error(w, "invalid tenant_id UUID", http.StatusBadRequest)
		return
	}

	entityType, entityID, err := model.ParseEntity(payload.Object)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	subjectType, subjectID, _, err := model.ParseSubject(payload.Subject)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	checkReq := engine.CheckRequest{
		TenantID:    tenantUUID,
		EntityType:  entityType,
		EntityID:    entityID,
		Relation:    payload.Permission,
		SubjectType: subjectType,
		SubjectID:   subjectID,
	}

	// Select engine: Cold (Raw DB) or Warm (Multi-Tier Cached)
	eng := h.cachedEngine
	if r.Header.Get("X-Bypass-Cache") == "true" {
		eng = h.rawEngine
	}

	var allowed bool
	err = database.WithinTenantTx(r.Context(), h.pool, tenantUUID, func(tx pgx.Tx) error {
		var checkErr error
		allowed, checkErr = eng.Check(r.Context(), tx, checkReq)
		return checkErr
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(CheckHTTPResponse{Allowed: allowed})
}
