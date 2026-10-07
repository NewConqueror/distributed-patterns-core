package middleware

import (
	"encoding/json"
	"net/http"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/tenant"

	"github.com/google/uuid"
)

const HeaderTenantID = "X-Tenant-ID"

type ErrorResponse struct {
	Error   string `json:"error"`
	Details string `json:"details,omitempty"`
}

// TenantGuardMiddleware extracts and validates the tenant ID from incoming HTTP headers.
// If the header is missing or is not a valid UUID format, it aborts the request immediately
// without touching the database connection pool.
func TenantGuardMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantIDStr := r.Header.Get(HeaderTenantID)
		if tenantIDStr == "" {
			writeJSONError(w, http.StatusBadRequest, "Missing required header: "+HeaderTenantID)
			return
		}

		tenantID, err := uuid.Parse(tenantIDStr)
		if err != nil || tenantID == uuid.Nil {
			writeJSONError(w, http.StatusBadRequest, "Invalid tenant ID format: must be a non-nil UUID")
			return
		}

		// Inject validated tenant ID into request context
		ctx := tenant.WithTenantID(r.Context(), tenantID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func writeJSONError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(ErrorResponse{
		Error: message,
	})
}
