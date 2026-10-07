package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/tenant"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTenantGuardMiddleware(t *testing.T) {
	testCases := []struct {
		name           string
		headerValue    string
		hasHeader      bool
		expectedStatus int
		expectInCtx    bool
	}{
		{
			name:           "Valid UUID header",
			headerValue:    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
			hasHeader:      true,
			expectedStatus: http.StatusOK,
			expectInCtx:    true,
		},
		{
			name:           "Missing Header",
			hasHeader:      false,
			expectedStatus: http.StatusBadRequest,
			expectInCtx:    false,
		},
		{
			name:           "Invalid UUID string",
			headerValue:    "not-a-valid-uuid",
			hasHeader:      true,
			expectedStatus: http.StatusBadRequest,
			expectInCtx:    false,
		},
		{
			name:           "Nil UUID",
			headerValue:    uuid.Nil.String(),
			hasHeader:      true,
			expectedStatus: http.StatusBadRequest,
			expectInCtx:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var capturedTenantID uuid.UUID
			nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var err error
				capturedTenantID, err = tenant.FromContext(r.Context())
				require.NoError(t, err)
				w.WriteHeader(http.StatusOK)
			})

			handlerToTest := TenantGuardMiddleware(nextHandler)

			req := httptest.NewRequest(http.MethodGet, "/documents", nil)
			if tc.hasHeader {
				req.Header.Set(HeaderTenantID, tc.headerValue)
			}
			rec := httptest.NewRecorder()

			handlerToTest.ServeHTTP(rec, req)

			assert.Equal(t, tc.expectedStatus, rec.Code)
			if tc.expectInCtx {
				expectedUUID, _ := uuid.Parse(tc.headerValue)
				assert.Equal(t, expectedUUID, capturedTenantID)
			}
		})
	}
}
