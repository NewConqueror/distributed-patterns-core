package handler

import (
	"database/sql"
	"encoding/json"
	"net/http"

	"flashsale/internal/model"
)

type Handler struct {
	db *sql.DB
}

func NewHandler(db *sql.DB) *Handler {
	return &Handler{db: db}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	// Orders
	mux.HandleFunc("POST /api/v1/orders/naive", h.CreateOrderNaive)
	mux.HandleFunc("POST /api/v1/orders/pessimistic", h.CreateOrderPessimistic)
	mux.HandleFunc("POST /api/v1/orders/optimistic", h.CreateOrderOptimistic)

	// Products
	mux.HandleFunc("GET /api/v1/products/{id}", h.GetProduct)
	mux.HandleFunc("POST /api/v1/products/{id}/reset", h.ResetProductStock)

	// Health Check
	mux.HandleFunc("GET /healthz", h.HealthCheck)
}

func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	if err := h.db.PingContext(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "Database unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, model.ErrorResponse{Error: message})
}
