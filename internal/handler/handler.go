package handler

import (
	"database/sql"
	"encoding/json"
	"net/http"

	"flashsale/internal/database"
	"flashsale/internal/model"
)

type Handler struct {
	db    *sql.DB
	redis *database.RedisService
}

func NewHandler(db *sql.DB, redisSvc *database.RedisService) *Handler {
	return &Handler{db: db, redis: redisSvc}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	// Orders
	mux.HandleFunc("POST /api/v1/orders/naive", h.CreateOrderNaive)
	mux.HandleFunc("POST /api/v1/orders/pessimistic", h.CreateOrderPessimistic)
	mux.HandleFunc("POST /api/v1/orders/optimistic", h.CreateOrderOptimistic)
	mux.HandleFunc("POST /api/v1/orders/fast", h.CreateOrderFast)

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
	if h.redis != nil {
		if err := h.redis.Client.Ping(r.Context()).Err(); err != nil {
			writeError(w, http.StatusServiceUnavailable, "Redis unreachable")
			return
		}
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
