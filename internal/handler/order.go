package handler

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"flashsale/internal/model"
)

// CreateOrderNaive handles order creation using the Naive (Flawed) approach.
//
// ANTIPATTERN (Check-Then-Act):
// This endpoint purposely does NOT use row locks (e.g. SELECT FOR UPDATE),
// optimistic concurrency checks, or distributed locks.
//
// Under concurrent load:
// 1. Multiple requests read the same positive stock value.
// 2. All pass the application-level `if stock > 0` check.
// 3. All attempt to decrement stock sequentially.
// 4. Results in overselling, or database check constraint errors (`CHECK (stock >= 0)`),
//    and connection pool exhaustion if open connections are unlimited.
func (h *Handler) CreateOrderNaive(w http.ResponseWriter, r *http.Request) {
	var req model.OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	if req.ProductID <= 0 || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "product_id and user_id are required")
		return
	}

	tx, err := h.db.BeginTx(r.Context(), nil)
	if err != nil {
		log.Printf("[DB ERROR - BeginTx]: %v\n", err)
		writeError(w, http.StatusInternalServerError, "Failed to begin transaction: "+err.Error())
		return
	}
	defer tx.Rollback()

	// 1. SELECT stock FROM products WHERE id = :id
	var stock int
	err = tx.QueryRowContext(r.Context(), "SELECT stock FROM products WHERE id = $1", req.ProductID).Scan(&stock)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "Product not found")
			return
		}
		log.Printf("[DB ERROR - Select Stock]: %v\n", err)
		writeError(w, http.StatusInternalServerError, "Failed to query stock: "+err.Error())
		return
	}

	// 2. Application-level check: if stock > 0
	if stock <= 0 {
		writeError(w, http.StatusBadRequest, "Out of stock")
		return
	}

	// 3. UPDATE products SET stock = stock - 1 WHERE id = :id
	_, err = tx.ExecContext(r.Context(), "UPDATE products SET stock = stock - 1 WHERE id = $1", req.ProductID)
	if err != nil {
		log.Printf("[DB ERROR - Update Stock]: %v\n", err)
		writeError(w, http.StatusInternalServerError, "Failed to update stock: "+err.Error())
		return
	}

	// 4. INSERT INTO orders ...
	var orderID int
	err = tx.QueryRowContext(
		r.Context(),
		"INSERT INTO orders (product_id, user_id) VALUES ($1, $2) RETURNING id",
		req.ProductID,
		req.UserID,
	).Scan(&orderID)
	if err != nil {
		log.Printf("[DB ERROR - Insert Order]: %v\n", err)
		writeError(w, http.StatusInternalServerError, "Failed to record order: "+err.Error())
		return
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[DB ERROR - Commit]: %v\n", err)
		writeError(w, http.StatusInternalServerError, "Failed to commit transaction: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, model.OrderResponse{
		OrderID:   orderID,
		ProductID: req.ProductID,
		UserID:    req.UserID,
		Message:   "Order created successfully (Naive)",
	})
}
