package handler

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"flashsale/internal/model"
)

// GetProduct retrieves product details including current stock.
func (h *Handler) GetProduct(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid product ID")
		return
	}

	var product model.Product
	err = h.db.QueryRowContext(r.Context(), "SELECT id, name, stock, version FROM products WHERE id = $1", id).
		Scan(&product.ID, &product.Name, &product.Stock, &product.Version)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "Product not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "Database error: "+err.Error())
		return
	}

	redisStock := -1
	if h.redis != nil {
		if s, err := h.redis.GetProductStock(r.Context(), id); err == nil {
			redisStock = s
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":          product.ID,
		"name":        product.Name,
		"stock":       product.Stock,
		"version":     product.Version,
		"redis_stock": redisStock,
	})
}

// ResetProductStock resets product stock, version to 0, clears orders, and resets Redis cache.
func (h *Handler) ResetProductStock(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid product ID")
		return
	}

	targetStock := 100
	var req model.ResetStockRequest
	if r.Body != nil && r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err == nil && req.Stock > 0 {
			targetStock = req.Stock
		}
	}

	tx, err := h.db.BeginTx(r.Context(), nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to start transaction: "+err.Error())
		return
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(r.Context(), "UPDATE products SET stock = $1, version = 0 WHERE id = $2", targetStock, id)
	if err != nil {
		log.Printf("[DB ERROR - Reset Stock]: %v\n", err)
		writeError(w, http.StatusInternalServerError, "Failed to reset stock: "+err.Error())
		return
	}

	_, err = tx.ExecContext(r.Context(), "DELETE FROM orders WHERE product_id = $1", id)
	if err != nil {
		log.Printf("[DB ERROR - Clear Orders]: %v\n", err)
		writeError(w, http.StatusInternalServerError, "Failed to clear orders: "+err.Error())
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to commit reset: "+err.Error())
		return
	}

	// Synchronize Redis cache key
	if h.redis != nil {
		if err := h.redis.InitProductStock(r.Context(), id, targetStock); err != nil {
			log.Printf("[REDIS ERROR - Reset Stock]: %v\n", err)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"message":     "Product stock, orders, and Redis cache reset successfully",
		"product_id":  id,
		"stock":       targetStock,
		"redis_stock": targetStock,
	})
}
