package handler

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"flashsale/internal/model"
	"github.com/google/uuid"
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

// CreateOrderPessimistic handles order creation using Pessimistic Locking (SELECT ... FOR UPDATE).
//
// MECHANISM:
// - Begins a database transaction.
// - Executes `SELECT stock FROM products WHERE id = $1 FOR UPDATE`.
//   This acquires an exclusive row-level lock on the product row.
// - Other transactions trying to read the same row with FOR UPDATE or perform UPDATE/DELETE
//   will block at the database level until this transaction finishes (COMMIT / ROLLBACK).
// - Guarantees 100% data consistency (no overselling, no negative stock).
//
// TRADE-OFF:
// - Serializes access at the database level, creating a queue.
// - P95/P99 latency increases significantly.
// - Holding connections while waiting for row locks can cause connection pool starvation.
func (h *Handler) CreateOrderPessimistic(w http.ResponseWriter, r *http.Request) {
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
		log.Printf("[DB ERROR - BeginTx (Pessimistic)]: %v\n", err)
		writeError(w, http.StatusInternalServerError, "Failed to begin transaction: "+err.Error())
		return
	}
	defer tx.Rollback()

	// 1. SELECT stock FROM products WHERE id = :id FOR UPDATE (Row-level lock)
	var stock int
	err = tx.QueryRowContext(r.Context(), "SELECT stock FROM products WHERE id = $1 FOR UPDATE", req.ProductID).Scan(&stock)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "Product not found")
			return
		}
		log.Printf("[DB ERROR - Select FOR UPDATE]: %v\n", err)
		writeError(w, http.StatusInternalServerError, "Failed to query stock with lock: "+err.Error())
		return
	}

	// 2. Application-level check: if stock <= 0, rollback and return 400
	if stock <= 0 {
		writeError(w, http.StatusBadRequest, "Out of stock")
		return
	}

	// 3. UPDATE products SET stock = stock - 1 WHERE id = :id
	_, err = tx.ExecContext(r.Context(), "UPDATE products SET stock = stock - 1 WHERE id = $1", req.ProductID)
	if err != nil {
		log.Printf("[DB ERROR - Update Stock (Pessimistic)]: %v\n", err)
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
		log.Printf("[DB ERROR - Insert Order (Pessimistic)]: %v\n", err)
		writeError(w, http.StatusInternalServerError, "Failed to record order: "+err.Error())
		return
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[DB ERROR - Commit (Pessimistic)]: %v\n", err)
		writeError(w, http.StatusInternalServerError, "Failed to commit transaction: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, model.OrderResponse{
		OrderID:   orderID,
		ProductID: req.ProductID,
		UserID:    req.UserID,
		Message:   "Order created successfully (Pessimistic Lock)",
	})
}

// CreateOrderOptimistic handles order creation using Optimistic Concurrency Control (OCC) with versioning.
//
// MECHANISM:
// - Reads current stock and version without acquiring a database lock.
// - Attempts atomic update: UPDATE products SET stock = stock - 1, version = version + 1 WHERE id = :id AND version = :current_version
// - Checks RowsAffected():
//   - If 1: update succeeded, inserts order, commits.
//   - If 0: conflict detected (another transaction modified the version).
// - Retries up to 3 times with exponential backoff.
//
// HIGH CONTENTION BEHAVIOR (The "Retry Storm"):
// In flash sale scenarios where hundreds of concurrent requests target the EXACT same item:
// - Only 1 request succeeds per version bump.
// - All other concurrent requests fail their CAS update and enter the retry loop simultaneously.
// - This triggers a "Retry Storm", multiplying DB queries, burning CPU, and exhausting connection pools,
//   causing widespread 409 Conflict failures.
func (h *Handler) CreateOrderOptimistic(w http.ResponseWriter, r *http.Request) {
	var req model.OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	if req.ProductID <= 0 || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "product_id and user_id are required")
		return
	}

	const maxRetries = 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		// 1. SELECT stock, version FROM products WHERE id = :id (Lock-free read)
		var stock, currentVersion int
		err := h.db.QueryRowContext(r.Context(),
			"SELECT stock, version FROM products WHERE id = $1", req.ProductID).
			Scan(&stock, &currentVersion)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusNotFound, "Product not found")
				return
			}
			log.Printf("[DB ERROR - Optimistic Select]: %v\n", err)
			writeError(w, http.StatusInternalServerError, "Failed to read product: "+err.Error())
			return
		}

		// 2. Stok kontrolü: Stok 0 veya altındaysa tekrar denemenin anlamı yok
		if stock <= 0 {
			writeError(w, http.StatusBadRequest, "Out of stock")
			return
		}

		// 3. CAS Update (Compare-And-Swap) in transaction
		tx, err := h.db.BeginTx(r.Context(), nil)
		if err != nil {
			log.Printf("[DB ERROR - Optimistic BeginTx]: %v\n", err)
			writeError(w, http.StatusInternalServerError, "Failed to begin transaction: "+err.Error())
			return
		}

		res, err := tx.ExecContext(r.Context(),
			"UPDATE products SET stock = stock - 1, version = version + 1 WHERE id = $1 AND version = $2",
			req.ProductID, currentVersion)
		if err != nil {
			_ = tx.Rollback()
			log.Printf("[DB ERROR - Optimistic Update]: %v\n", err)
			writeError(w, http.StatusInternalServerError, "Failed to update stock: "+err.Error())
			return
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			_ = tx.Rollback()
			writeError(w, http.StatusInternalServerError, "Failed to check affected rows: "+err.Error())
			return
		}

		// 4. Eğer satır etkilenmediyse (rowsAffected == 0), başka biri araya girmiştir -> Çakışma (Conflict)
		if rowsAffected == 0 {
			_ = tx.Rollback()
			log.Printf("[OCC CONFLICT] Product %d version mismatch (read version: %d, attempt %d/%d)\n",
				req.ProductID, currentVersion, attempt+1, maxRetries)

			if attempt < maxRetries-1 {
				// Exponential backoff: 10ms, 20ms
				backoff := time.Duration(10*(1<<attempt)) * time.Millisecond
				time.Sleep(backoff)
				continue
			}

			// Maksimum deneme aşıldı
			writeError(w, http.StatusConflict, "Optimistic lock conflict: retry limit exceeded")
			return
		}

		// 5. Başarılı ise sipariş kaydını oluştur
		var orderID int
		err = tx.QueryRowContext(
			r.Context(),
			"INSERT INTO orders (product_id, user_id) VALUES ($1, $2) RETURNING id",
			req.ProductID,
			req.UserID,
		).Scan(&orderID)
		if err != nil {
			_ = tx.Rollback()
			log.Printf("[DB ERROR - Optimistic Insert Order]: %v\n", err)
			writeError(w, http.StatusInternalServerError, "Failed to record order: "+err.Error())
			return
		}

		if err := tx.Commit(); err != nil {
			log.Printf("[DB ERROR - Optimistic Commit]: %v\n", err)
			writeError(w, http.StatusInternalServerError, "Failed to commit order: "+err.Error())
			return
		}

		writeJSON(w, http.StatusCreated, model.OrderResponse{
			OrderID:   orderID,
			ProductID: req.ProductID,
			UserID:    req.UserID,
			Message:   "Order created successfully (Optimistic Lock)",
		})
		return
	}
}

// CreateOrderFast handles lightning-fast order reservations entirely in Redis via atomic Lua scripting.
//
// MECHANISM (Zero Database Traffic at Ingestion):
// 1. Executes preloaded Lua script (EVALSHA) on single-threaded Redis.
// 2. Returns 1 -> Atomically decremented stock in Redis.
//    Generates unique reservation_id (UUID) and responds HTTP 202 Accepted: {"status": "PENDING", "reservation_id": uuid}.
// 3. Returns 0 -> Out of stock. Responds HTTP 410 Gone / 400 Bad Request immediately. Request NEVER hits PostgreSQL!
// 4. Returns -1 -> Product not found in Redis. Responds HTTP 404 Not Found.
//
// GUARANTEES:
// - Sub-10ms P99 latency.
// - Zero DB connections consumed during traffic spike.
// - Zero overselling guaranteed by Redis single-threaded execution model.
func (h *Handler) CreateOrderFast(w http.ResponseWriter, r *http.Request) {
	var req model.OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	if req.ProductID <= 0 || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "product_id and user_id are required")
		return
	}

	if h.redis == nil {
		writeError(w, http.StatusInternalServerError, "Redis service unavailable")
		return
	}

	// 1. Generate unique reservation_id (UUID)
	reservationID := uuid.New().String()

	// 2. Execute preloaded atomic Lua script on Redis via EVALSHA
	// Decrements stock AND appends to Redis Stream in ONE atomic step (Zero Dual-Write Hazard!)
	res, err := h.redis.DecrStockAndEnqueue(r.Context(), req.ProductID, reservationID, req.UserID)
	if err != nil {
		log.Printf("[REDIS ERROR]: %v\n", err)
		writeError(w, http.StatusInternalServerError, "Redis error: "+err.Error())
		return
	}

	switch res {
	case 1:
		// Success: Stock decremented and event enqueued atomically
		writeJSON(w, http.StatusAccepted, model.FastOrderResponse{
			Status:        "PENDING",
			ReservationID: reservationID,
		})
	case 0:
		// Out of stock: Fast fail! Database is 100% shielded from this traffic.
		writeError(w, http.StatusGone, "Tükendi (Out of stock)")
	case -1:
		// Product not found
		writeError(w, http.StatusNotFound, "Product not found")
	default:
		writeError(w, http.StatusInternalServerError, "Unexpected reservation response")
	}
}
