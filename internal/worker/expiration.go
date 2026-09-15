package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"flashsale/internal/database"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// ExpirationWorker listens for Redis Key Expiration events (__keyevent@0__:expired)
// and handles automatic order cancellation and inventory rollback.
//
// EDGE CASE DEFENSE:
// If a customer reserves stock during a flash sale but fails to complete payment
// within the reservation window (e.g. 10 minutes), this worker:
// 1. Catches the expired key event (order:{reservation_id}:ttl).
// 2. Verifies the order is still PENDING in PostgreSQL.
// 3. Atomically updates order status to EXPIRED.
// 4. Rolls back inventory in both PostgreSQL (+1) and Redis (INCR).
// 5. Appends an OrderExpired event to outbox_events.
type ExpirationWorker struct {
	db       *sql.DB
	redis    *database.RedisService
	pubsub   *redis.PubSub
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewExpirationWorker(db *sql.DB, redisSvc *database.RedisService) *ExpirationWorker {
	return &ExpirationWorker{
		db:       db,
		redis:    redisSvc,
		stopChan: make(chan struct{}),
	}
}

func (w *ExpirationWorker) Start(ctx context.Context) {
	w.pubsub = w.redis.SubscribeKeyExpiration(ctx)
	w.wg.Add(1)
	go w.run(ctx)
	log.Println("[EXPIRATION WORKER] Redis TTL expiration & stock rollback worker started.")
}

func (w *ExpirationWorker) Stop() {
	close(w.stopChan)
	if w.pubsub != nil {
		_ = w.pubsub.Close()
	}
	w.wg.Wait()
	log.Println("[EXPIRATION WORKER] Expiration worker stopped cleanly.")
}

func (w *ExpirationWorker) run(ctx context.Context) {
	defer w.wg.Done()

	ch := w.pubsub.Channel()

	for {
		select {
		case <-w.stopChan:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if msg != nil && msg.Payload != "" {
				w.handleExpiredKey(ctx, msg.Payload)
			}
		}
	}
}

func (w *ExpirationWorker) handleExpiredKey(ctx context.Context, key string) {
	// Expected key pattern: order:{reservation_id}:ttl
	if !strings.HasPrefix(key, "order:") || !strings.HasSuffix(key, ":ttl") {
		return
	}

	reservationID := strings.TrimPrefix(key, "order:")
	reservationID = strings.TrimSuffix(reservationID, ":ttl")

	log.Printf("[EXPIRATION WORKER] Detected expired reservation key: %s (reservation_id: %s)\n", key, reservationID)

	if err := w.rollbackExpiredReservation(ctx, reservationID); err != nil {
		log.Printf("[EXPIRATION ERROR] Failed to rollback reservation %s: %v\n", reservationID, err)
	}
}

func (w *ExpirationWorker) rollbackExpiredReservation(ctx context.Context, reservationID string) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin rollback tx: %w", err)
	}
	defer tx.Rollback()

	// 1. Query order by reservation_id with row lock
	var orderID, productID int
	var status, userID string
	err = tx.QueryRowContext(ctx,
		"SELECT id, product_id, user_id, status FROM orders WHERE reservation_id = $1 FOR UPDATE",
		reservationID,
	).Scan(&orderID, &productID, &userID, &status)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Order might not have been flushed or was invalid
			log.Printf("[EXPIRATION WARNING] No order found in DB for expired reservation: %s\n", reservationID)
			return nil
		}
		return fmt.Errorf("failed to query order: %w", err)
	}

	// 2. Only rollback if the order is still in PENDING state
	if status != "PENDING" {
		log.Printf("[EXPIRATION INFO] Reservation %s status is '%s', skipping rollback.\n", reservationID, status)
		return nil
	}

	// 3. Update order status to EXPIRED
	_, err = tx.ExecContext(ctx,
		"UPDATE orders SET status = 'EXPIRED' WHERE id = $1 AND status = 'PENDING'",
		orderID,
	)
	if err != nil {
		return fmt.Errorf("failed to mark order as EXPIRED: %w", err)
	}

	// 4. Return stock to PostgreSQL (+1)
	_, err = tx.ExecContext(ctx,
		"UPDATE products SET stock = stock + 1 WHERE id = $1",
		productID,
	)
	if err != nil {
		return fmt.Errorf("failed to restore stock in DB: %w", err)
	}

	// 5. Insert OrderExpired event into Transactional Outbox
	eventID := uuid.New().String()
	payloadMap := map[string]any{
		"order_id":       orderID,
		"reservation_id": reservationID,
		"product_id":     productID,
		"user_id":        userID,
		"reason":         "RESERVATION_TTL_EXPIRED",
		"expired_at":     time.Now().UTC(),
	}
	payloadBytes, _ := json.Marshal(payloadMap)

	_, err = tx.ExecContext(ctx,
		"INSERT INTO outbox_events (id, aggregate_type, payload, status) VALUES ($1, $2, $3, $4)",
		eventID, "OrderExpired", string(payloadBytes), "PROCESSED",
	)
	if err != nil {
		return fmt.Errorf("failed to insert OrderExpired outbox event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit rollback tx: %w", err)
	}

	// 6. Return stock to Redis (+1) via atomic INCR
	newRedisStock, err := w.redis.IncrProductStock(ctx, productID)
	if err != nil {
		log.Printf("[REDIS ROLLBACK ERROR] Failed to INCR product %d stock in Redis: %v\n", productID, err)
	} else {
		log.Printf("[ROLLBACK SUCCESS] Restored 1 stock for product %d. New Redis stock: %d. Order %s marked EXPIRED.\n",
			productID, newRedisStock, reservationID)
	}

	return nil
}
