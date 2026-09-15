package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"flashsale/internal/config"
	"flashsale/internal/database"
	"flashsale/internal/metrics"
	"github.com/segmentio/kafka-go"
)

type BatchConsumer struct {
	db          *sql.DB
	redis       *database.RedisService
	kafkaWriter *kafka.Writer
	stopChan    chan struct{}
	wg          sync.WaitGroup

	batchSize     int
	flushInterval time.Duration
}

func NewBatchConsumer(cfg *config.Config, db *sql.DB, redisSvc *database.RedisService) *BatchConsumer {
	var kw *kafka.Writer
	if cfg.KafkaBrokers != "" {
		kw = &kafka.Writer{
			Addr:     kafka.TCP(strings.Split(cfg.KafkaBrokers, ",")...),
			Topic:    cfg.KafkaTopic,
			Balancer: &kafka.LeastBytes{},
		}
		log.Printf("[KAFKA] Initialized writer for topic %s on %s\n", cfg.KafkaTopic, cfg.KafkaBrokers)
	}

	return &BatchConsumer{
		db:            db,
		redis:         redisSvc,
		kafkaWriter:   kw,
		stopChan:      make(chan struct{}),
		batchSize:     100,
		flushInterval: 50 * time.Millisecond,
	}
}

func (c *BatchConsumer) Start(ctx context.Context) {
	c.wg.Add(1)
	go c.run(ctx)
	log.Println("[BATCH CONSUMER] Order reservation consumer worker started (100 items / 50ms flush).")
}

func (c *BatchConsumer) Stop() {
	close(c.stopChan)
	c.wg.Wait()
	if c.kafkaWriter != nil {
		_ = c.kafkaWriter.Close()
	}
	log.Println("[BATCH CONSUMER] Consumer worker stopped cleanly.")
}

func (c *BatchConsumer) run(ctx context.Context) {
	defer c.wg.Done()

	lastID := "0-0"
	var batch []database.ReservationEvent
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			// Flush any remaining buffered items before exit
			if len(batch) > 0 {
				_ = c.flushBatch(context.Background(), batch)
			}
			return

		case <-ticker.C:
			if len(batch) > 0 {
				if err := c.flushBatch(ctx, batch); err != nil {
					log.Printf("[BATCH CONSUMER ERROR]: %v\n", err)
				} else {
					batch = batch[:0]
					metrics.KafkaConsumerLag.Set(0)
				}
			}

		default:
			// Read up to batchSize items from Redis Stream with 20ms block timeout
			events, newLastID, err := c.redis.ReadStreamBatch(ctx, lastID, int64(c.batchSize-len(batch)), 20*time.Millisecond)
			if err != nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}

			if len(events) > 0 {
				lastID = newLastID
				batch = append(batch, events...)
				metrics.KafkaConsumerLag.Set(float64(len(batch)))

				// If batch threshold reached (100 items), flush immediately without waiting for 50ms ticker!
				if len(batch) >= c.batchSize {
					if err := c.flushBatch(ctx, batch); err != nil {
						log.Printf("[BATCH CONSUMER ERROR]: %v\n", err)
					} else {
						batch = batch[:0]
						metrics.KafkaConsumerLag.Set(0)
					}
				}
			}
		}
	}
}

// flushBatch writes all buffered reservations into PostgreSQL in ONE single transaction,
// reducing connection pressure and database CPU load to near zero.
func (c *BatchConsumer) flushBatch(ctx context.Context, batch []database.ReservationEvent) error {
	if len(batch) == 0 {
		return nil
	}

	start := time.Now()

	// -------------------------------------------------------------------------
	// Tier 1 Idempotency Defense: Redis In-Memory SETNX (60s TTL)
	// Eliminates duplicates caused by network timeouts & At-Least-Once delivery
	// -------------------------------------------------------------------------
	var uniqueEvents []database.ReservationEvent
	for _, event := range batch {
		acquired, err := c.redis.TryAcquireIdempotencyLock(ctx, event.ReservationID, 60*time.Second)
		if err != nil {
			log.Printf("[IDEMPOTENCY ERROR]: Failed to check lock for %s: %v\n", event.ReservationID, err)
		}
		if !acquired {
			log.Printf("[IDEMPOTENCY DEFENSE] Duplicate message detected for reservation_id=%s. Skipping insertion (At-Least-Once defense).\n", event.ReservationID)
			continue
		}
		uniqueEvents = append(uniqueEvents, event)
	}

	if len(uniqueEvents) == 0 {
		log.Println("[BATCH CONSUMER] All messages in batch were duplicates. Flushed 0 orders.")
		return nil
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin batch tx: %w", err)
	}
	defer tx.Rollback()

	// 1. Multi-row batch INSERT INTO orders with Tier 2 Idempotency (ON CONFLICT DO NOTHING)
	orderValues := make([]string, 0, len(uniqueEvents))
	orderArgs := make([]any, 0, len(uniqueEvents)*5)
	argIdx := 1

	productCounts := make(map[int]int)

	for _, event := range uniqueEvents {
		orderValues = append(orderValues, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)", argIdx, argIdx+1, argIdx+2, argIdx+3, argIdx+4))
		orderArgs = append(orderArgs, event.ReservationID, event.ProductID, event.UserID, "PENDING", event.CreatedAt)
		argIdx += 5

		productCounts[event.ProductID]++
	}

	orderQuery := fmt.Sprintf("INSERT INTO orders (reservation_id, product_id, user_id, status, created_at) VALUES %s ON CONFLICT (reservation_id) DO NOTHING",
		strings.Join(orderValues, ", "))
	if _, err := tx.ExecContext(ctx, orderQuery, orderArgs...); err != nil {
		return fmt.Errorf("failed to execute batch insert orders: %w", err)
	}

	// 2. Decrement products.stock in PostgreSQL by total items ordered per product
	for productID, count := range productCounts {
		_, err := tx.ExecContext(ctx, "UPDATE products SET stock = stock - $1 WHERE id = $2", count, productID)
		if err != nil {
			return fmt.Errorf("failed to update product stock: %w", err)
		}
	}

	// 3. Multi-row batch INSERT INTO outbox_events
	outboxValues := make([]string, 0, len(uniqueEvents))
	outboxArgs := make([]any, 0, len(uniqueEvents)*4)
	outboxArgIdx := 1

	var kafkaMessages []kafka.Message

	for _, event := range uniqueEvents {
		payloadMap := map[string]any{
			"reservation_id": event.ReservationID,
			"product_id":     event.ProductID,
			"user_id":        event.UserID,
			"status":         "PENDING",
			"created_at":     event.CreatedAt,
		}
		payloadBytes, _ := json.Marshal(payloadMap)

		outboxValues = append(outboxValues, fmt.Sprintf("($%d, $%d, $%d, $%d)",
			outboxArgIdx, outboxArgIdx+1, outboxArgIdx+2, outboxArgIdx+3))
		outboxArgs = append(outboxArgs, event.ReservationID, "OrderReservation", string(payloadBytes), "PROCESSED")
		outboxArgIdx += 4

		if c.kafkaWriter != nil {
			kafkaMessages = append(kafkaMessages, kafka.Message{
				Key:   []byte(event.ReservationID),
				Value: payloadBytes,
			})
		}
	}

	outboxQuery := fmt.Sprintf("INSERT INTO outbox_events (id, aggregate_type, payload, status) VALUES %s ON CONFLICT (id) DO NOTHING",
		strings.Join(outboxValues, ", "))
	if _, err := tx.ExecContext(ctx, outboxQuery, outboxArgs...); err != nil {
		return fmt.Errorf("failed to execute batch insert outbox_events: %w", err)
	}

	// Commit batch transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit batch tx: %w", err)
	}

	// 4. Mark idempotency locks as COMPLETED
	for _, event := range uniqueEvents {
		_ = c.redis.MarkIdempotencyCompleted(ctx, event.ReservationID)
	}

	// 5. If Kafka writer configured, publish messages to Kafka topic
	if c.kafkaWriter != nil && len(kafkaMessages) > 0 {
		go func(msgs []kafka.Message) {
			if err := c.kafkaWriter.WriteMessages(context.Background(), msgs...); err != nil {
				log.Printf("[KAFKA ERROR]: Failed to publish %d messages: %v\n", len(msgs), err)
			}
		}(kafkaMessages)
	}

	log.Printf("[BATCH CONSUMER] Successfully flushed %d orders into PostgreSQL in %v (1 single query)\n",
		len(uniqueEvents), time.Since(start))

	return nil
}
