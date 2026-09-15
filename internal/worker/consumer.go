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

				// If batch threshold reached (100 items), flush immediately without waiting for 50ms ticker!
				if len(batch) >= c.batchSize {
					if err := c.flushBatch(ctx, batch); err != nil {
						log.Printf("[BATCH CONSUMER ERROR]: %v\n", err)
					} else {
						batch = batch[:0]
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

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin batch tx: %w", err)
	}
	defer tx.Rollback()

	// 1. Multi-row batch INSERT INTO orders
	orderValues := make([]string, 0, len(batch))
	orderArgs := make([]any, 0, len(batch)*3)
	argIdx := 1

	productCounts := make(map[int]int)

	for _, event := range batch {
		orderValues = append(orderValues, fmt.Sprintf("($%d, $%d, $%d)", argIdx, argIdx+1, argIdx+2))
		orderArgs = append(orderArgs, event.ProductID, event.UserID, event.CreatedAt)
		argIdx += 3

		productCounts[event.ProductID]++
	}

	orderQuery := fmt.Sprintf("INSERT INTO orders (product_id, user_id, created_at) VALUES %s",
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
	outboxValues := make([]string, 0, len(batch))
	outboxArgs := make([]any, 0, len(batch)*4)
	outboxArgIdx := 1

	var kafkaMessages []kafka.Message

	for _, event := range batch {
		payloadMap := map[string]any{
			"reservation_id": event.ReservationID,
			"product_id":     event.ProductID,
			"user_id":        event.UserID,
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

	outboxQuery := fmt.Sprintf("INSERT INTO outbox_events (id, aggregate_type, payload, status) VALUES %s",
		strings.Join(outboxValues, ", "))
	if _, err := tx.ExecContext(ctx, outboxQuery, outboxArgs...); err != nil {
		return fmt.Errorf("failed to execute batch insert outbox_events: %w", err)
	}

	// Commit batch transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit batch tx: %w", err)
	}

	// 4. If Kafka writer configured, publish messages to Kafka topic
	if c.kafkaWriter != nil && len(kafkaMessages) > 0 {
		go func(msgs []kafka.Message) {
			if err := c.kafkaWriter.WriteMessages(context.Background(), msgs...); err != nil {
				log.Printf("[KAFKA ERROR]: Failed to publish %d messages: %v\n", len(msgs), err)
			}
		}(kafkaMessages)
	}

	log.Printf("[BATCH CONSUMER] Successfully flushed %d orders into PostgreSQL in %v (1 single query)\n",
		len(batch), time.Since(start))

	return nil
}
