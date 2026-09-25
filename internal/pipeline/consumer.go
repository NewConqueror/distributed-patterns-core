package pipeline

import (
	"context"
	"errors"
	"io"
	"log"

	"github.com/segmentio/kafka-go"

	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/circuitbreaker"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/config"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/dlq"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/transformer"
)

type Consumer struct {
	reader      *kafka.Reader
	transformer *transformer.Transformer
	batcher     *Batcher
	dlqProducer dlq.Producer
	cb          *circuitbreaker.CircuitBreaker
}

func NewConsumer(
	cfg *config.Config,
	transformer *transformer.Transformer,
	batcher *Batcher,
	reader *kafka.Reader,
	dlqProducer dlq.Producer,
	cb *circuitbreaker.CircuitBreaker,
) *Consumer {
	return &Consumer{
		reader:      reader,
		transformer: transformer,
		batcher:     batcher,
		dlqProducer: dlqProducer,
		cb:          cb,
	}
}

// CreateKafkaReader instantiates a segmentio/kafka-go Reader with manual commit configuration
func CreateKafkaReader(cfg *config.Config) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		GroupID:        cfg.KafkaGroupID,
		Topic:          cfg.KafkaTopic,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: 0,    // 0 = Manual commit mode (disable auto-commit for At-Least-Once Delivery)
		StartOffset:    kafka.FirstOffset,
	})
}

// Start listens to Kafka topic, transforms incoming messages, and pushes them to the batching engine
func (c *Consumer) Start(ctx context.Context) error {
	log.Printf("[Kafka Consumer] Listening on topic '%s' with GroupID '%s' (Manual Commit: ON)...",
		c.reader.Config().Topic, c.reader.Config().GroupID)

	for {
		select {
		case <-ctx.Done():
			log.Println("[Kafka Consumer] Context cancelled, shutting down fetch loop...")
			return nil
		default:
			// 1. Circuit Breaker Guard: If Elasticsearch is down/unhealthy, pause consumer fetch loop
			if err := c.cb.WaitIfOpen(ctx); err != nil {
				return nil
			}

			// 2. FetchMessage retrieves the next message WITHOUT auto-committing the offset
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
					return nil
				}
				log.Printf("[Kafka Fetch Error] Error fetching message: %v", err)
				continue
			}

			// 3. Transform Debezium event into Elasticsearch BulkItem
			bulkItem, err := c.transformer.Transform(msg)
			if err != nil {
				// Permanent Poison Pill detected: Send to DLQ and advance offset to avoid pipeline deadlock
				log.Printf("[Poison Pill] Malformed CDC message detected (Partition: %d, Offset: %d): %v",
					msg.Partition, msg.Offset, err)

				if dlqErr := c.dlqProducer.Publish(ctx, msg, dlq.ErrorTypeMalformedJSON, err.Error()); dlqErr != nil {
					log.Printf("[DLQ Error] Failed to publish poison message to DLQ: %v", dlqErr)
				}

				// Crucial: Commit offset so the consumer does not crash-loop on the bad message!
				if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
					log.Printf("[Commit Warning] Failed to commit poison pill offset: %v", commitErr)
				}
				continue
			}

			// 4. Push to micro-batching buffer
			c.batcher.Push(*bulkItem)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
