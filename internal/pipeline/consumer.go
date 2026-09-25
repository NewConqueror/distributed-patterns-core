package pipeline

import (
	"context"
	"errors"
	"io"
	"log"

	"github.com/segmentio/kafka-go"

	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/config"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/transformer"
)

type Consumer struct {
	reader      *kafka.Reader
	transformer *transformer.Transformer
	batcher     *Batcher
}

func NewConsumer(
	cfg *config.Config,
	transformer *transformer.Transformer,
	batcher *Batcher,
	reader *kafka.Reader,
) *Consumer {
	return &Consumer{
		reader:      reader,
		transformer: transformer,
		batcher:     batcher,
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
			// FetchMessage retrieves the next message WITHOUT auto-committing the offset
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
					return nil
				}
				log.Printf("[Kafka Fetch Error] Error fetching message: %v", err)
				continue
			}

			// Transform Debezium event into Elasticsearch BulkItem
			bulkItem, err := c.transformer.Transform(msg)
			if err != nil {
				log.Printf("[Transform Warning] Skipping malformed message (Partition: %d, Offset: %d): %v",
					msg.Partition, msg.Offset, err)
				// Still commit skipped messages to avoid poisonous message loops
				_ = c.reader.CommitMessages(ctx, msg)
				continue
			}

			// Push to micro-batching buffer
			c.batcher.Push(*bulkItem)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
