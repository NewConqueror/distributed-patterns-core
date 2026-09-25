package pipeline

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/elasticsearch"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/model"
)

type Batcher struct {
	batchSize     int
	flushInterval time.Duration
	esClient      *elasticsearch.Client
	kafkaReader   *kafka.Reader

	itemChan chan model.BulkItem
	mu       sync.Mutex
	buffer   []model.BulkItem
}

func NewBatcher(
	batchSize int,
	flushInterval time.Duration,
	esClient *elasticsearch.Client,
	kafkaReader *kafka.Reader,
) *Batcher {
	return &Batcher{
		batchSize:     batchSize,
		flushInterval: flushInterval,
		esClient:      esClient,
		kafkaReader:   kafkaReader,
		itemChan:      make(chan model.BulkItem, batchSize*2),
		buffer:        make([]model.BulkItem, 0, batchSize),
	}
}

// Push adds an item to the incoming channel
func (b *Batcher) Push(item model.BulkItem) {
	b.itemChan <- item
}

// Start runs the batching loop with size and time triggers
func (b *Batcher) Start(ctx context.Context) error {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	log.Printf("[Batcher] Starting micro-batching engine (BatchSize: %d, FlushInterval: %v)", b.batchSize, b.flushInterval)

	for {
		select {
		case <-ctx.Done():
			log.Println("[Batcher] Shutdown signal received, flushing remaining buffered events...")
			b.flush(context.Background())
			return nil

		case item, ok := <-b.itemChan:
			if !ok {
				// Channel closed
				b.flush(context.Background())
				return nil
			}

			b.buffer = append(b.buffer, item)
			// Trigger 1: Size Limit Reached
			if len(b.buffer) >= b.batchSize {
				if err := b.flush(ctx); err != nil {
					log.Printf("[Batcher Error] Size trigger flush failed: %v", err)
				}
			}

		case <-ticker.C:
			// Trigger 2: Time Limit (Flush Interval) Reached
			if len(b.buffer) > 0 {
				if err := b.flush(ctx); err != nil {
					log.Printf("[Batcher Error] Time trigger flush failed: %v", err)
				}
			}
		}
	}
}

// flush sends accumulated items to Elasticsearch /_bulk and commits Kafka offsets on success
func (b *Batcher) flush(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	count := len(b.buffer)
	if count == 0 {
		return nil
	}

	itemsToFlush := b.buffer
	b.buffer = make([]model.BulkItem, 0, b.batchSize)

	// Filter out tombstones that have no document to delete (empty version/id) if any
	var validBulkItems []model.BulkItem
	var kafkaMsgs []kafka.Message

	for _, item := range itemsToFlush {
		kafkaMsgs = append(kafkaMsgs, item.KafkaMsg)
		// Only send to ES if it's not a redundant empty tombstone
		if item.IsTombstone && item.ID == "" {
			continue
		}
		validBulkItems = append(validBulkItems, item)
	}

	start := time.Now()

	// 1. Send Bulk to Elasticsearch (if there are items to index/delete)
	if len(validBulkItems) > 0 {
		ndjson, err := elasticsearch.BuildNDJSON(validBulkItems)
		if err != nil {
			// Put items back into buffer to prevent data loss
			b.buffer = append(itemsToFlush, b.buffer...)
			return err
		}

		resp, err := b.esClient.Bulk(ctx, ndjson)
		if err != nil {
			// Do NOT commit Kafka messages; put items back
			b.buffer = append(itemsToFlush, b.buffer...)
			return err
		}

		elapsed := time.Since(start)
		log.Printf("[ES Bulk Success] Indexed/Deleted %d documents in %v (ES Took: %dms, HasErrors: %v)",
			len(validBulkItems), elapsed, resp.Took, resp.Errors)
	}

	// 2. Manual Commit to Kafka (At-Least-Once Delivery Guarantee)
	// Kafka messages are only committed AFTER Elasticsearch has accepted the batch!
	if len(kafkaMsgs) > 0 {
		commitStart := time.Now()
		if err := b.kafkaReader.CommitMessages(ctx, kafkaMsgs...); err != nil {
			log.Printf("[Kafka Commit Error] Failed to commit %d offsets: %v", len(kafkaMsgs), err)
			return err
		}
		log.Printf("[Kafka Commit] Successfully committed %d message offsets in %v", len(kafkaMsgs), time.Since(commitStart))
	}

	return nil
}
