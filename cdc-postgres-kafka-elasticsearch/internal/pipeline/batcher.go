package pipeline

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/circuitbreaker"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/dlq"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/elasticsearch"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/metrics"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/model"
)

type Batcher struct {
	batchSize     int
	flushInterval time.Duration
	esClient      *elasticsearch.Client
	kafkaReader   *kafka.Reader
	dlqProducer   dlq.Producer
	cb            *circuitbreaker.CircuitBreaker

	itemChan chan model.BulkItem
	mu       sync.Mutex
	buffer   []model.BulkItem
}

func NewBatcher(
	batchSize int,
	flushInterval time.Duration,
	esClient *elasticsearch.Client,
	kafkaReader *kafka.Reader,
	dlqProducer dlq.Producer,
	cb *circuitbreaker.CircuitBreaker,
) *Batcher {
	return &Batcher{
		batchSize:     batchSize,
		flushInterval: flushInterval,
		esClient:      esClient,
		kafkaReader:   kafkaReader,
		dlqProducer:   dlqProducer,
		cb:            cb,
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

// FlushPublic allows external callers (such as graceful shutdown) to force an immediate flush
func (b *Batcher) FlushPublic(ctx context.Context) error {
	return b.flush(ctx)
}

// flush sends accumulated items to Elasticsearch /_bulk and commits Kafka offsets on success
func (b *Batcher) flush(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	count := len(b.buffer)
	if count == 0 {
		return nil
	}

	// 0. Wait if Circuit Breaker is OPEN due to ongoing Elasticsearch outage
	if err := b.cb.WaitIfOpen(ctx); err != nil {
		return err
	}

	itemsToFlush := b.buffer
	b.buffer = make([]model.BulkItem, 0, b.batchSize)

	var validBulkItems []model.BulkItem
	var kafkaMsgs []kafka.Message

	for _, item := range itemsToFlush {
		kafkaMsgs = append(kafkaMsgs, item.KafkaMsg)
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

		bulkResult, err := b.esClient.Bulk(ctx, ndjson)
		if err != nil {
			// Handle Transient Errors -> Trip Circuit Breaker
			if elasticsearch.IsTransient(err) {
				b.cb.Trip(err)
			}
			// Do NOT commit Kafka messages; restore buffer
			b.buffer = append(itemsToFlush, b.buffer...)
			return err
		}

		elapsed := time.Since(start)
		log.Printf("[ES Bulk Success] Processed %d documents in %v (ES Took: %dms, HasErrors: %v)",
			len(validBulkItems), elapsed, bulkResult.Response.Took, bulkResult.Response.Errors)

		// Record Prometheus Metrics for Bulk Flush & Batch Size
		metrics.RecordBulkFlush(elapsed.Seconds())
		metrics.SetBatchSize(float64(len(validBulkItems)))

		// Calculate End-to-End Latency: T_indexed - T_wal_commit
		var totalLatencyMs int64
		var latencyCount int64
		now := time.Now()

		for _, item := range validBulkItems {
			if _, isFailed := bulkResult.PermanentMap[item.ID]; isFailed {
				metrics.RecordEventConsumed("failure")
				continue
			}
			metrics.RecordEventConsumed("success")

			if item.SourceTsMs > 0 {
				commitTime := time.UnixMilli(item.SourceTsMs)
				diff := now.Sub(commitTime)
				if diff > 0 {
					metrics.RecordE2ELatency(diff.Seconds())
					totalLatencyMs += diff.Milliseconds()
					latencyCount++
				}
			}
		}

		if latencyCount > 0 {
			avgLatency := totalLatencyMs / latencyCount
			log.Printf("[E2E Latency Benchmark] Average E2E Latency: %dms (Postgres WAL Commit -> ES Indexed) [n=%d]",
				avgLatency, latencyCount)
		}

		// 2. Handle Permanent Item Failures (Route to DLQ)
		if len(bulkResult.PermanentMap) > 0 {
			log.Printf("[DLQ Notice] Routing %d permanently failed items to DLQ...", len(bulkResult.PermanentMap))
			for _, item := range validBulkItems {
				if reason, isFailed := bulkResult.PermanentMap[item.ID]; isFailed {
					metrics.RecordDLQEvent(dlq.ErrorTypeMappingConflict)
					if dlqErr := b.dlqProducer.Publish(ctx, item.KafkaMsg, dlq.ErrorTypeMappingConflict, reason); dlqErr != nil {
						log.Printf("[DLQ Error] Failed to publish message %s to DLQ: %v", item.ID, dlqErr)
					}
				}
			}
		}
	}

	// 3. Manual Commit to Kafka (At-Least-Once Delivery Guarantee)
	// We commit the offsets of ALL processed items (including items sent to DLQ to advance the offset)
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
