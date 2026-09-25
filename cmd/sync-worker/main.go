package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/circuitbreaker"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/config"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/dlq"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/elasticsearch"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/pipeline"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/transformer"
)

func main() {
	log.Println("==========================================================")
	log.Println(" Starting Real-Time CDC Go Sync Worker (Postgres -> ES)   ")
	log.Println(" Resilience: DLQ + Circuit Breaker + Graceful Shutdown    ")
	log.Println("==========================================================")

	cfg := config.LoadConfig()

	// 1. Elasticsearch Client & Index Verification
	esClient := elasticsearch.NewClient(cfg.ElasticsearchURL)
	ctxPing, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPing()

	if err := esClient.Ping(ctxPing); err != nil {
		log.Printf("[Warning] Elasticsearch initial ping failed (%v). Circuit breaker will manage retries...", err)
	} else {
		log.Println("[Elasticsearch] Connected successfully.")
	}

	if err := esClient.EnsureIndexWithAlias(context.Background(), cfg.ElasticsearchIndex, cfg.ElasticsearchSearchAlias); err != nil {
		log.Printf("[Warning] Could not ensure index '%s' with alias '%s': %v", cfg.ElasticsearchIndex, cfg.ElasticsearchSearchAlias, err)
	}

	// 2. Circuit Breaker Setup for Elasticsearch Health Tracking
	cb := circuitbreaker.NewCircuitBreaker(esClient, cfg.CBInitialBackoff, cfg.CBMaxBackoff)

	// 3. Dead Letter Queue (DLQ) Producer Setup
	dlqProducer := dlq.NewKafkaDLQProducer(cfg.KafkaBrokers, cfg.DLQTopic)
	defer func() {
		if err := dlqProducer.Close(); err != nil {
			log.Printf("[DLQ Close Error]: %v", err)
		}
	}()

	// 4. Transformer & In-Memory Category Lookup Cache
	categoryCache := transformer.NewCategoryCache()
	trans := transformer.NewTransformer(categoryCache, cfg.ElasticsearchIndex)

	// 5. Kafka Reader with Manual Commit
	kafkaReader := pipeline.CreateKafkaReader(cfg)
	defer func() {
		if err := kafkaReader.Close(); err != nil {
			log.Printf("[Kafka Reader Close Error]: %v", err)
		}
	}()

	// 6. Micro-batching Engine with Circuit Breaker and DLQ support
	batcher := pipeline.NewBatcher(cfg.BatchSize, cfg.FlushInterval, esClient, kafkaReader, dlqProducer, cb)

	// 7. Pipeline Consumer with Pause/Resume and Poison Pill isolation
	consumer := pipeline.NewConsumer(cfg, trans, batcher, kafkaReader, dlqProducer, cb)

	// 8. Graceful Shutdown Setup (SIGINT, SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	// Run Batcher in background
	go func() {
		if err := batcher.Start(ctx); err != nil {
			log.Printf("[Batcher Terminated]: %v", err)
		}
	}()

	// Run Consumer in background
	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Printf("[Consumer Terminated]: %v", err)
		}
	}()

	log.Printf("[Worker Active] Topic: '%s' -> Index: '%s' | DLQ: '%s' | Batch: %d (%v)",
		cfg.KafkaTopic, cfg.ElasticsearchIndex, cfg.DLQTopic, cfg.BatchSize, cfg.FlushInterval)

	// Wait for OS shutdown signal
	sig := <-sigChan
	log.Printf("\n[Graceful Shutdown] Signal '%v' received. Shutting down pipeline...", sig)

	// 1. Stop Kafka consumer from pulling new messages
	cancel()

	// 2. Perform a final flush of any remaining in-memory buffered events
	log.Println("[Graceful Shutdown] Flushing remaining buffered events to Elasticsearch...")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()

	if err := batcher.FlushPublic(shutdownCtx); err != nil {
		log.Printf("[Graceful Shutdown Warning] Final flush reported error: %v", err)
	} else {
		log.Println("[Graceful Shutdown] Final buffer flush and offset commit completed successfully.")
	}

	// 3. Grace period for clean connection teardown
	time.Sleep(500 * time.Millisecond)
	log.Println("[Graceful Shutdown] All components stopped cleanly. Goodbye!")
}
