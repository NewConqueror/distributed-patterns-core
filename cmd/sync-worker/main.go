package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/config"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/elasticsearch"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/pipeline"
	"Real-Time_CDCPipeline_SyncingPostgreSQL_WALtoElasticsearch_viaKafkaDebezium/internal/transformer"
)

func main() {
	log.Println("==========================================================")
	log.Println(" Starting Real-Time CDC Go Sync Worker (Postgres -> ES)   ")
	log.Println("==========================================================")

	cfg := config.LoadConfig()

	// 1. Elasticsearch Client & Index Verification
	esClient := elasticsearch.NewClient(cfg.ElasticsearchURL)
	ctxPing, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPing()

	if err := esClient.Ping(ctxPing); err != nil {
		log.Printf("[Warning] Elasticsearch ping failed (%v). Will retry during execution...", err)
	} else {
		log.Println("[Elasticsearch] Connected successfully.")
	}

	if err := esClient.EnsureIndex(context.Background(), cfg.ElasticsearchIndex); err != nil {
		log.Printf("[Warning] Could not ensure index '%s': %v", cfg.ElasticsearchIndex, err)
	}

	// 2. Transformer & In-Memory Category Lookup Cache
	categoryCache := transformer.NewCategoryCache()
	trans := transformer.NewTransformer(categoryCache, cfg.ElasticsearchIndex)

	// 3. Kafka Reader with Manual Commit
	kafkaReader := pipeline.CreateKafkaReader(cfg)
	defer func() {
		if err := kafkaReader.Close(); err != nil {
			log.Printf("[Kafka Reader Close Error]: %v", err)
		}
	}()

	// 4. Micro-batching Engine
	batcher := pipeline.NewBatcher(cfg.BatchSize, cfg.FlushInterval, esClient, kafkaReader)

	// 5. Pipeline Consumer
	consumer := pipeline.NewConsumer(cfg, trans, batcher, kafkaReader)

	// 6. Graceful Shutdown Setup
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

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

	log.Printf("[Worker Ready] Consuming from '%s' -> Syncing to '%s' (BatchSize: %d, FlushInterval: %v)",
		cfg.KafkaTopic, cfg.ElasticsearchIndex, cfg.BatchSize, cfg.FlushInterval)

	<-sigChan
	log.Println("\n[Shutdown] Termination signal received. Performing graceful shutdown...")

	// Cancel context to trigger flush and stop reading
	cancel()

	// Give a grace period for in-flight flushes to complete
	time.Sleep(1 * time.Second)
	log.Println("[Shutdown] Go Sync Worker gracefully stopped. Goodbye!")
}
