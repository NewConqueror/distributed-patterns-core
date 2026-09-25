package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// Kafka
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string

	// Elasticsearch
	ElasticsearchURL         string
	ElasticsearchIndex       string
	ElasticsearchSearchAlias string

	// Micro-batching & Buffering
	BatchSize     int
	FlushInterval time.Duration

	// DLQ (Dead Letter Queue)
	DLQTopic string

	// Circuit Breaker & Backoff
	CBInitialBackoff time.Duration
	CBMaxBackoff     time.Duration
}

func LoadConfig() *Config {
	return &Config{
		KafkaBrokers:             getEnvSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
		KafkaTopic:               getEnv("KAFKA_TOPIC", "pg_cdc.public.products"),
		KafkaGroupID:             getEnv("KAFKA_GROUP_ID", "es-products-sync-group"),
		ElasticsearchURL:         getEnv("ELASTICSEARCH_URL", "http://localhost:9200"),
		ElasticsearchIndex:       getEnv("ELASTICSEARCH_INDEX", "products_v1"),
		ElasticsearchSearchAlias: getEnv("ELASTICSEARCH_SEARCH_ALIAS", "products_search_alias"),
		BatchSize:                getEnvInt("BATCH_SIZE", 500),
		FlushInterval:            time.Duration(getEnvInt("FLUSH_INTERVAL_MS", 200)) * time.Millisecond,
		DLQTopic:                 getEnv("DLQ_TOPIC", "cdc.products.dlq"),
		CBInitialBackoff:         time.Duration(getEnvInt("CB_INITIAL_BACKOFF_SEC", 1)) * time.Second,
		CBMaxBackoff:             time.Duration(getEnvInt("CB_MAX_BACKOFF_SEC", 16)) * time.Second,
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			return intVal
		}
	}
	return defaultVal
}

func getEnvSlice(key string, defaultVal []string) []string {
	if val := os.Getenv(key); val != "" {
		parts := strings.Split(val, ",")
		var cleaned []string
		for _, p := range parts {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				cleaned = append(cleaned, trimmed)
			}
		}
		if len(cleaned) > 0 {
			return cleaned
		}
	}
	return defaultVal
}
