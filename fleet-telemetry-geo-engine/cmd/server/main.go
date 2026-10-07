package main

import (
	"flag"
	"log"
	"time"

	"geopulse/internal/buffer"
	"geopulse/internal/persistence"
	"geopulse/internal/server"
	"geopulse/internal/spatial"
)

func main() {
	port := flag.Int("port", 50051, "Port to listen on")
	metricsPort := flag.Int("metrics-port", 2112, "HTTP port to expose Prometheus /metrics endpoint (0 to disable)")
	bufferCap := flag.Int("buffer-cap", 10000, "Ring buffer capacity for ingestion backpressure")
	numWorkers := flag.Int("workers", 4, "Number of concurrent ingestion workers")

	// Redis Hot-Path CLI flags
	enableRedis := flag.Bool("enable-redis", true, "Enable Redis hot-path spatial indexing & pub/sub")
	redisAddr := flag.String("redis-addr", "localhost:6379", "Redis host:port address")
	redisPass := flag.String("redis-pass", "", "Redis password")
	redisDB := flag.Int("redis-db", 0, "Redis database index")
	staleThreshold := flag.Duration("stale-threshold", 30*time.Second, "Inactive duration before driver is purged as ghost")
	sweepInterval := flag.Duration("sweep-interval", 5*time.Second, "Interval between ghost driver sweep cycles")

	// PostgreSQL Cold-Path Batch Persistence CLI flags
	enablePostgres := flag.Bool("enable-postgres", true, "Enable PostgreSQL cold-path batch persistence")
	pgHost := flag.String("pg-host", "localhost", "PostgreSQL host")
	pgPort := flag.Int("pg-port", 5432, "PostgreSQL port")
	pgUser := flag.String("pg-user", "fatih", "PostgreSQL username")
	pgPass := flag.String("pg-pass", "", "PostgreSQL password")
	pgDB := flag.String("pg-db", "geopulse", "PostgreSQL database name")
	pgSSL := flag.String("pg-ssl", "disable", "PostgreSQL SSL mode (disable, require)")
	batchSize := flag.Int("batch-size", 250, "Batch count flush threshold (size-based flush rule)")
	batchInterval := flag.Duration("batch-interval", 3*time.Second, "Batch timer flush threshold (time-based flush rule)")
	bulkMode := flag.String("bulk-mode", "copy", "Bulk insertion mode (copy or multi_insert)")

	flag.Parse()

	redisCfg := spatial.DefaultRedisConfig()
	redisCfg.Addr = *redisAddr
	redisCfg.Password = *redisPass
	redisCfg.DB = *redisDB
	redisCfg.StaleThreshold = *staleThreshold
	redisCfg.SweepInterval = *sweepInterval

	pgCfg := persistence.DefaultPostgresConfig()
	pgCfg.Host = *pgHost
	pgCfg.Port = *pgPort
	pgCfg.User = *pgUser
	pgCfg.Password = *pgPass
	pgCfg.Database = *pgDB
	pgCfg.SSLMode = *pgSSL

	batchCfg := persistence.DefaultBatchConfig()
	batchCfg.BatchSize = *batchSize
	batchCfg.FlushInterval = *batchInterval
	if *bulkMode == "multi_insert" {
		batchCfg.BulkMode = persistence.BulkModeMultiInsert
	} else {
		batchCfg.BulkMode = persistence.BulkModeCopy
	}

	cfg := server.Config{
		Port:           *port,
		MetricsPort:    *metricsPort,
		BufferCapacity: *bufferCap,
		NumWorkers:     *numWorkers,
		DropStrategy:   buffer.DropOldest,
		EnableRedis:    *enableRedis,
		RedisCfg:       redisCfg,
		EnablePostgres: *enablePostgres,
		PostgresCfg:    pgCfg,
		BatchCfg:       batchCfg,
	}

	if err := server.Run(cfg, nil); err != nil {
		log.Fatalf("Server exited with error: %v", err)
	}
}
