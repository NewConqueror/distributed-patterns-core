package main

import (
	"flag"
	"log"
	"time"

	"geopulse/internal/buffer"
	"geopulse/internal/server"
	"geopulse/internal/spatial"
)

func main() {
	port := flag.Int("port", 50051, "Port to listen on")
	bufferCap := flag.Int("buffer-cap", 10000, "Ring buffer capacity for ingestion backpressure")
	numWorkers := flag.Int("workers", 4, "Number of concurrent ingestion workers")

	// Redis CLI flags
	enableRedis := flag.Bool("enable-redis", true, "Enable Redis hot-path spatial indexing & pub/sub")
	redisAddr := flag.String("redis-addr", "localhost:6379", "Redis host:port address")
	redisPass := flag.String("redis-pass", "", "Redis password")
	redisDB := flag.Int("redis-db", 0, "Redis database index")
	staleThreshold := flag.Duration("stale-threshold", 30*time.Second, "Inactive duration before driver is purged as ghost")
	sweepInterval := flag.Duration("sweep-interval", 5*time.Second, "Interval between ghost driver sweep cycles")

	flag.Parse()

	redisCfg := spatial.DefaultRedisConfig()
	redisCfg.Addr = *redisAddr
	redisCfg.Password = *redisPass
	redisCfg.DB = *redisDB
	redisCfg.StaleThreshold = *staleThreshold
	redisCfg.SweepInterval = *sweepInterval

	cfg := server.Config{
		Port:           *port,
		BufferCapacity: *bufferCap,
		NumWorkers:     *numWorkers,
		DropStrategy:   buffer.DropOldest,
		EnableRedis:    *enableRedis,
		RedisCfg:       redisCfg,
	}

	if err := server.Run(cfg, nil); err != nil {
		log.Fatalf("Server exited with error: %v", err)
	}
}
