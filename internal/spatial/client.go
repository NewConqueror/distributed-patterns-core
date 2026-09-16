package spatial

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// NewRedisClient initializes a Redis client with configured connection pooling,
// timeouts, and exponential retry policies, validating connectivity with Ping.
func NewRedisClient(cfg RedisConfig, logger *slog.Logger) (*redis.Client, error) {
	if logger == nil {
		logger = slog.Default()
	}

	opts := &redis.Options{
		Addr:            cfg.Addr,
		Password:        cfg.Password,
		DB:              cfg.DB,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		DialTimeout:     cfg.DialTimeout,
		ReadTimeout:     cfg.ReadTimeout,
		WriteTimeout:    cfg.WriteTimeout,
		MaxRetries:      cfg.MaxRetries,
		MinRetryBackoff: cfg.MinRetryBackoff,
		MaxRetryBackoff: cfg.MaxRetryBackoff,
	}

	client := redis.NewClient(opts)

	// Validate connection pool readiness with ping and timeout
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	start := time.Now()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to ping Redis at %s: %w", cfg.Addr, err)
	}

	logger.Info("Connected to Redis instance",
		"addr", cfg.Addr,
		"db", cfg.DB,
		"pool_size", cfg.PoolSize,
		"min_idle_conns", cfg.MinIdleConns,
		"latency", time.Since(start),
	)

	return client, nil
}
