package cache

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisConfig holds connection parameters for Redis.
type RedisConfig struct {
	Host     string
	Port     int
	Password string
	DB       int
}

// DefaultRedisConfig returns default connection settings.
func DefaultRedisConfig() RedisConfig {
	host := "localhost"
	if h := os.Getenv("REDIS_HOST"); h != "" {
		host = h
	}

	port := 6379
	if p := os.Getenv("REDIS_PORT"); p != "" {
		if pInt, err := strconv.Atoi(p); err == nil {
			port = pInt
		}
	}

	db := 0
	if d := os.Getenv("REDIS_DB"); d != "" {
		if dInt, err := strconv.Atoi(d); err == nil {
			db = dInt
		}
	}

	return RedisConfig{
		Host:     host,
		Port:     port,
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       db,
	}
}

// NewRedisClient creates and validates a connection to Redis.
func NewRedisClient(ctx context.Context, cfg RedisConfig) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to ping redis: %w", err)
	}

	return client, nil
}
