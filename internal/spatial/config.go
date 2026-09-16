package spatial

import (
	"time"
)

// RedisConfig defines connection pooling, timeouts, retry policies,
// and key namespace configurations for the Redis spatial layer.
type RedisConfig struct {
	// Connection Settings
	Addr     string
	Password string
	DB       int

	// Connection Pool Settings
	PoolSize     int
	MinIdleConns int

	// Timeouts
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// Retry Policy
	MaxRetries      int
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration

	// Key Namespaces
	ActiveKey           string // Sorted Set key for GEOADD (default: "drivers:active")
	HeartbeatKey        string // Sorted Set key for timestamp tracking (default: "drivers:heartbeat")
	MetaKeyPrefix       string // Hash prefix for driver metadata (default: "driver:meta:")
	PubSubChannelPrefix string // Prefix for regional fan-out (default: "driver.updates.")

	// Stale Cleanup / TTL Settings
	StaleThreshold time.Duration // Inactivity threshold before driver is deemed stale (default: 30s)
	SweepInterval  time.Duration // Frequency of background sweeper runs (default: 5s)
}

// DefaultRedisConfig provides production-grade defaults for GeoPulse.
func DefaultRedisConfig() RedisConfig {
	return RedisConfig{
		Addr:                "localhost:6379",
		Password:            "",
		DB:                  0,
		PoolSize:            50,
		MinIdleConns:        10,
		DialTimeout:         5 * time.Second,
		ReadTimeout:         3 * time.Second,
		WriteTimeout:        3 * time.Second,
		MaxRetries:          3,
		MinRetryBackoff:     100 * time.Millisecond,
		MaxRetryBackoff:     500 * time.Millisecond,
		ActiveKey:           "drivers:active",
		HeartbeatKey:        "drivers:heartbeat",
		MetaKeyPrefix:       "driver:meta:",
		PubSubChannelPrefix: "driver.updates.",
		StaleThreshold:      30 * time.Second,
		SweepInterval:       5 * time.Second,
	}
}
