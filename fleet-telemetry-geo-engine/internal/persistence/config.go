package persistence

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// BulkInsertMode defines the bulk insertion mechanism into PostgreSQL.
type BulkInsertMode string

const (
	// BulkModeCopy uses PostgreSQL high-speed streaming COPY protocol (fastest).
	BulkModeCopy BulkInsertMode = "copy"
	// BulkModeMultiInsert uses parameterized multi-row INSERT INTO ... VALUES (...), (...)
	BulkModeMultiInsert BulkInsertMode = "multi_insert"
)

// PostgresConfig encapsulates PostgreSQL connection pooling and credential parameters.
type PostgresConfig struct {
	Host            string
	Port            int
	User            string
	Password        string
	Database        string
	SSLMode         string // e.g. "disable", "require"
	MaxConns        int32
	MinConns        int32
	MaxConnIdleTime time.Duration
	MaxConnLifetime time.Duration
	ConnectTimeout  time.Duration
}

// ConnString formats the PostgreSQL connection URI.
func (c PostgresConfig) ConnString() string {
	host := c.Host
	if strings.HasPrefix(host, "/") {
		return fmt.Sprintf("postgres://%s@/%s?host=%s", c.User, c.Database, host)
	}

	// Auto-detect local unix socket if no password provided and socket exists
	if c.Password == "" && (host == "localhost" || host == "127.0.0.1") {
		if _, err := os.Stat("/var/run/postgresql/.s.PGSQL.5432"); err == nil {
			return fmt.Sprintf("postgres://%s@/%s?host=/var/run/postgresql", c.User, c.Database)
		}
	}

	if c.Password != "" {
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s&connect_timeout=%d",
			c.User, c.Password, host, c.Port, c.Database, c.SSLMode, int(c.ConnectTimeout.Seconds()))
	}
	return fmt.Sprintf("postgres://%s@%s:%d/%s?sslmode=%s&connect_timeout=%d",
		c.User, host, c.Port, c.Database, c.SSLMode, int(c.ConnectTimeout.Seconds()))
}

// DefaultPostgresConfig returns standard production defaults for PostgreSQL.
func DefaultPostgresConfig() PostgresConfig {
	return PostgresConfig{
		Host:            "localhost",
		Port:            5432,
		User:            "fatih",
		Password:        "",
		Database:        "geopulse",
		SSLMode:         "disable",
		MaxConns:        20,
		MinConns:        4,
		MaxConnIdleTime: 5 * time.Minute,
		MaxConnLifetime: 30 * time.Minute,
		ConnectTimeout:  5 * time.Second,
	}
}

// BatchConfig defines flush thresholds and worker parameters for the in-memory batch buffer.
type BatchConfig struct {
	// Size-based flush trigger: Number of records before an immediate flush occurs (default: 250)
	BatchSize int

	// Time-based flush trigger: Flush interval if batch size has not been met (default: 3s)
	FlushInterval time.Duration

	// Queue depth for decoupled async flusher worker
	QueueCapacity int

	// Bulk insertion strategy: "copy" or "multi_insert"
	BulkMode BulkInsertMode
}

// DefaultBatchConfig returns recommended settings according to GeoPulse specifications.
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		BatchSize:     250,
		FlushInterval: 3 * time.Second,
		QueueCapacity: 64,
		BulkMode:      BulkModeCopy,
	}
}
