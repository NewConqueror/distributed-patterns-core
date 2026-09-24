package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config, PostgreSQL bağlantı yapılandırması
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
}

// ConnectionString, pgxpool için standart dsn dizesi üretir.
func (c Config) ConnectionString() string {
	ssl := c.SSLMode
	if ssl == "" {
		ssl = "disable"
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Database, ssl)
}

// DefaultDockerConfig, docker-compose için varsayılan yapılandırma
func DefaultDockerConfig() Config {
	return Config{
		Host:     "localhost",
		Port:     5433,
		User:     "ledger_user",
		Password: "ledger_password",
		Database: "financial_ledger",
		SSLMode:  "disable",
	}
}

// NewPool, PostgreSQL bağlantı havuzunu (pool) ilklendirir ve ping atar.
func NewPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	connStr := cfg.ConnectionString()
	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("geçersiz veritabanı bağlantı dizesi: %w", err)
	}

	poolConfig.MaxConns = 25
	poolConfig.MinConns = 5
	poolConfig.MaxConnLifetime = 30 * time.Minute
	poolConfig.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("bağlantı havuzu oluşturulamadı: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("veritabanına bağlanılamadı (ping hatası): %w", err)
	}

	return pool, nil
}

// RunMigrations, verilen sql dosyasını veya migrations dizinindeki şemayı veritabanında çalıştırır.
func RunMigrations(ctx context.Context, pool *pgxpool.Pool, migrationPath string) error {
	content, err := os.ReadFile(migrationPath)
	if err != nil {
		// Eğer mutlak yol bulunamazsa çalışma dizinine göre ara
		cleaned := filepath.Clean(migrationPath)
		content, err = os.ReadFile(cleaned)
		if err != nil {
			return fmt.Errorf("migration dosyası okunamadı: %w", err)
		}
	}

	sqlScript := string(content)
	// Yorum satırlarını veya boşlukları temizleyerek topluca çalıştır
	if strings.TrimSpace(sqlScript) == "" {
		return nil
	}

	_, err = pool.Exec(ctx, sqlScript)
	if err != nil {
		return fmt.Errorf("migration çalıştırılırken hata: %w", err)
	}

	return nil
}
