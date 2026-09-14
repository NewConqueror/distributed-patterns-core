package database

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"flashsale/internal/config"
	_ "github.com/lib/pq"
)

func NewPostgresDB(cfg *config.Config) (*sql.DB, error) {
	dsn := cfg.GetDSN()
	log.Printf("[DB] Connecting to database: %s\n", dsn)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database driver: %w", err)
	}

	if cfg.DBMaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.DBMaxOpenConns)
		log.Printf("[DB] MaxOpenConns set to %d\n", cfg.DBMaxOpenConns)
	} else {
		log.Println("[DB] MaxOpenConns is unlimited (default)")
	}

	if cfg.DBMaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.DBMaxIdleConns)
	}
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Println("[DB] Successfully connected to PostgreSQL.")
	return db, nil
}
