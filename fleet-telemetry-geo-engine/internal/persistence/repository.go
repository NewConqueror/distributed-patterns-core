package persistence

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Repository defines the contract for persisting telemetry batches to PostgreSQL.
type Repository interface {
	InitSchema(ctx context.Context) error
	BulkInsert(ctx context.Context, records []*TelemetryRecord) error
	Close()
}

// PostgresRepository manages connection pooling and bulk persistence to PostgreSQL + PostGIS.
type PostgresRepository struct {
	pool       *pgxpool.Pool
	cfg        PostgresConfig
	batchCfg   BatchConfig
	hasPostGIS bool
	logger     *slog.Logger
}

// NewPostgresRepository initializes a connection pool to PostgreSQL.
func NewPostgresRepository(cfg PostgresConfig, batchCfg BatchConfig, logger *slog.Logger) (*PostgresRepository, error) {
	if logger == nil {
		logger = slog.Default()
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.ConnString())
	if err != nil {
		return nil, fmt.Errorf("invalid postgres connection config: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	poolCfg.MaxConnLifetime = cfg.MaxConnLifetime

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed connecting to postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed pinging postgres at %s:%d: %w", cfg.Host, cfg.Port, err)
	}

	logger.Info("Connected to PostgreSQL instance",
		"host", cfg.Host,
		"port", cfg.Port,
		"database", cfg.Database,
		"max_conns", cfg.MaxConns,
		"min_conns", cfg.MinConns,
	)

	return &PostgresRepository{
		pool:     pool,
		cfg:      cfg,
		batchCfg: batchCfg,
		logger:   logger,
	}, nil
}

// Pool returns the underlying pgx connection pool.
func (r *PostgresRepository) Pool() *pgxpool.Pool {
	return r.pool
}

// HasPostGIS returns whether the connected database has PostGIS extension enabled.
func (r *PostgresRepository) HasPostGIS() bool {
	return r.hasPostGIS
}

// InitSchema ensures the driver_locations table and spatial GiST indices are present.
func (r *PostgresRepository) InitSchema(ctx context.Context) error {
	// 1. Attempt to enable PostGIS extension
	_, err := r.pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS postgis;")
	if err == nil {
		r.hasPostGIS = true
		r.logger.Info("PostGIS spatial extension verified and enabled")
	} else {
		r.hasPostGIS = false
		r.logger.Warn("PostGIS extension not available on PostgreSQL host. Using compatible coordinates schema. (To enable full PostGIS GiST index: sudo apt install postgresql-16-postgis-3)",
			"notice", err.Error(),
		)
	}

	// 2. Create table based on PostGIS availability
	if r.hasPostGIS {
		ddl := `
		CREATE TABLE IF NOT EXISTS driver_locations (
			id BIGSERIAL PRIMARY KEY,
			driver_id VARCHAR(64) NOT NULL,
			latitude DOUBLE PRECISION NOT NULL,
			longitude DOUBLE PRECISION NOT NULL,
			bearing REAL NOT NULL DEFAULT 0.0,
			speed REAL NOT NULL DEFAULT 0.0,
			recorded_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			geom geometry(Point, 4326)
		);
		CREATE INDEX IF NOT EXISTS idx_driver_locations_geom 
			ON driver_locations USING GIST(geom);
		CREATE INDEX IF NOT EXISTS idx_driver_locations_driver_time 
			ON driver_locations (driver_id, recorded_at DESC);
		`
		if _, err := r.pool.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("failed creating PostGIS driver_locations schema: %w", err)
		}
	} else {
		ddl := `
		CREATE TABLE IF NOT EXISTS driver_locations (
			id BIGSERIAL PRIMARY KEY,
			driver_id VARCHAR(64) NOT NULL,
			latitude DOUBLE PRECISION NOT NULL,
			longitude DOUBLE PRECISION NOT NULL,
			bearing REAL NOT NULL DEFAULT 0.0,
			speed REAL NOT NULL DEFAULT 0.0,
			recorded_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_driver_locations_driver_time 
			ON driver_locations (driver_id, recorded_at DESC);
		`
		if _, err := r.pool.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("failed creating standard driver_locations schema: %w", err)
		}
	}

	r.logger.Info("Initialized driver_locations table and indices", "has_postgis", r.hasPostGIS)
	return nil
}

// BulkInsert flushes an accumulated batch of telemetry records using COPY or multi-row INSERT.
func (r *PostgresRepository) BulkInsert(ctx context.Context, records []*TelemetryRecord) error {
	if len(records) == 0 {
		return nil
	}

	start := time.Now()

	var err error
	if r.batchCfg.BulkMode == BulkModeCopy {
		err = r.bulkInsertCopy(ctx, records)
	} else {
		err = r.bulkInsertMultiRow(ctx, records)
	}

	if err != nil {
		return fmt.Errorf("bulk insert failed for %d records: %w", len(records), err)
	}

	r.logger.Debug("Flushed batch to PostgreSQL",
		"records", len(records),
		"mode", r.batchCfg.BulkMode,
		"duration", time.Since(start),
	)

	return nil
}

// bulkInsertCopy streams rows into PostgreSQL via binary COPY protocol (fastest throughput).
func (r *PostgresRepository) bulkInsertCopy(ctx context.Context, records []*TelemetryRecord) error {
	var (
		columns []string
		rows    [][]interface{}
	)

	if r.hasPostGIS {
		columns = []string{"driver_id", "latitude", "longitude", "bearing", "speed", "recorded_at", "geom"}
		rows = make([][]interface{}, len(records))
		for i, rec := range records {
			geomEWKT := fmt.Sprintf("SRID=4326;POINT(%.7f %.7f)", rec.Longitude, rec.Latitude)
			rows[i] = []interface{}{
				rec.DriverID,
				rec.Latitude,
				rec.Longitude,
				rec.Bearing,
				rec.Speed,
				rec.RecordedAt,
				geomEWKT,
			}
		}
	} else {
		columns = []string{"driver_id", "latitude", "longitude", "bearing", "speed", "recorded_at"}
		rows = make([][]interface{}, len(records))
		for i, rec := range records {
			rows[i] = []interface{}{
				rec.DriverID,
				rec.Latitude,
				rec.Longitude,
				rec.Bearing,
				rec.Speed,
				rec.RecordedAt,
			}
		}
	}

	count, err := r.pool.CopyFrom(
		ctx,
		pgx.Identifier{"driver_locations"},
		columns,
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("CopyFrom failed: %w", err)
	}

	if int(count) != len(records) {
		r.logger.Warn("CopyFrom row count mismatch", "expected", len(records), "inserted", count)
	}

	return nil
}

// bulkInsertMultiRow executes a parameterized multi-row INSERT within a single transaction.
func (r *PostgresRepository) bulkInsertMultiRow(ctx context.Context, records []*TelemetryRecord) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var (
		sb   strings.Builder
		args []interface{}
	)

	if r.hasPostGIS {
		sb.WriteString("INSERT INTO driver_locations (driver_id, latitude, longitude, bearing, speed, recorded_at, geom) VALUES ")
		paramIdx := 1
		for i, rec := range records {
			if i > 0 {
				sb.WriteString(", ")
			}
			// Use PostGIS function ST_SetSRID(ST_MakePoint(lon, lat), 4326)
			sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, ST_SetSRID(ST_MakePoint($%d, $%d), 4326))",
				paramIdx, paramIdx+1, paramIdx+2, paramIdx+3, paramIdx+4, paramIdx+5, paramIdx+2, paramIdx+1))
			args = append(args, rec.DriverID, rec.Latitude, rec.Longitude, rec.Bearing, rec.Speed, rec.RecordedAt)
			paramIdx += 6
		}
	} else {
		sb.WriteString("INSERT INTO driver_locations (driver_id, latitude, longitude, bearing, speed, recorded_at) VALUES ")
		paramIdx := 1
		for i, rec := range records {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)",
				paramIdx, paramIdx+1, paramIdx+2, paramIdx+3, paramIdx+4, paramIdx+5))
			args = append(args, rec.DriverID, rec.Latitude, rec.Longitude, rec.Bearing, rec.Speed, rec.RecordedAt)
			paramIdx += 6
		}
	}

	if _, err := tx.Exec(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("multi-insert query execution failed: %w", err)
	}

	return tx.Commit(ctx)
}

// Close terminates all pool connections.
func (r *PostgresRepository) Close() {
	r.pool.Close()
	r.logger.Info("PostgreSQL connection pool closed")
}
