-- =====================================================================
-- GeoPulse Database Initialization Script (Docker Entrypoint)
-- =====================================================================

-- 1. Enable PostGIS extension for spatial types and indexing
CREATE EXTENSION IF NOT EXISTS postgis;

-- 2. Telemetry ingestion historical table
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

-- 3. High-performance Spatial GiST Index for microsecond bounding-box & distance queries
CREATE INDEX IF NOT EXISTS idx_driver_locations_geom 
    ON driver_locations USING GIST(geom);

-- 4. Composite index for time-series driver telemetry lookups
CREATE INDEX IF NOT EXISTS idx_driver_locations_driver_time 
    ON driver_locations (driver_id, recorded_at DESC);
