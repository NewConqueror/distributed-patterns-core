-- init-db.sql
-- Runs automatically on container startup

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'app_user') THEN
        CREATE ROLE app_user WITH LOGIN PASSWORD 'app_password';
    END IF;
END
$$;

-- Apply migrations
\i /docker-entrypoint-initdb.d/migrations/000001_init_schema.up.sql
\i /docker-entrypoint-initdb.d/migrations/000002_create_tuples.up.sql

-- Grant permissions to app_user (non-superuser subject to RLS)
GRANT USAGE ON SCHEMA public TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app_user;
