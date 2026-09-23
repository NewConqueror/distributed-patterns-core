-- Migration Rollback: 000002_create_tuples.down.sql

DROP POLICY IF EXISTS tuples_tenant_isolation_policy ON tuples;
ALTER TABLE tuples NO FORCE ROW LEVEL SECURITY;
ALTER TABLE tuples DISABLE ROW LEVEL SECURITY;

DROP INDEX IF EXISTS idx_tuples_subject_relation;
DROP INDEX IF EXISTS idx_tuples_reverse;
DROP INDEX IF EXISTS idx_tuples_lookup;
DROP INDEX IF EXISTS uq_tuples_tenant_entry;

DROP TABLE IF EXISTS tuples;
