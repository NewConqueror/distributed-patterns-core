-- Migration: 000002_create_tuples.up.sql
-- Description: Relation tuples table for Google Zanzibar ReBAC model with composite indexes and RLS

CREATE TABLE IF NOT EXISTS tuples (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    entity_type VARCHAR(64) NOT NULL,
    entity_id VARCHAR(128) NOT NULL,
    relation VARCHAR(64) NOT NULL,
    subject_type VARCHAR(64) NOT NULL,
    subject_id VARCHAR(128) NOT NULL,
    subject_relation VARCHAR(64) DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Unique constraint ensuring no duplicate tuples exist within a tenant
-- Using COALESCE for nullable subject_relation to prevent duplicate NULL entries
CREATE UNIQUE INDEX IF NOT EXISTS uq_tuples_tenant_entry 
    ON tuples (tenant_id, entity_type, entity_id, relation, subject_type, subject_id, COALESCE(subject_relation, ''));

-- Primary lookup index: Given entity (e.g. document:doc-1) and relation (e.g. viewer), find subjects
CREATE INDEX IF NOT EXISTS idx_tuples_lookup 
    ON tuples (tenant_id, entity_type, entity_id, relation);

-- Reverse lookup index: Given subject (e.g. user:u-123), find entities or memberships
CREATE INDEX IF NOT EXISTS idx_tuples_reverse 
    ON tuples (tenant_id, subject_type, subject_id);

-- Secondary lookup for subject_relation expansions (e.g. subject is workspace:ws-1#member)
CREATE INDEX IF NOT EXISTS idx_tuples_subject_relation 
    ON tuples (tenant_id, subject_type, subject_id, subject_relation);

-- Enable RLS and FORCE RLS on tuples table to guarantee complete multi-tenant isolation
ALTER TABLE tuples ENABLE ROW LEVEL SECURITY;
ALTER TABLE tuples FORCE ROW LEVEL SECURITY;

CREATE POLICY tuples_tenant_isolation_policy ON tuples
    FOR ALL
    USING (tenant_id = current_tenant_id())
    WITH CHECK (tenant_id = current_tenant_id());
