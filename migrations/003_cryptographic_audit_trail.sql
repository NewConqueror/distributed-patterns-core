-- ==============================================================================
-- 003_cryptographic_audit_trail.sql: Cryptographic Hash-Chain & Tamper-Evidence
-- ==============================================================================

-- 1. entries tablosuna hesap bazlı hash zinciri sütunları ekle
ALTER TABLE entries
ADD COLUMN IF NOT EXISTS prev_hash VARCHAR(64) NOT NULL DEFAULT '',
ADD COLUMN IF NOT EXISTS current_hash VARCHAR(64) NOT NULL DEFAULT '',
ADD COLUMN IF NOT EXISTS sequence_num BIGINT NOT NULL DEFAULT 1;

CREATE INDEX IF NOT EXISTS idx_entries_account_sequence 
ON entries (account_id, sequence_num ASC);

CREATE INDEX IF NOT EXISTS idx_entries_current_hash 
ON entries (current_hash);

-- 2. transactions tablosuna global defter hash zinciri sütunları ekle
ALTER TABLE transactions
ADD COLUMN IF NOT EXISTS prev_hash VARCHAR(64) NOT NULL DEFAULT '',
ADD COLUMN IF NOT EXISTS current_hash VARCHAR(64) NOT NULL DEFAULT '',
ADD COLUMN IF NOT EXISTS sequence_num BIGINT NOT NULL DEFAULT 1;

CREATE INDEX IF NOT EXISTS idx_transactions_sequence 
ON transactions (sequence_num ASC);

CREATE INDEX IF NOT EXISTS idx_transactions_current_hash 
ON transactions (current_hash);
