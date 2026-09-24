-- ==============================================================================
-- 002_concurrency_and_snapshots.sql: Concurrency Control & Snapshotting Engine
-- ==============================================================================

-- 1. accounts tablosuna Optimistic Concurrency Control (OCC) için version sütunu ekle
ALTER TABLE accounts 
ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;

-- 2. account_snapshots tablosu (Okuma Optimizasyonu / Snapshotting Engine)
-- Her sorguda milyonlarca satırı SUM(amount) yapmaktan kaçınmak için periyodik checkpoint tablosu.
CREATE TABLE IF NOT EXISTS account_snapshots (
    id VARCHAR(64) PRIMARY KEY,
    account_id VARCHAR(64) NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    balance BIGINT NOT NULL, -- Snapshot anındaki bakiye (en küçük birim: cent/kuruş)
    last_entry_id VARCHAR(64) REFERENCES entries(id) ON DELETE RESTRICT, -- Dahil edilen en son girdinin ID'si
    entry_count BIGINT NOT NULL DEFAULT 0, -- Snapshot anına kadar olan toplam girdi adedi
    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Hızlı erişim için indeksler: En son snapshot'ı getirmek için (account_id, snapshot_at DESC)
CREATE INDEX IF NOT EXISTS idx_snapshots_account_id_time 
ON account_snapshots (account_id, snapshot_at DESC);

CREATE INDEX IF NOT EXISTS idx_snapshots_last_entry_id 
ON account_snapshots (last_entry_id);
