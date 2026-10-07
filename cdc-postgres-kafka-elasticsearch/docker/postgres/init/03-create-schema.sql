-- ========================================================================
-- 1.3 Şema Tasarımı ve Replica Identity Stratejisi
-- ========================================================================

-- Gerekli eklentiler
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Örnek Senaryo: Products Tablosu
CREATE TABLE IF NOT EXISTS products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(255) NOT NULL,
    description TEXT,
    price NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    category_id INT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'ACTIVE',
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ========================================================================
-- Replica Identity Seçimi: FULL
-- ========================================================================
-- Varsayılan (DEFAULT) sadece Primary Key'in eski değerini tutar.
-- REPLICA IDENTITY FULL ile UPDATE ve DELETE işlemlerinde satırın 
-- TÜM kolonlarının önceki değerleri (before state) WAL loglarına yazılır.
-- Bu sayede Debezium 'before' payload'ını tam doldurur ve Elasticsearch
-- tarafında diff analizi / invalidation eksiksiz yapılabilir.
-- ========================================================================
ALTER TABLE products REPLICA IDENTITY FULL;

-- ========================================================================
-- Debezium Signaling Table (Canlı / Ad-Hoc Incremental Snapshot İçin)
-- ========================================================================
CREATE TABLE IF NOT EXISTS debezium_signal (
    id VARCHAR(64) PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    data VARCHAR(2048) NULL
);

-- Debezium için Logical Replication Publication Tanımlaması
-- products ve debezium_signal tablolarını yayına açar
CREATE PUBLICATION dbz_publication FOR TABLE products, debezium_signal;

-- cdc_user'a gerekli yetkileri ver
GRANT SELECT ON TABLE products TO cdc_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE debezium_signal TO cdc_user;

-- Örnek Seed Verileri (Başlangıç testleri için)
INSERT INTO products (id, title, description, price, category_id, status, version)
VALUES 
    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'Logitech MX Master 3S', 'Ergonomic wireless mouse with 8K DPI sensor', 99.99, 101, 'ACTIVE', 1),
    ('b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22', 'Keychron Q1 Pro', 'Custom mechanical keyboard with wireless support', 199.50, 102, 'ACTIVE', 1),
    ('c2eebc99-9c0b-4ef8-bb6d-6bb9bd380a33', 'Dell UltraSharp U2723QE', '27-inch 4K UHD IPS Black Monitor with USB-C Hub', 549.00, 103, 'ACTIVE', 1)
ON CONFLICT (id) DO NOTHING;
