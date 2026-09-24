-- ==============================================================================
-- 001_init_schema.sql: Double-Entry Immutable Ledger & Idempotency Schema
-- ==============================================================================

-- 1. ENUM Tipleri
CREATE TYPE account_type_enum AS ENUM (
    'ASSET',
    'LIABILITY',
    'EQUITY',
    'REVENUE',
    'EXPENSE'
);

CREATE TYPE entry_direction_enum AS ENUM (
    'DEBIT',
    'CREDIT'
);

CREATE TYPE idempotency_status_enum AS ENUM (
    'IN_FLIGHT',
    'RESOLVED',
    'FAILED'
);

-- 2. ACCOUNTS (Hesaplar Tablosu)
CREATE TABLE IF NOT EXISTS accounts (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    account_type account_type_enum NOT NULL,
    currency VARCHAR(3) NOT NULL CHECK (length(currency) = 3 AND currency = upper(currency)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_accounts_currency ON accounts (currency);

-- 3. TRANSACTIONS (İşlemler Tablosu - Append Only)
CREATE TABLE IF NOT EXISTS transactions (
    id VARCHAR(64) PRIMARY KEY,
    description TEXT NOT NULL DEFAULT '',
    currency VARCHAR(3) NOT NULL CHECK (length(currency) = 3 AND currency = upper(currency)),
    reversal_of_id VARCHAR(64) REFERENCES transactions(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions (created_at);
CREATE INDEX IF NOT EXISTS idx_transactions_reversal_of ON transactions (reversal_of_id);

-- 4. ENTRIES (Girdiler / Hareketler Tablosu - Append Only)
CREATE TABLE IF NOT EXISTS entries (
    id VARCHAR(64) PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL REFERENCES transactions(id) ON DELETE RESTRICT,
    account_id VARCHAR(64) NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    direction entry_direction_enum NOT NULL,
    amount BIGINT NOT NULL CHECK (amount > 0), -- En küçük para birimi (cent/kuruş) ve kesinlikle > 0
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entries_tx_id ON entries (transaction_id);
CREATE INDEX IF NOT EXISTS idx_entries_account_id ON entries (account_id);

-- 5. IDEMPOTENCY_KEYS (İdempotency Durum Çizelgesi Tablosu)
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key VARCHAR(255) PRIMARY KEY,
    request_hash VARCHAR(64) NOT NULL, -- SHA-256 Hex Hash
    status idempotency_status_enum NOT NULL DEFAULT 'IN_FLIGHT',
    response_code INT,
    response_body JSONB,
    locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_idempotency_keys_status ON idempotency_keys (status);

-- ==============================================================================
-- 6. VERİTABANI TETİKLEYİCİLERİ (DATABASE TRIGGERS & CONSTRAINTS)
-- ==============================================================================

-- 6.1. Finansal Değişmezlik Kuralı: entries ve transactions ASLA güncellenemez veya silinemez
CREATE OR REPLACE FUNCTION trg_prevent_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Finansal Değişmezlik İhlali: % tablosunda UPDATE veya DELETE işlemi kesinlikle yasaktır!', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_immutable_entries ON entries;
CREATE TRIGGER trg_immutable_entries
BEFORE UPDATE OR DELETE ON entries
FOR EACH ROW
EXECUTE FUNCTION trg_prevent_modification();

DROP TRIGGER IF EXISTS trg_immutable_transactions ON transactions;
CREATE TRIGGER trg_immutable_transactions
BEFORE UPDATE OR DELETE ON transactions
FOR EACH ROW
EXECUTE FUNCTION trg_prevent_modification();

-- 6.2. Çift Taraflı Muhasebe Denkliği Denetimi (DEFERRABLE INITIALLY DEFERRED)
-- İşlem tamamlanıp COMMIT edilmeden hemen önce çalışır.
-- Her transaction için: sum(Debit) - sum(Credit) = 0 olmalıdır.
CREATE OR REPLACE FUNCTION trg_check_transaction_balance()
RETURNS TRIGGER AS $$
DECLARE
    v_balance BIGINT;
    v_debit_count INT;
    v_credit_count INT;
    v_tx_id VARCHAR(64);
BEGIN
    v_tx_id := NEW.transaction_id;

    SELECT 
        COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN amount ELSE -amount END), 0),
        COUNT(CASE WHEN direction = 'DEBIT' THEN 1 END),
        COUNT(CASE WHEN direction = 'CREDIT' THEN 1 END)
    INTO v_balance, v_debit_count, v_credit_count
    FROM entries
    WHERE transaction_id = v_tx_id;

    -- En az 1 Borç ve 1 Alacak girdisi bulunmalıdır
    IF v_debit_count = 0 OR v_credit_count = 0 THEN
        RAISE EXCEPTION 'Muhasebe Kuralı İhlali: İşlem (ID: %) en az bir BORÇ (DEBIT) ve bir ALACAK (CREDIT) girdisi içermelidir (Debit: %, Credit: %)', 
            v_tx_id, v_debit_count, v_credit_count;
    END IF;

    -- Borç ve Alacak toplamı eşit olmalıdır (sum(Debit) - sum(Credit) == 0)
    IF v_balance <> 0 THEN
        RAISE EXCEPTION 'Çift Taraflı Muhasebe Denkliği İhlali (Unbalanced Transaction): İşlem (ID: %) için borç ve alacak toplamları eşit değil! Net fark: %', 
            v_tx_id, v_balance;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_deferrable_transaction_balance ON entries;
CREATE CONSTRAINT TRIGGER trg_deferrable_transaction_balance
AFTER INSERT ON entries
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION trg_check_transaction_balance();
