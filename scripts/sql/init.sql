-- Products Table
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    stock INT NOT NULL CHECK (stock >= 0),
    version INT NOT NULL DEFAULT 0
);

-- Orders Table
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    product_id INT REFERENCES products(id),
    user_id VARCHAR(64) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transactional Outbox Table
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Initial Seed Data
INSERT INTO products (name, stock)
SELECT 'Bilet/Telefon', 100
WHERE NOT EXISTS (SELECT 1 FROM products WHERE id = 1);
