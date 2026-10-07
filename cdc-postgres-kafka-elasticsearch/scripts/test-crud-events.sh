#!/usr/bin/env bash
set -euo pipefail

POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-cdc_postgres}"
DB_NAME="${DB_NAME:-inventory_db}"
TEST_UUID="f47ac10b-58cc-4372-a567-0e02b2c3d479"

echo "=========================================================================="
echo " Faz 2: PostgreSQL Olay Simülasyonu (CREATE -> UPDATE -> DELETE)"
echo "=========================================================================="

echo -e "\n1. INSERT İşlemi Yapılıyor (Beklenen Debezium op: 'c')..."
docker exec -i "${POSTGRES_CONTAINER}" psql -U postgres -d "${DB_NAME}" <<EOF
INSERT INTO products (id, title, description, price, category_id, status, version)
VALUES ('${TEST_UUID}', 'Sony WH-1000XM5', 'Wireless Noise Canceling Headphones', 399.99, 105, 'ACTIVE', 1)
ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, price = EXCLUDED.price;
EOF
echo "✔ INSERT tamamlandı. Kafka topic'e 'c' (create) mesajı iletildi."

sleep 2

echo -e "\n2. UPDATE İşlemi Yapılıyor (Beklenen Debezium op: 'u' ve 'before' dolu)..."
docker exec -i "${POSTGRES_CONTAINER}" psql -U postgres -d "${DB_NAME}" <<EOF
UPDATE products 
SET price = 349.99, 
    status = 'DISCOUNTED', 
    version = version + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE id = '${TEST_UUID}';
EOF
echo "✔ UPDATE tamamlandı. 'REPLICA IDENTITY FULL' sayesinde eski fiyat 399.99 'before' içinde yer almalı."

sleep 2

echo -e "\n3. DELETE İşlemi Yapılıyor (Beklenen: Debezium op: 'd' + Null Tombstone)..."
docker exec -i "${POSTGRES_CONTAINER}" psql -U postgres -d "${DB_NAME}" <<EOF
DELETE FROM products WHERE id = '${TEST_UUID}';
EOF
echo "✔ DELETE tamamlandı. Önce 'd' (delete) event'i, ardından tombstone null mesajı basıldı."

echo -e "\n=========================================================================="
echo " Doğrulama için ./scripts/kafka/consume-products-topic.sh çalıştırabilirsiniz."
echo "=========================================================================="
