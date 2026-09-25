#!/usr/bin/env bash
set -euo pipefail

POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-cdc_postgres}"
DB_NAME="${DB_NAME:-inventory_db}"
TABLE_NAME="${1:-public.products}"

echo "=========================================================================="
echo " Faz 5: Debezium Sinyal Tablosu ile Canlı Snapshot Tetikleme"
echo "=========================================================================="
echo "Hedef Tablo: ${TABLE_NAME}"
echo "Veritabanı: ${DB_NAME} (Konteyner: ${POSTGRES_CONTAINER})"
echo ""

SIGNAL_ID="snapshot-$(date +%s)"

echo "debezium_signal tablosuna 'execute-snapshot' sinyali yazılıyor..."

docker exec -i "${POSTGRES_CONTAINER}" psql -U postgres -d "${DB_NAME}" <<EOF
INSERT INTO debezium_signal (id, type, data)
VALUES (
    '${SIGNAL_ID}',
    'execute-snapshot',
    '{"data-collections": ["${TABLE_NAME}"], "type": "INCREMENTAL"}'
);

SELECT id, type, data FROM debezium_signal ORDER BY id DESC LIMIT 1;
EOF

echo ""
echo "✔ Incremental snapshot sinyali başarıyla PostgreSQL'e yazıldı."
echo "Debezium WAL akışı üzerinden bu sinyali yakalayacak ve '${TABLE_NAME}'"
echo "için canlı sistemi durdurmadan arka planda su seviyesi (watermark)"
echo "pencereleri ile snapshot kayıtlarını Kafka'ya yeniden basacaktır."
echo ""
echo "İlerlemeyi izlemek için ./scripts/kafka/consume-products-topic.sh çalıştırabilirsiniz."
