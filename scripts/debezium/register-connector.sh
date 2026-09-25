#!/usr/bin/env bash
set -euo pipefail

DEBEZIUM_HOST="${DEBEZIUM_HOST:-localhost:8083}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/register-postgres-connector.json"

echo "=== Debezium PostgreSQL Connector Kaydı Başlatılıyor ==="
echo "Bağlantı Noktası: http://${DEBEZIUM_HOST}"

# Connect API hazır olana kadar bekle
until curl -s -f "http://${DEBEZIUM_HOST}/connectors" > /dev/null; do
    echo "Debezium Connect REST API bekleniyor (http://${DEBEZIUM_HOST}/connectors)..."
    sleep 3
done

echo "Debezium Connect hazır! Connector kaydediliyor..."

RESPONSE=$(curl -s -X POST \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  "http://${DEBEZIUM_HOST}/connectors" \
  -d @"${CONFIG_FILE}")

echo "Cevap: ${RESPONSE}"
echo ""
echo "Mevcut Aktif Bağlayıcılar:"
curl -s "http://${DEBEZIUM_HOST}/connectors" | grep -o '\[.*\]' || true
echo ""
echo "Connector Durumu:"
curl -s "http://${DEBEZIUM_HOST}/connectors/inventory-postgres-connector/status" || true
echo ""
