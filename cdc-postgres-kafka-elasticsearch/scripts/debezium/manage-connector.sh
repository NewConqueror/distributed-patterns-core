#!/usr/bin/env bash
set -euo pipefail

DEBEZIUM_HOST="${DEBEZIUM_HOST:-localhost:8083}"
CONNECTOR_NAME="inventory-postgres-connector"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RAW_CONFIG="${SCRIPT_DIR}/register-postgres-connector.json"
SCHEMA_CONFIG="${SCRIPT_DIR}/register-postgres-connector-with-schema.json"

usage() {
    echo "Kullanım: $0 {register|register-schema|status|delete|restart|pause|resume|config}"
    echo ""
    echo "Komutlar:"
    echo "  register          : Raw JSON modunda connector kaydeder (schemas.enable=false, tavsiye edilen hafif format)"
    echo "  register-schema   : Şema metadatalı modda connector kaydeder (schemas.enable=true)"
    echo "  status            : Connector ve task çalışma durumunu getirir"
    echo "  config            : Mevcut connector yapılandırmasını listeler"
    echo "  restart           : Connector'ı ve alt task'ları yeniden başlatır"
    echo "  pause             : Olay yakalamayı geçici olarak duraklatır"
    echo "  resume            : Duraklatılan olay yakalamayı devam ettirir"
    echo "  delete            : Connector'ı siler"
    exit 1
}

format_json() {
    if command -v jq > /dev/null 2>&1; then
        jq .
    elif command -v python3 > /dev/null 2>&1; then
        python3 -m json.tool
    else
        cat
    fi
}

wait_for_debezium() {
    echo "Debezium Connect REST API bekleniyor (http://${DEBEZIUM_HOST}/connectors)..."
    until curl -s -f "http://${DEBEZIUM_HOST}/connectors" > /dev/null 2>&1; do
        sleep 2
    done
    echo "Debezium Connect hazır."
}

ACTION="${1:-}"

case "$ACTION" in
    register)
        wait_for_debezium
        echo "Connector kaydediliyor (Raw JSON: schemas.enable=false)..."
        curl -s -X POST \
            -H "Content-Type: application/json" \
            "http://${DEBEZIUM_HOST}/connectors" \
            -d @"${RAW_CONFIG}" | format_json
        echo -e "\nDurum sorgulanıyor:"
        sleep 2
        curl -s "http://${DEBEZIUM_HOST}/connectors/${CONNECTOR_NAME}/status" | format_json
        ;;
    register-schema)
        wait_for_debezium
        echo "Connector kaydediliyor (Schema Metadata: schemas.enable=true)..."
        curl -s -X POST \
            -H "Content-Type: application/json" \
            "http://${DEBEZIUM_HOST}/connectors" \
            -d @"${SCHEMA_CONFIG}" | format_json
        echo -e "\nDurum sorgulanıyor:"
        sleep 2
        curl -s "http://${DEBEZIUM_HOST}/connectors/${CONNECTOR_NAME}/status" | format_json
        ;;
    status)
        echo "=== Connector: ${CONNECTOR_NAME} Durumu ==="
        curl -s "http://${DEBEZIUM_HOST}/connectors/${CONNECTOR_NAME}/status" | format_json
        echo ""
        ;;
    config)
        echo "=== Connector: ${CONNECTOR_NAME} Konfigürasyonu ==="
        curl -s "http://${DEBEZIUM_HOST}/connectors/${CONNECTOR_NAME}/config" | format_json
        echo ""
        ;;
    restart)
        echo "Connector ve task yeniden başlatılıyor..."
        curl -s -X POST "http://${DEBEZIUM_HOST}/connectors/${CONNECTOR_NAME}/restart?includeTasks=true" | format_json || true
        echo "İşlem tamamlandı."
        ;;
    pause)
        echo "Connector duraklatılıyor..."
        curl -s -X PUT "http://${DEBEZIUM_HOST}/connectors/${CONNECTOR_NAME}/pause"
        echo "Duraklatıldı."
        ;;
    resume)
        echo "Connector devam ettiriliyor..."
        curl -s -X PUT "http://${DEBEZIUM_HOST}/connectors/${CONNECTOR_NAME}/resume"
        echo "Devam ettirildi."
        ;;
    delete)
        echo "Connector siliniyor..."
        curl -s -X DELETE "http://${DEBEZIUM_HOST}/connectors/${CONNECTOR_NAME}"
        echo "Silindi."
        ;;
    *)
        usage
        ;;
esac
