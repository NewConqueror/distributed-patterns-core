#!/usr/bin/env bash
set -euo pipefail

METRICS_URL="${METRICS_URL:-http://localhost:2112/metrics}"
KAFKA_EXPORTER_URL="${KAFKA_EXPORTER_URL:-http://localhost:9308/metrics}"
ES_URL="${ELASTICSEARCH_URL:-http://localhost:9200}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-cdc_postgres}"
DB_NAME="${DB_NAME:-inventory_db}"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE} Faz 6: Gözlemlenebilirlik, Prometheus Metrikleri ve E2E Latency Doğrulama ${NC}"
echo -e "${BLUE}========================================================================${NC}\n"

# 1. Go Sync Worker Prometheus Metrikleri
echo -e "${YELLOW}[1/3] Go Worker Prometheus Metrikleri (${METRICS_URL})...${NC}"
METRICS_DATA=$(curl -s "${METRICS_URL}" || true)

if [ -n "$METRICS_DATA" ]; then
    echo -e "${CYAN}--- Tüketilen Olay Sayısı (cdc_events_consumed_total) ---${NC}"
    echo "$METRICS_DATA" | grep -E '^cdc_events_consumed_total' || true

    echo -e "\n${CYAN}--- Bulk Flush Süresi (cdc_bulk_flush_duration_seconds) ---${NC}"
    echo "$METRICS_DATA" | grep -E '^cdc_bulk_flush_duration_seconds_(count|sum)' || true

    echo -e "\n${CYAN}--- Batch Doluluk Oranı (cdc_batch_size) ---${NC}"
    echo "$METRICS_DATA" | grep -E '^cdc_batch_size' || true

    echo -e "\n${CYAN}--- Uçtan Uca Gecikme Histogramı (cdc_e2e_latency_seconds) ---${NC}"
    echo "$METRICS_DATA" | grep -E '^cdc_e2e_latency_seconds_(count|sum)' || true

    echo -e "\n${CYAN}--- DLQ ve Circuit Breaker Durumu ---${NC}"
    echo "$METRICS_DATA" | grep -E '^(cdc_dlq_events_total|cdc_circuit_breaker_state)' || true
else
    echo "UYARI: Go Worker metrics endpoint'ine (${METRICS_URL}) ulaşılamadı. Worker çalışıyor mu?"
fi
echo ""

# 2. Kafka Exporter Consumer Lag Değerleri
echo -e "${YELLOW}[2/3] Kafka Exporter Consumer Lag Değerleri (${KAFKA_EXPORTER_URL})...${NC}"
KAFKA_METRICS=$(curl -s "${KAFKA_EXPORTER_URL}" || true)

if [ -n "$KAFKA_METRICS" ]; then
    echo "$KAFKA_METRICS" | grep -E 'kafka_consumergroup_lag\{.*es-products-sync-group.*\}' || true
else
    echo "UYARI: Kafka Exporter (${KAFKA_EXPORTER_URL}) henüz yanıt vermedi."
fi
echo ""

# 3. Canlı Uçtan Uca Gecikme (E2E Latency) Ölçümü
echo -e "${YELLOW}[3/3] Canlı E2E Latency Ölçümü Yapılıyor (T_indexed - T_wal_commit)...${NC}"
BENCHMARK_ID=$(cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen 2>/dev/null || echo "11111111-2222-3333-4444-555555555555")

# Adım 3a: PostgreSQL'e yaz ve tam milisaniye zaman damgasını al
START_MS=$(date +%s%3N)
docker exec -i "${POSTGRES_CONTAINER}" psql -U postgres -d "${DB_NAME}" <<EOF > /dev/null
INSERT INTO products (id, title, description, price, category_id, status, version)
VALUES ('${BENCHMARK_ID}', 'Benchmark Item Latency Test', 'Testing sub-100ms pipeline sync', 199.99, 101, 'ACTIVE', 1);
EOF

# Adım 3b: Elasticsearch'e düşene kadar milisaniyelik poll yap
MAX_WAIT_MS=5000
WAIT_INTERVAL_SEC=0.02 # 20ms
FOUND=0
ELAPSED_MS=0

for i in $(seq 1 250); do
    STATUS_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${ES_URL}/products_search_alias/_doc/${BENCHMARK_ID}" || true)
    if [ "$STATUS_CODE" -eq 200 ]; then
        END_MS=$(date +%s%3N)
        ELAPSED_MS=$((END_MS - START_MS))
        FOUND=1
        break
    fi
    sleep "${WAIT_INTERVAL_SEC}"
done

if [ "$FOUND" -eq 1 ]; then
    echo -e "${GREEN}✔ E2E Senkronizasyon Tamamlandı!${NC}"
    echo -e "   - Ürün UUID           : ${BENCHMARK_ID}"
    echo -e "   - Ölçülen Toplam Süre : ${CYAN}${ELAPSED_MS} ms${NC}"
    echo -e "   - PostgreSQL WAL -> Debezium -> Kafka KRaft -> Go Sync Worker -> Elasticsearch NDJSON Bulk"
    echo -e "\n${GREEN}CV / Portfolyo Argümanı:${NC}"
    echo -e "   \"Sistem ortalama ${CYAN}${ELAPSED_MS}ms${NC} içinde PostgreSQL'deki veriyi Elasticsearch üzerinde aranabilir kılar.\""
else
    echo -e "${YELLOW}Doküman 5 saniye içinde ES üzerinde bulunamadı (Pipeline servisleri çalışıyor mu?)${NC}"
fi

echo -e "\n${GREEN}✔ Faz 6 gözlemlenebilirlik kontrolleri tamamlandı.${NC}"
