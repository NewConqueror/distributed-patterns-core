#!/usr/bin/env bash
set -euo pipefail

ES_URL="${ELASTICSEARCH_URL:-http://localhost:9200}"
INDEX_NAME="${ELASTICSEARCH_INDEX:-products}"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE} Faz 3: Elasticsearch Senkronizasyon ve Arama Doğrulama                ${NC}"
echo -e "${BLUE}========================================================================${NC}\n"

echo -e "${YELLOW}1. Elasticsearch Cluster Sağlık Durumu:${NC}"
curl -s "${ES_URL}/_cluster/health?pretty" || echo "Elasticsearch'e ulaşılamadı!"
echo ""

echo -e "${YELLOW}2. '${INDEX_NAME}' İndeks İstatistikleri ve Doküman Sayısı:${NC}"
curl -s "${ES_URL}/_cat/indices/${INDEX_NAME}?v&s=index" || true
echo ""

echo -e "${YELLOW}3. '${INDEX_NAME}' İndeksindeki Son Senkronize Edilen Dokümanlar:${NC}"
RESPONSE=$(curl -s "${ES_URL}/${INDEX_NAME}/_search?pretty&size=10")

if command -v jq > /dev/null 2>&1; then
    echo "$RESPONSE" | jq '{
        total_hits: .hits.total.value,
        documents: [.hits.hits[] | {
            id: ._id,
            version: ._version,
            title: ._source.title,
            category_name: ._source.category_name,
            price: ._source.price,
            status: ._source.status,
            db_version: ._source.version,
            indexed_at: ._source.indexed_at
        }]
    }'
elif command -v python3 > /dev/null 2>&1; then
    echo "$RESPONSE" | python3 -m json.tool
else
    echo "$RESPONSE"
fi

echo -e "\n${GREEN}✔ Elasticsearch arama sorgusu başarıyla tamamlandı.${NC}"
