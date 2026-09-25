#!/usr/bin/env bash
set -euo pipefail

ES_URL="${ELASTICSEARCH_URL:-http://localhost:9200}"
ALIAS_NAME="${ELASTICSEARCH_SEARCH_ALIAS:-products_search_alias}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-cdc_postgres}"
DB_NAME="${DB_NAME:-inventory_db}"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE} Faz 5: Re-indexing, Alias ve Snapshot Senkronizasyon Doğrulama         ${NC}"
echo -e "${BLUE}========================================================================${NC}\n"

# 1. Alias Durumu ve Hangi İndekse Bağlı Olduğu
echo -e "${YELLOW}[1/4] '${ALIAS_NAME}' Alias Durumu Sorgulanıyor...${NC}"
curl -s "${ES_URL}/_alias/${ALIAS_NAME}?pretty" || echo "Alias bulunamadı veya ES erişilemez!"
echo ""

# 2. Alias Üzerinden Şeffaf Arama Sorgusu
echo -e "${YELLOW}[2/4] '${ALIAS_NAME}' Alias'ı Üzerinden Arama Yapılıyor...${NC}"
SEARCH_RES=$(curl -s "${ES_URL}/${ALIAS_NAME}/_search?size=5&pretty" || true)

if command -v jq > /dev/null 2>&1; then
    echo "$SEARCH_RES" | jq '{
        total_documents: .hits.total.value,
        routed_index: .hits.hits[0]._index,
        sample_titles: [.hits.hits[]._source.title]
    }' || echo "$SEARCH_RES"
else
    echo "$SEARCH_RES"
fi
echo ""

# 3. PostgreSQL Debezium Sinyal Tablosu Kontrolü
echo -e "${YELLOW}[3/4] PostgreSQL 'debezium_signal' Tablosundaki Sinyaller...${NC}"
docker exec -i "${POSTGRES_CONTAINER}" psql -U postgres -d "${DB_NAME}" -c "
SELECT id, type, data FROM debezium_signal ORDER BY id DESC LIMIT 5;
" || echo "Sinyal tablosu henüz sorgulanamadı."

# 4. PostgreSQL Replikasyon Slotu ve LSN Durumu
echo -e "\n${YELLOW}[4/4] PostgreSQL Replikasyon Slotu ve LSN Durumu...${NC}"
docker exec -i "${POSTGRES_CONTAINER}" psql -U postgres -d "${DB_NAME}" -c "
SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn 
FROM pg_replication_slots;
" || true

echo -e "\n${GREEN}✔ Faz 5 doğrulama adımları tamamlandı!${NC}"
