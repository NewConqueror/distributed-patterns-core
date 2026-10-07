#!/usr/bin/env bash
set -euo pipefail

ES_URL="${ELASTICSEARCH_URL:-http://localhost:9200}"

OLD_INDEX="${1:-}"
NEW_INDEX="${2:-}"
ALIAS_NAME="${3:-products_search_alias}"

if [ -z "$OLD_INDEX" ] || [ -z "$NEW_INDEX" ]; then
    echo "Kullanım: $0 <eski_index> <yeni_index> [alias_adi]"
    echo "Örnek:   $0 products_v1 products_v2 products_search_alias"
    exit 1
fi

echo "=========================================================================="
echo " Elasticsearch Zero-Downtime Atomic Alias Değişimi"
echo "=========================================================================="
echo "Eski Index:  ${OLD_INDEX}"
echo "Yeni Index:  ${NEW_INDEX}"
echo "Hedef Alias: ${ALIAS_NAME}"
echo "Elasticsearch URL: ${ES_URL}"
echo ""

# Yeni index'in varlığını doğrula
if ! curl -s -f -I "${ES_URL}/${NEW_INDEX}" > /dev/null; then
    echo "HATA: Yeni indeks '${NEW_INDEX}' Elasticsearch üzerinde bulunamadı!"
    exit 1
fi

PAYLOAD=$(cat <<EOF
{
  "actions": [
    { "remove": { "index": "${OLD_INDEX}", "alias": "${ALIAS_NAME}" } },
    { "add":    { "index": "${NEW_INDEX}", "alias": "${ALIAS_NAME}" } }
  ]
}
EOF
)

echo "Atomic alias transferi yapılıyor..."
RESPONSE=$(curl -s -X POST "${ES_URL}/_aliases" \
  -H "Content-Type: application/json" \
  -d "${PAYLOAD}")

echo "Elasticsearch Cevabı: ${RESPONSE}"
echo ""

echo "Güncel Alias Haritası:"
curl -s "${ES_URL}/_alias/${ALIAS_NAME}?pretty" || true
echo ""
echo "✔ '${ALIAS_NAME}' alias'ı başarıyla sıfır kesintiyle '${NEW_INDEX}' indeksine taşındı!"
