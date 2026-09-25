#!/usr/bin/env bash
set -euo pipefail

TOTAL_OPS="${1:-3000}"
CONCURRENCY="${2:-16}"
BATCH_SIZE="${3:-50}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "=========================================================================="
echo " Faz 7.1: Yüksek Eşzamanlı Yük Testi (Stress Test & Catch-Up Benchmark)   "
echo "=========================================================================="
echo "İşlem Sayısı : ${TOTAL_OPS}"
echo "Eşzamanlılık : ${CONCURRENCY} worker"
echo "Batch Boyutu : ${BATCH_SIZE}"
echo ""

# Binary derli mi kontrol et
if [ ! -f "${ROOT_DIR}/bin/stress-generator" ]; then
    echo "stress-generator derleniyor..."
    (cd "${ROOT_DIR}" && GOTOOLCHAIN=local go build -o bin/stress-generator ./cmd/stress-generator)
fi

"${ROOT_DIR}/bin/stress-generator" \
    -total="${TOTAL_OPS}" \
    -concurrency="${CONCURRENCY}" \
    -batch="${BATCH_SIZE}" \
    -postgres-url="postgres://postgres:postgres_master_pass@localhost:5432/inventory_db?sslmode=disable" \
    -es-url="http://localhost:9200" \
    -es-alias="products_search_alias"
