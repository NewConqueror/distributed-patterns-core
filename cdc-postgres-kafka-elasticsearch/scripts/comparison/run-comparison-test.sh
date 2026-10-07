#!/usr/bin/env bash
set -euo pipefail

SAMPLES="${1:-100}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "=========================================================================="
echo " Faz 7.2: Dual-Write (Geleneksel) vs CDC (Debezium + Kafka) Karşılaştırma "
echo "=========================================================================="
echo "Test Edilecek Örnek Sayısı: ${SAMPLES}"
echo ""

if [ ! -f "${ROOT_DIR}/bin/dual-write-test" ]; then
    echo "dual-write-test derleniyor..."
    (cd "${ROOT_DIR}" && GOTOOLCHAIN=local go build -o bin/dual-write-test ./scripts/comparison)
fi

"${ROOT_DIR}/bin/dual-write-test" \
    -samples="${SAMPLES}" \
    -postgres-url="postgres://postgres:postgres_master_pass@localhost:5432/inventory_db?sslmode=disable" \
    -es-url="http://localhost:9200"
