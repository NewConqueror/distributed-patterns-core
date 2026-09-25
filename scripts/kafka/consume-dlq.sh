#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-cdc_kafka}"
DLQ_TOPIC="${DLQ_TOPIC:-cdc.products.dlq}"

echo "=========================================================================="
echo " DLQ Consumer: ${DLQ_TOPIC}"
echo " Hata Türü, Neden, Orijinal Offset ve Zehirli Mesaj Payload'ı İzleniyor..."
echo "=========================================================================="
echo "(Durdurmak için Ctrl+C tuşlarına basabilirsiniz)"
echo ""

DOCKER_TTY="-i"
if [ -t 0 ]; then
    DOCKER_TTY="-it"
fi

docker exec ${DOCKER_TTY} "${KAFKA_CONTAINER}" /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic "${DLQ_TOPIC}" \
    --from-beginning \
    --property print.partition=true \
    --property print.timestamp=true \
    --property print.key=true \
    --property print.headers=true \
    --property print.value=true \
    --property key.separator="  ||  KEY: " \
    --property headers.separator="  ||  HEADERS: "
