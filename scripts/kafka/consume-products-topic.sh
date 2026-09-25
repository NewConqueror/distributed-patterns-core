#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-cdc_kafka}"
TOPIC_NAME="${TOPIC_NAME:-pg_cdc.public.products}"

echo "================================================================="
echo " Kafka Consumer: ${TOPIC_NAME}"
echo " Partition, Key, Timestamp ve Payload (op) İzleniyor..."
echo "================================================================="
echo "(Durdurmak için Ctrl+C tuşlarına basabilirsiniz)"
echo ""

DOCKER_TTY="-i"
if [ -t 0 ]; then
    DOCKER_TTY="-it"
fi

docker exec ${DOCKER_TTY} "${KAFKA_CONTAINER}" /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic "${TOPIC_NAME}" \
    --from-beginning \
    --property print.partition=true \
    --property print.timestamp=true \
    --property print.key=true \
    --property print.value=true \
    --property key.separator="  ||  KEY: " \
    --property null.literal="[NULL_TOMBSTONE_MESSAGE]"
