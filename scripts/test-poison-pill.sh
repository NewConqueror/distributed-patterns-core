#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-cdc_kafka}"
TARGET_TOPIC="${TARGET_TOPIC:-pg_cdc.public.products}"

echo "=========================================================================="
echo " Faz 4: Zehirli Mesaj (Poison Pill) Simülasyonu"
echo "=========================================================================="
echo "Hedef Topic: ${TARGET_TOPIC}"
echo "Zehirli (bozuk JSON) bir mesaj üretiliyor..."

# Bozuk JSON ve geçersiz payload bas
docker exec -i "${KAFKA_CONTAINER}" /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic "${TARGET_TOPIC}" \
    --property parse.key=true \
    --property key.separator=":" <<EOF
poison-key-999:{"before":null,"after":MALFORMED_CORRUPTED_PAYLOAD_HERE!!,"op":"c"}
EOF

echo -e "\n✔ Zehirli mesaj Kafka topic'ine enjekte edildi."
echo "Beklenen Davranış:"
echo "1. Go Worker zehirli mesajı yakalayacak (JSON Unmarshal hatası)."
echo "2. Mesaj 'cdc.products.dlq' topic'ine diagnostic header'lar ile yönlendirilecek."
echo "3. Ana topic offset'i commit edilecek ve boru hattı kilitlenmeden devam edecek."
echo ""
echo "Doğrulamak için ./scripts/kafka/consume-dlq.sh çalıştırabilirsiniz."
