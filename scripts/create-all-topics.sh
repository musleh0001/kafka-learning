#!/usr/bin/env bash
set -euo pipefail

TOPICS=(
  "orders"
  "order-events"
  "inventory-events"
  "payment-events"
  "payment-retry-1"
  "payment-retry-2"
  "payment-retry-3"
  "payment-dlt"
)

echo "Provisioning Kafka topics (3 partitions each)..."

for TOPIC in "${TOPICS[@]}"; do
  echo "  Creating topic: ${TOPIC}..."
  docker exec kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --create \
    --if-not-exists \
    --topic "${TOPIC}" \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1
done

echo "Listing all topics in Kafka:"
docker exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
