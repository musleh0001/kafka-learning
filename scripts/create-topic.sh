#!/usr/bin/env bash
TOPIC_NAME=${1:-orders}
PARTITIONS=${2:-3}

echo "Creating topic '${TOPIC_NAME}' with ${PARTITIONS} partition(s)..."
docker exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic "${TOPIC_NAME}" \
  --bootstrap-server localhost:9092 \
  --partitions "${PARTITIONS}" \
  --replication-factor 1

