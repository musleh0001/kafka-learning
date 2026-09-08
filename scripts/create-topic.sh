#!/usr/bin/env bash
TOPIC_NAME=${1:-orders}

echo "Creating topic: ${TOPIC_NAME}..."
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic "${TOPIC_NAME}" \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
