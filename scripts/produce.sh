#!/usr/bin/env bash
TOPIC_NAME=${1:-orders}

echo "Starting console producer for topic: ${TOPIC_NAME} (Type messages and press Enter, Ctrl+C to exit)..."
docker exec -it kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --topic "${TOPIC_NAME}" \
  --bootstrap-server localhost:9092
