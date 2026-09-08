#!/usr/bin/env bash
TOPIC_NAME=${1:-orders}

echo "Starting console consumer for topic: ${TOPIC_NAME} (Reading from beginning, Ctrl+C to exit)..."
docker exec -it kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic "${TOPIC_NAME}" \
  --from-beginning \
  --bootstrap-server localhost:9092
