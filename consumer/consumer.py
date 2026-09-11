import json
import os

from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
GROUP_ID = "order-processing-group"
CONSUMER_ID = os.getenv("CONSUMER_ID", "consumer-1")

conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "client.id": CONSUMER_ID,
    "auto.offset.reset": "earliest",
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC])

print("Consumer started...")
print("Waiting for messages...")


try:
    while True:
        message = consumer.poll(1.0)

        if not message:
            continue

        if message.error():
            print(f"Consumer error: {message.error()}")
            continue

        try:
            raw_value = message.value().decode("utf-8")  # type: ignore
            order = json.loads(raw_value)
        except Exception as exc:
            print(f"Error: {exc}")
            continue

        print(
            f"[{CONSUMER_ID}] "
            f"Received order: "
            f"[{order['event']}]: "
            f"customer_id={order['customer_id']}, "
            f"partition={message.partition()}, "
            f"offset={message.offset()}"
        )
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
