import json
import os
import signal

from confluent_kafka import Consumer, KafkaError

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
GROUP_ID = "order-processing-group"
CONSUMER_ID = os.getenv("CONSUMER_ID", "consumer-1")

running = True


def shutdown_handler(signum, frame):
    global running
    print("\nStopping consumer cleanly...")
    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "client.id": CONSUMER_ID,
    "auto.offset.reset": "earliest",
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC])

print(
    f"🚀 [{CONSUMER_ID}] Consumer started. Subscribed to '{TOPIC}'. "
    f"Waiting for messages (Ctrl+C to exit)..."
)

try:
    while running:
        message = consumer.poll(1.0)

        if not message:
            continue

        err = message.error()
        if err:
            if err.code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Consumer error: {err}")
            continue


        try:
            if message.value() is None:
                print(
                    f"[{CONSUMER_ID}] Received tombstone at "
                    f"partition={message.partition()}, offset={message.offset()}"
                )
                continue


            raw_value = message.value().decode("utf-8")  # type: ignore
            order = json.loads(raw_value)

            if isinstance(order, dict):
                event_name = order.get("event") or order.get("event_type") or "event"
                cust_id = order.get("customer_id") or order.get("data", {}).get(
                    "customer_id", "N/A"
                )
                print(
                    f"[{CONSUMER_ID}] "
                    f"Received order: "
                    f"[{event_name}]: "
                    f"customer_id={cust_id}, "
                    f"partition={message.partition()}, "
                    f"offset={message.offset()}"
                )
            else:
                print(f"[{CONSUMER_ID}] Received non-dict payload: {order}")
        except Exception as exc:
            print(f"Error parsing message: {exc}")
            continue

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
    print("🔒 Consumer connection closed.")
