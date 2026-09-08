import json

from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
GROUP_ID = "order-processing-group"


consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    }
)

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

        order_id = order["order_id"]
        amount = order["amount"]

        print(f"Processing order {order_id}...")

        if amount > 50000:
            print(f"Order {order_id} requires manual review.")
        else:
            print(f"Order {order_id} approved.")
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
