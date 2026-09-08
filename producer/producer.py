import json

from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"


def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
        return

    print(
        "Message delivered successfully: "
        f"topic={msg.topic()}, "
        f"partition={msg.partition()}, "
        f"offset={msg.offset()}"
    )


producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
orders = [
    {
        "event": "order.created",
        "order_id": 1003,
        "customer_id": 503,
        "product": "Mouse",
        "amount": 1500,
    },
    {
        "event": "order.created",
        "order_id": 1004,
        "customer_id": 504,
        "product": "Monitor",
        "amount": 25000,
    },
    {
        "event": "order.created",
        "order_id": 1005,
        "customer_id": 505,
        "product": "Headphones",
        "amount": 5000,
    },
]

for order in orders:
    producer.produce(
        TOPIC,
        key=str(order["order_id"]),
        value=json.dumps(order),
        callback=delivery_report,
    )
producer.flush()
