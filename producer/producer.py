import json

from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"


def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
        return

    key = msg.key().decode("utf-8") if msg.key() else "None"
    print(
        "Message delivered successfully: "
        f"topic={msg.topic()}, "
        f"key={key}, "
        f"partition={msg.partition()}, "
        f"offset={msg.offset()}"
    )


conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "linger.ms": 20,  # Wait up to 20ms to batch messages
    "batch.size": 65536,  # 64 KB batch size
    "compression.type": "snappy",
}
producer = Producer(conf)


for customer_id in range(1, 1000):
    order = {
        "event": "order.created",
        "order_id": customer_id * 100,
        "customer_id": customer_id,
        "amount": 1000,
    }

    try:
        producer.produce(
            TOPIC,
            key=str(order["customer_id"]),
            value=json.dumps(order),
            callback=delivery_report,
        )
    except BufferError:
        producer.poll(0.5)
        producer.produce(
            TOPIC,
            key=str(order["customer_id"]),
            value=json.dumps(order),
            callback=delivery_report,
        )
    producer.poll(0)

producer.flush()
