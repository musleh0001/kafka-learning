import json

from confluent_kafka import Producer

from .config import config

conf = {
    "bootstrap.servers": config.kafka_bootstrap_servers,
    "client.id": "order-service",
}
producer = Producer(conf)


def delivery_report(err, msg):
    if err:
        print(f">>> [ERROR] Message delivery failed: {err}")
        return

    print(
        "[MESSAGE DELIVERED] "
        f"topic={msg.topic()} "
        f"partition={msg.partition()} "
        f"offset={msg.ofset()}"
    )


def publish_order(event: dict):
    producer.produce(
        topic=config.orders_topic,
        key=event["order_id"],
        value=json.dumps(event),
        callback=delivery_report,
    )
    producer.poll(0)


def flush():
    producer.flush()
