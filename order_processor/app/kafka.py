import json

from confluent_kafka import Producer

from common.events import delivery_report

from .config import config

conf = {
    "bootstrap.servers": config.kafka_bootstrap_servers,
    "client.id": config.group_id,
}
producer = Producer(conf)


def publish_order_completed(correlation_id: str, order_id: str):
    event = {
        "event_id": f"{order_id}-completed",
        "event_type": "order.completed",
        "event_version": 1,
        "correlation_id": correlation_id,
        "data": {
            "order_id": order_id,
            "status": "COMPLETED",
        },
    }

    producer.produce(
        topic=config.order_topic,
        key=correlation_id,
        value=json.dumps(event),
        callback=delivery_report,
    )
    producer.poll(0)


def flush():
    producer.flush()
