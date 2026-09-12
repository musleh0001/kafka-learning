import json

from confluent_kafka import Producer

from common.events import delivery_report

from .config import config

conf = {
    "bootstrap.servers": config.kafka_bootstrap_servers,
    "client.id": "inventory-service",
}
producer = Producer(conf)


def publish_inventory_reserved(event: dict):
    producer.produce(
        topic=config.inventory_topic,
        key=event["correlation_id"],
        value=json.dumps(event),
        callback=delivery_report,
    )
    producer.poll(0)


def flush():
    producer.flush()
