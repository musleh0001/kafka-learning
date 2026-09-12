import json

from confluent_kafka import Producer

from .config import config

conf = {"bootstrap.servers": config.kafka_bootstrap_servers}
producer = Producer(conf)


def publish_retry(event: dict):
    retry_count = event.get("retry_count", 0)

    if retry_count >= 3:
        topic = "payment-dlt"
    else:
        topic = f"payment-retry-{retry_count + 1}"

    event["retry_count"] = retry_count + 1

    producer.produce(topic=topic, key=event["correlation_id"], value=json.dumps(event))
    producer.poll(0)


def flush():
    producer.flush()
