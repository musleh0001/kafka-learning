import json

from confluent_kafka import Producer

from common.events import delivery_report

from .config import config
from .retry import get_retry_topic

conf = {"bootstrap.servers": config.kafka_bootstrap_servers}
producer = Producer(conf)


def publish_retry(event: dict):
    retry_count = event.get("retry_count", 0)
    topic = get_retry_topic(retry_count)
    event["retry_count"] = retry_count + 1

    producer.produce(
        topic=topic,
        key=event["correlation_id"],
        value=json.dumps(event),
        callback=delivery_report,
    )
    producer.poll(0)


def flush():
    producer.flush()
