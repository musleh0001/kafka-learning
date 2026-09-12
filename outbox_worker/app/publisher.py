import json

from confluent_kafka import Producer

from .config import config

producer = Producer(
    {
        "bootstrap.servers": config.kafka_bootstrap_servers,
        "client_id": "outbox-worker",
        "enable.idempotence": True,
    }
)


def delivery_report(err, message):
    if err:
        print("Kafka delivery failed: ", err)
        return

    print(
        "[EVENT DELIVERED]: "
        f"topic={message.topic()} "
        f"partition={message.partition()} "
        f"offset={message.offset()}"
    )


def publish_event(event):
    producer.produce(
        topic="order-events",
        key=event["aggregate_id"],
        value=json.dumps(event["payload"]),
        callback=delivery_report,
    )

    producer.poll(0)


def flush():
    producer.flush()
