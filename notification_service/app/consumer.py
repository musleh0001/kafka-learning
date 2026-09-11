import json

from confluent_kafka import Consumer

from .config import config

conf = {
    "bootstrap.servers": config.kafka_bootstrap_servers,
    "group.id": config.notification_group,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
consumer = Consumer(conf)


def send_notification(event: dict):
    print(f"Sending confirmation for {event['data']['order_id']}")


def run():
    consumer.subscribe([config.orders_topic])

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"Kafka error: {message.error()}")
                continue

            event = json.loads(message.value().decode("utf-8"))  # type: ignore

            if event.get("event_type") != "order.completed":
                consumer.commit(message=message)
                continue

            send_notification(event)
            consumer.commit(message=message)
    except KeyboardInterrupt:
        print("Stopping notification service")
    finally:
        consumer.close()
