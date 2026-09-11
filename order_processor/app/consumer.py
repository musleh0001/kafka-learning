import json

from confluent_kafka import Consumer

from .config import config
from .kafka import flush, publish_order_completed
from .state import (
    is_order_completed,
    mark_inventory_reserved,
    mark_payment_completed,
)

consumer = Consumer(
    {
        "bootstrap.servers": config.kafka_bootstrap_servers,
        "group.id": config.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)


def process_event(event):
    event_type = event["event_type"]

    order_id = event["data"]["order_id"]

    if event_type == "payment.completed":
        mark_payment_completed(order_id)

    elif event_type == "inventory.reserved":
        mark_inventory_reserved(order_id)

    if is_order_completed(order_id):
        print(f"\n[ORDER] {order_id} completed")

        publish_order_completed(
            correlation_id=event["correlation_id"],
            order_id=order_id,
        )


def run():
    consumer.subscribe([config.payment_topic, config.inventory_topic])

    try:
        while True:
            message = consumer.poll(1.0)

            if not message:
                continue

            if message.error():
                print(f"Kafka error: {message.error()}")
                continue

            event = json.loads(message.value().decode("utf-8"))  # type: ignore

            print(f"[ORDER PROCESSOR] {event['event_type']}")

            process_event(event)

            consumer.commit(message=message)

    except KeyboardInterrupt:
        print("Stopping order processor")

    finally:
        flush()
        consumer.close()
