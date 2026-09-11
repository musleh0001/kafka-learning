import json

from confluent_kafka import Consumer

from common.events import create_event

from .config import config
from .kafka import flush, publish_inventory_reserved

conf = {
    "bootstrap.servers": config.kafka_bootstrap_servers,
    "group.id": config.inventory_group,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
consumer = Consumer(conf)


def reserve_inventory(event):
    print(f"[INVENTORY] Reserving product {event['data']['product_id']}")
    print(f"[INVENTORY] Quantity: {event['data']['quantity']}")
    print("[INVENTORY] Inventory reserved")


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

            if event.get("event_type") != "order.created":
                consumer.commit(message=message)
                continue

            reserve_inventory(event)
            data = {
                "order_id": event["data"]["order_id"],
                "product_id": event["data"]["product_id"],
                "quantity": event["data"]["quantity"],
                "status": "RESERVED",
            }
            inventory_event = create_event(
                event_type="inventory.reserved",
                correlation_id=event["correlation_id"],
                data=data,
            )
            publish_inventory_reserved(inventory_event)
            print("Published inventory.reserved")

            consumer.commit(message=message)
    except KeyboardInterrupt:
        print("Stopping inventory service")
    finally:
        flush()
        consumer.close()
