import json

from confluent_kafka import Consumer

from .config import config

conf = {
    "bootstrap.servers": config.kafka_bootstrap_servers,
    "group.id": config.inventory_group,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
consumer = Consumer(conf)


def reserve_inventory(event):
    print(f"[INVENTORY] Reserving product {event['product_id']}")
    print(f"[INVENTORY] Quantity: {event['quantity']}")
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
            reserve_inventory(event)
            consumer.commit(message=message)
    except KeyboardInterrupt:
        print("Stopping inventory service")
    finally:
        consumer.close()
