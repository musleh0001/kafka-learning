import json
import signal

from confluent_kafka import Consumer, KafkaError

from common.events import create_event

from .config import config
from .kafka import flush, publish_inventory_reserved

running = True


def shutdown_handler(signum, frame):
    global running
    print("\nStopping inventory service gracefully...")
    running = False


conf = {
    "bootstrap.servers": config.kafka_bootstrap_servers,
    "group.id": config.inventory_group,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
consumer = Consumer(conf)


def reserve_inventory(event: dict):
    data = event.get("data", {})
    print(f"[INVENTORY] Reserving product {data.get('product_id')}")
    print(f"[INVENTORY] Quantity: {data.get('quantity')}")
    print("[INVENTORY] Inventory reserved")


def run():
    global running
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    consumer.subscribe([config.orders_topic])
    print(f"🚀 Inventory service started. Subscribed to {config.orders_topic}")

    try:
        while running:
            message = consumer.poll(1.0)

            if message is None:
                continue

            err = message.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[INVENTORY] Kafka error: {err}")
                continue


            try:
                event = json.loads(message.value().decode("utf-8"))  # type: ignore
            except Exception as exc:
                print(f"[INVENTORY] JSON decode error: {exc}")
                consumer.commit(message=message)
                continue

            if (
                not isinstance(event, dict)
                or event.get("event_type") != "order.created"
            ):
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
        print("🔒 Inventory service stopped.")
