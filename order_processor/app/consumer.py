import json
import signal

from confluent_kafka import Consumer, KafkaError

from .config import config
from .kafka import flush, publish_order_completed
from .state import (
    is_order_completed,
    mark_inventory_reserved,
    mark_payment_completed,
)

running = True


def shutdown_handler(signum, frame):
    global running
    print("\nStopping order processor gracefully...")
    running = False


consumer = Consumer(
    {
        "bootstrap.servers": config.kafka_bootstrap_servers,
        "group.id": config.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)


def process_event(event: dict):
    event_type = event.get("event_type")
    data = event.get("data", {})
    order_id = data.get("order_id")

    if not order_id:
        return

    if event_type == "payment.completed":
        mark_payment_completed(order_id)
    elif event_type == "inventory.reserved":
        mark_inventory_reserved(order_id)
    else:
        return

    if is_order_completed(order_id):
        print(f"\n[ORDER PROCESSOR] All requirements met! Order {order_id} completed.")
        correlation_id = str(event.get("correlation_id") or order_id)
        publish_order_completed(
            correlation_id=correlation_id,
            order_id=order_id,
        )


def run():
    global running
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    consumer.subscribe([config.payment_topic, config.inventory_topic])
    print(
        f"🚀 Order processor started. "
        f"Subscribed to: {config.payment_topic}, {config.inventory_topic}"
    )

    try:
        while running:
            message = consumer.poll(1.0)

            if not message:
                continue

            err = message.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[ORDER PROCESSOR] Kafka error: {err}")
                continue

            try:
                event = json.loads(message.value().decode("utf-8"))  # type: ignore
            except Exception as exc:
                print(f"[ORDER PROCESSOR] JSON decode error: {exc}")
                consumer.commit(message=message)
                continue

            if isinstance(event, dict):
                print(f"[ORDER PROCESSOR] Received {event.get('event_type')}")
                process_event(event)

            consumer.commit(message=message)

    except KeyboardInterrupt:
        print("Stopping order processor")

    finally:
        flush()
        consumer.close()
        print("🔒 Order processor stopped.")
