import json
import signal

from confluent_kafka import Consumer, KafkaError

from .config import config

running = True


def shutdown_handler(signum, frame):
    global running
    print("\nStopping notification service gracefully...")
    running = False


conf = {
    "bootstrap.servers": config.kafka_bootstrap_servers,
    "group.id": config.notification_group,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
consumer = Consumer(conf)


def send_notification(event: dict):
    order_id = event.get("data", {}).get("order_id", "unknown")
    print(f"📩 [NOTIFICATION] Sending order confirmation for order #{order_id}!")


def run():
    global running
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    consumer.subscribe([config.orders_topic])
    print(f"🚀 Notification service started. Subscribed to {config.orders_topic}")

    try:
        while running:
            message = consumer.poll(1.0)

            if message is None:
                continue

            err = message.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[NOTIFICATION] Kafka error: {err}")
                continue


            try:
                event = json.loads(message.value().decode("utf-8"))  # type: ignore
            except Exception as exc:
                print(f"[NOTIFICATION] JSON decode error: {exc}")
                consumer.commit(message=message)
                continue

            if (
                not isinstance(event, dict)
                or event.get("event_type") != "order.completed"
            ):
                consumer.commit(message=message)
                continue

            send_notification(event)
            consumer.commit(message=message)
    except KeyboardInterrupt:
        print("Stopping notification service")
    finally:
        consumer.close()
        print("🔒 Notification service stopped.")
