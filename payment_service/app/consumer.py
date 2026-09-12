import json

from confluent_kafka import Consumer, KafkaError

from common.events import create_event
from common.idempotency import already_processed, mark_processed

from .config import config
from .kafka import flush, publish_payment_completed
from .retry_producer import flush as flush_retry
from .retry_producer import publish_retry
from .shutdown import is_running, setup_graceful_shutdown

conf = {
    "bootstrap.servers": config.kafka_bootstrap_servers,
    "group.id": config.payment_group,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
consumer = Consumer(conf)


def process_payment(event: dict):
    data = event["data"]
    amount = (
        int(data["amount"]) if data["amount"] == int(data["amount"]) else data["amount"]
    )

    print("[PAYMENT] Processing order")
    print(f"[PAYMENT] Charging {amount}")
    print("[PAYMENT] Payment successful")


def run():
    setup_graceful_shutdown()
    consumer.subscribe([config.orders_topic])
    print(f"🚀 Payment service started. Subscribed to {config.orders_topic}")

    try:
        while is_running():
            message = consumer.poll(1.0)

            if not message:
                continue

            err = message.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Kafka error: {err}")
                continue

            try:
                event = json.loads(message.value().decode("utf-8"))  # type: ignore
            except Exception as exc:
                print(f"[PAYMENT] JSON decode error: {exc}")
                consumer.commit(message=message)
                continue

            event_id = event.get("event_id")
            if event_id and already_processed(
                config.database_url, event_id, config.payment_group
            ):
                print(f"[PAYMENT] Skipping duplicate {event_id}")
                consumer.commit(message=message)
                continue

            if event.get("event_type") != "order.created":
                consumer.commit(message=message)
                continue

            try:
                process_payment(event)
            except Exception as exc:
                print(f"[PAYMENT] Failed: {exc}")
                publish_retry(event)
                consumer.commit(message=message)
                continue

            data = {
                "order_id": event["data"]["order_id"],
                "amount": event["data"]["amount"],
                "status": "COMPLETED",
            }
            payment_event = create_event(
                event_type="payment.completed",
                correlation_id=event["correlation_id"],
                data=data,
            )
            publish_payment_completed(payment_event)
            print("Published payment.completed")

            if event_id:
                mark_processed(config.database_url, event_id, config.payment_group)

            consumer.commit(message=message)
    except KeyboardInterrupt:
        print("Stopping payment service")
    finally:
        flush()
        flush_retry()
        consumer.close()
        print("🔒 Payment service stopped.")
