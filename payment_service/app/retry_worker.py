import json

from confluent_kafka import Consumer, KafkaError

from common.events import create_event
from common.idempotency import mark_processed

from .config import config
from .kafka import flush as flush_kafka
from .kafka import publish_payment_completed
from .retry_producer import (
    flush as flush_retry,
)
from .retry_producer import (
    publish_retry,
)
from .shutdown import is_running, setup_graceful_shutdown

conf = {
    "bootstrap.servers": config.kafka_bootstrap_servers,
    "group.id": "payment-retry-worker",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
consumer = Consumer(conf)

TOPICS = [
    "payment-retry-1",
    "payment-retry-2",
    "payment-retry-3",
]


def run():
    setup_graceful_shutdown()
    consumer.subscribe(TOPICS)
    print(f"🚀 Payment retry worker started. Subscribed to {TOPICS}")

    try:
        while is_running():
            message = consumer.poll(1.0)

            if message is None:
                continue

            err = message.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[RETRY] Kafka error: {err}")
                continue


            try:
                event = json.loads(message.value().decode("utf-8"))  # type: ignore
            except Exception as exc:
                print(f"[RETRY] JSON decode error: {exc}")
                consumer.commit(message=message)
                continue

            order_id = event.get("data", {}).get("order_id")
            print(
                f"[RETRY] Processing retry={event.get('retry_count')} "
                f"for order={order_id}"
            )


            try:
                # Simulate payment processing again
                print(
                    f"[RETRY] Payment successful for order {event['data']['order_id']}"
                )

                # Mark processed in idempotency store
                event_id = event.get("event_id")
                if event_id:
                    mark_processed(config.database_url, event_id, config.payment_group)

                # Notify order processor that payment has completed
                payment_event = create_event(
                    event_type="payment.completed",
                    correlation_id=event["correlation_id"],
                    data={
                        "order_id": event["data"]["order_id"],
                        "amount": event["data"]["amount"],
                        "status": "COMPLETED",
                    },
                )
                publish_payment_completed(payment_event)
                print("[RETRY] Published payment.completed")

                consumer.commit(message=message)

            except Exception as exc:
                print(f"[RETRY] Failed again: {exc}")
                publish_retry(event)
                consumer.commit(message=message)

    except KeyboardInterrupt:
        print("Stopping payment retry worker")

    finally:
        flush_kafka()
        flush_retry()
        consumer.close()
        print("🔒 Payment retry worker stopped.")
