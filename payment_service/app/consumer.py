import json

from confluent_kafka import Consumer

from common.events import create_event

from .config import config
from .kafka import flush, publish_payment_completed

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
    consumer.subscribe([config.orders_topic])

    try:
        while True:
            message = consumer.poll(1.0)

            if not message:
                continue

            if message.error():
                print(f"Kafka error: {message.error()}")
                continue

            event = json.loads(message.value().decode("utf-8"))  # type: ignore

            if event.get("event_type") != "order.created":
                consumer.commit(message=message)
                continue

            process_payment(event)

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

            consumer.commit(message=message)
    except KeyboardInterrupt:
        print("Stopping payment service")
    finally:
        flush()
        consumer.close()
