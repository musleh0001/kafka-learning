import json

from confluent_kafka import Consumer

from .config import config

conf = {
    "bootstrap.servers": config.kafka_bootstrap_servers,
    "group.id": config.payment_group,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
consumer = Consumer(conf)


def process_payment(event: dict):
    print(f"[PAYMENT] Processing order {event['order_id']}")
    print(f"[PAYMENT] Amount: {event['amount']}")
    print(f"[PAYMENT] Payment completed")


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

            print(
                f"Received event: "
                f"partition={message.partition()} "
                f"offset={message.offset()}"
            )

            process_payment(event)

            consumer.commit(message=message)
    except KeyboardInterrupt:
        print("Stopping payment service")
    finally:
        consumer.close()
