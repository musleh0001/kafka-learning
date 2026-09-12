import json

from confluent_kafka import Consumer

from .retry_producer import (
    publish_retry,
)

conf = {
    "bootstrap.servers": "localhost:9092",
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
    consumer.subscribe(TOPICS)

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(message.error())
                continue

            event = json.loads(message.value().decode("utf-8"))  # type: ignore

            print(f"[RETRY] Processing retry={event.get('retry_count')}")

            try:
                # Simulate payment
                # processing again.
                print("[RETRY] Payment successful")

                consumer.commit(message=message)

            except Exception:
                publish_retry(event)

                consumer.commit(message=message)

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()
