import time

from .config import config
from .database import (
    fetch_pending_events,
    get_connection,
    mark_failed,
    mark_published,
)
from .publisher import (
    flush,
    publish_event,
)


def process_batch():
    connection = get_connection()

    try:
        events = fetch_pending_events(
            connection,
            config.batch_size,
        )

        if not events:
            connection.rollback()
            return

        cursor = connection.cursor()

        for row in events:
            (
                database_id,
                event_id,
                aggregate_type,
                aggregate_id,
                event_type,
                payload,
                attempts,
            ) = row

            event = {
                "database_id": database_id,
                "event_id": str(event_id),
                "aggregate_type": aggregate_type,
                "aggregate_id": aggregate_id,
                "event_type": event_type,
                "payload": payload,
                "attempts": attempts,
            }

            try:
                print(
                    "Publishing event:",
                    event["event_id"],
                )

                publish_event(event)

                flush()

                mark_published(
                    cursor,
                    database_id,
                )

            except Exception as exc:
                print(
                    "Publishing failed:",
                    exc,
                )

                mark_failed(
                    cursor,
                    database_id,
                    exc,
                )

        connection.commit()

    except Exception:
        connection.rollback()

        raise

    finally:
        connection.close()


def run():

    print("Outbox worker started")

    try:
        while True:
            process_batch()

            time.sleep(config.poll_interval)

    except KeyboardInterrupt:
        print("Outbox worker stopped")


if __name__ == "__main__":
    run()
