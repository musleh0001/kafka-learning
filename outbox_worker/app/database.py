import psycopg2
from psycopg2 import connection

from .config import config


def get_connection() -> connection:
    return psycopg2.connect(config.database_url)


def fetch_pending_events(
    connection,
    batch_size,
):
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            id,
            event_id,
            aggregate_type,
            aggregate_id,
            event_type,
            payload,
            attempts
        FROM outbox_events
        WHERE published_at IS NULL
        ORDER BY id
        FOR UPDATE SKIP LOCKED
        LIMIT %s
        """,
        (batch_size,),
    )

    return cursor.fetchall()


def mark_published(cursor, event_id):
    cursor.execute(
        """
        UPDATE outbox_events
        SET published_at = NOW()
        WHERE id = %s
        """,
        (event_id,),
    )


def mark_failed(cursor, event_id, error):
    cursor.execute(
        """
        UPDATE outbox_events
        SET
            attempts = attempts + 1,
            last_error = %s
        WHERE id = %s
        """,
        (
            str(error),
            event_id,
        ),
    )
