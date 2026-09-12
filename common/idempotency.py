import psycopg2
from psycopg2.errors import UniqueViolation
from psycopg2.extensions import connection


def get_connection(database_url: str) -> connection:
    return psycopg2.connect(database_url)


def init_idempotency_table(database_url: str) -> None:
    """Ensure the processed_events table exists."""
    with get_connection(database_url) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_events (
                    id SERIAL PRIMARY KEY,
                    event_id VARCHAR(255) NOT NULL,
                    consumer_name VARCHAR(255) NOT NULL,
                    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    UNIQUE(event_id, consumer_name)
                );
                """
            )
        conn.commit()


def already_processed(database_url: str, event_id: str, consumer_name: str) -> bool:
    try:
        with get_connection(database_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT 1
                    FROM processed_events
                    WHERE event_id = %s
                        AND consumer_name = %s
                    """,
                    (event_id, consumer_name),
                )
                result = cursor.fetchone()
                return result is not None
    except psycopg2.errors.UndefinedTable:
        init_idempotency_table(database_url)
        return False


def mark_processed(database_url: str, event_id: str, consumer_name: str) -> None:
    conn = get_connection(database_url)
    try:
        with conn.cursor() as cursor:
            try:
                cursor.execute(
                    """
                    INSERT INTO processed_events (
                        event_id,
                        consumer_name
                    )
                    VALUES (%s, %s)
                    """,
                    (
                        event_id,
                        consumer_name,
                    ),
                )
                conn.commit()
            except UniqueViolation:
                conn.rollback()
    finally:
        conn.close()
