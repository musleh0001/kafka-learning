import psycopg2
from psycopg2 import connection
from psycopg2.errors import UniqueViolation


def get_connection(database_url: str) -> connection:
    return psycopg2.connect(database_url)


def already_processed(database_url: str, event_id: str, consumer_name: str):
    connection = get_connection(database_url)

    try:
        cursor = connection.cursor()

        cursor.execute(
            """
                SELECT 1
                FROM processed_events
                WHERE event_id = %s
                    AND consumer_name =%s
            """,
            (event_id, consumer_name),
        )

        result = cursor.fetchone()
        cursor.close()
        return result is not None
    finally:
        connection.close()


def mark_processed(database_url: str, event_id, consumer_name: str):
    connection = get_connection(database_url)

    try:
        cursor = connection.cursor()

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
            connection.commit()
        except UniqueViolation:
            connection.rollback()
        finally:
            cursor.close()
    finally:
        connection.close()
