import json

import psycopg2

from .config import config


def get_connection():
    return psycopg2.connect(config.database_url)


def create_tables():
    connection = get_connection()

    try:
        cursor = connection.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                order_id VARCHAR(100) UNIQUE NOT NULL,
                customer_id VARCHAR(100) NOT NULL,
                product_id VARCHAR(100) NOT NULL,
                quantity INTEGER NOT NULL,
                amount NUMERIC(12, 2) NOT NULL,
                status VARCHAR(50) NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS outbox_events (
                id BIGSERIAL PRIMARY KEY,
                event_id UUID UNIQUE NOT NULL,
                aggregate_type VARCHAR(100) NOT NULL,
                aggregate_id VARCHAR(255) NOT NULL,
                event_type VARCHAR(255) NOT NULL,
                payload JSONB NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                published_at TIMESTAMP NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT NULL
            )
            """
        )

        connection.commit()

    finally:
        cursor.close()
        connection.close()


def create_order_with_event(
    order_id,
    customer_id,
    product_id,
    quantity,
    amount,
    event,
):
    connection = get_connection()

    try:
        cursor = connection.cursor()

        # --------------------------------
        # 1. Create order
        # --------------------------------

        cursor.execute(
            """
            INSERT INTO orders (
                order_id,
                customer_id,
                product_id,
                quantity,
                amount,
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                order_id,
                customer_id,
                product_id,
                quantity,
                amount,
                "CREATED",
            ),
        )

        # --------------------------------
        # 2. Create outbox event
        # --------------------------------

        cursor.execute(
            """
            INSERT INTO outbox_events (
                event_id,
                aggregate_type,
                aggregate_id,
                event_type,
                payload
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s
            )
            """,
            (
                event["event_id"],
                "order",
                order_id,
                event["event_type"],
                json.dumps(event),
            ),
        )

        # --------------------------------
        # 3. Commit both together
        # --------------------------------

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()
        connection.close()
