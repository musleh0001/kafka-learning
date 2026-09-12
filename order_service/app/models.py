import psycopg2

from .config import config


def get_connection():
    return psycopg2.connect(config.database_url)


def create_tables():
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id SERIAL PRIMARY KEY,
                    order_id VARCHAR(100) UNIQUE NOT NULL,
                    customer_id VARCHAR(100) NOT NULL,
                    product_id VARCHAR(100) NOT NULL,
                    quantity INTEGER NOT NULL,
                    amount NUMERIC(12, 2) NOT NULL,
                    status VARCHAR(50) NOT NULL
                );

                CREATE TABLE IF NOT EXISTS processed_events (
                    id SERIAL PRIMARY KEY,
                    event_id VARCHAR(255) NOT NULL,
                    consumer_name VARCHAR(255) NOT NULL,
                    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),

                    UNIQUE(event_id, consumer_name)
                );
            """)
        connection.commit()


def create_order(
    order_id: str, customer_id: str, product_id: str, quantity: int, amount: float
):
    with get_connection() as connection:
        with connection.cursor() as cursor:
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
        connection.commit()
