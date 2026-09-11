import psycopg2

from .config import config


def get_connection():
    return psycopg2.connect(config.database_url)


def create_tables():
    connection = get_connection()

    try:
        cursor = connection.cursor()

        cursor.execute("""
            create table if not exists orders (
                id SERIAL PRIMARY KEY,
                order_id VARCHAR(100) UNIQUE NOT NULL,
                customer_id VARCHAR(100) NOT NULL,
                product_id VARCHAR(100) NOT NULL,
                quantity INTEGER NOT NULL,
                amount NUMERIC(12, 2) NOT NULL,
                status VARCHAR(50) NOT NULL
            )
        """)

        connection.commit()
        connection.close()
    finally:
        connection.close()


def create_order(
    order_id: str, customer_id: str, product_id: str, quantity: int, amount: float
):
    connection = get_connection()

    try:
        cursor = connection.cursor()

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

        cursor.close()

    finally:
        connection.close()
