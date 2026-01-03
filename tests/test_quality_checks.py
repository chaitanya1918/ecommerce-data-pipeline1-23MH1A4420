import psycopg2
import os

def test_no_null_primary_keys():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "ecommerce_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )

    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM staging.customers
        WHERE customer_id IS NULL
    """)
    assert cur.fetchone()[0] == 0

    conn.close()


def test_quantity_positive():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "ecommerce_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )

    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM staging.transaction_items
        WHERE quantity <= 0
    """)
    assert cur.fetchone()[0] == 0

    conn.close()