import psycopg2
import os

def test_staging_tables_have_data():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "ecommerce_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )

    cur = conn.cursor()
    tables = [
        "staging.customers",
        "staging.products",
        "staging.transactions",
        "staging.transaction_items"
    ]

    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        assert count > 0, f"{table} is empty"

    conn.close()