import psycopg2
import os

def test_production_tables_populated():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "ecommerce_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM production.customers")
    assert cur.fetchone()[0] > 0

    conn.close()


def test_warehouse_fact_sales_populated():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "ecommerce_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM warehouse.fact_sales")
    assert cur.fetchone()[0] > 0

    conn.close()