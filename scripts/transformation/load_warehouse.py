import psycopg2
import os

def main():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        dbname=os.getenv("DB_NAME", "ecommerce_db"),
        user=os.getenv("DB_USER", "admin"),
        password=os.getenv("DB_PASSWORD", "password"),
        port=os.getenv("DB_PORT", "5432")
    )
    cur = conn.cursor()

    print("Loading warehouse.fact_sales...")

    cur.execute("TRUNCATE warehouse.fact_sales;")
cur.execute("""
    INSERT INTO warehouse.fact_sales (
        date_key,
        customer_key,
        product_key,
        payment_key,
        quantity,
        total_amount
    )
    SELECT
        t.transaction_date::date,
        dc.customer_key,
        dp.product_key,
        dpm.payment_key,
        ti.quantity,
        ti.quantity * ti.unit_price
    FROM production.transactions t
    JOIN production.transaction_items ti
        ON t.transaction_id = ti.transaction_id
    JOIN production.customers pc
        ON t.customer_id = pc.customer_id
    JOIN warehouse.dim_customers dc
        ON pc.email = dc.email
    JOIN warehouse.dim_products dp
        ON ti.product_id = dp.product_id
    JOIN warehouse.dim_payment_method dpm
        ON t.payment_method = dpm.payment_method;
""")
    conn.commit()
    cur.close()
    conn.close()

    print("warehouse.fact_sales loaded successfully")

if __name__ == "__main__":
    main()