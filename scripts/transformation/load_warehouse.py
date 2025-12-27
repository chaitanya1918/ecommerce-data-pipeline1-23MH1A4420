import psycopg2
import os

def get_conn():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        port=os.environ.get("DB_PORT", 5432),
        database=os.environ.get("DB_NAME", "ecommerce_db"),
        user=os.environ.get("DB_USER", "admin"),
        password=os.environ.get("DB_PASSWORD", "password")
    )

def load_dim_customers():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO warehouse.dim_customers (
            customer_id,
            full_name,
            email,
            city,
            state,
            country,
            age_group,
            effective_date,
            is_current
        )
        SELECT
            customer_id,
            first_name || ' ' || last_name,
            email,
            city,
            state,
            country,
            age_group,
            CURRENT_DATE,
            TRUE
        FROM production.customers;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Warehouse dim_customers loaded successfully")

def load_dim_products():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO warehouse.dim_products (
            product_id,
            product_name,
            category,
            sub_category,
            price_range,
            effective_date,
            is_current
        )
        SELECT
            product_id,
            product_name,
            category,
            sub_category,
            CASE
                WHEN price < 50 THEN 'Budget'
                WHEN price < 200 THEN 'Mid-range'
                ELSE 'Premium'
            END,
            CURRENT_DATE,
            TRUE
        FROM production.products;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Warehouse dim_products loaded successfully")

def main():
    load_dim_customers()
    load_dim_products()

if __name__ == "__main__":
    main()