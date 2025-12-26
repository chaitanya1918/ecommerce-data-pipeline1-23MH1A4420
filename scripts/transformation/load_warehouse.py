import psycopg2

def get_conn():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="ecommerce_db",
        user="admin",
        password="password"
    )

def load_dim_customers():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        TRUNCATE warehouse.dim_customers;

        INSERT INTO warehouse.dim_customers (
            customer_id,
            full_name,
            email,
            city,
            state,
            country,
            age_group,
            customer_segment,
            registration_date,
            effective_date,
            end_date,
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
            'Regular',
            registration_date,
            CURRENT_DATE,
            NULL,
            TRUE
        FROM production.customers;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("dim_customers loaded")

def load_dim_products():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        TRUNCATE warehouse.dim_products;

        INSERT INTO warehouse.dim_products (
            product_id,
            product_name,
            category,
            sub_category,
            brand,
            price_range,
            effective_date,
            end_date,
            is_current
        )
        SELECT
            product_id,
            product_name,
            category,
            sub_category,
            brand,
            CASE
                WHEN price < 50 THEN 'Budget'
                WHEN price < 200 THEN 'Mid-range'
                ELSE 'Premium'
            END,
            CURRENT_DATE,
            NULL,
            TRUE
        FROM production.products;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("dim_products loaded")

if __name__ == "__main__":
    load_dim_customers()
    load_dim_products()
