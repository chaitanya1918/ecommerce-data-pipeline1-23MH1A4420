import psycopg2
import yaml


def get_connection():
    with open("config/config.yaml") as f:
        db = yaml.safe_load(f)["database"]

    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["name"],
        user=db["user"],
        password=db["password"]
    )


def run_etl():
    print("Starting ETL: staging to production")

    conn = get_connection()
    cur = conn.cursor()

    try:
        # PRODUCTS
        cur.execute(
            """
            INSERT INTO production.products (
                product_id,
                product_name,
                category,
                cost_price,
                selling_price,
                supplier_name,
                stock_quantity,
                supplier_id
            )
            SELECT
                product_id,
                product_name,
                category,
                cost_price,
                selling_price,
                supplier_name,
                stock_quantity,
                supplier_id
            FROM staging.products;
            """
        )

        # CUSTOMERS
        cur.execute(
            """
            INSERT INTO production.customers (
                customer_id,
                first_name,
                last_name,
                email,
                phone,
                registration_date,
                city,
                state,
                country,
                age_group
            )
            SELECT
                customer_id,
                first_name,
                last_name,
                email,
                phone,
                registration_date,
                city,
                state,
                country,
                age_group
            FROM staging.customers;
            """
        )

        # TRANSACTIONS
        cur.execute(
            """
            INSERT INTO production.transactions (
                transaction_id,
                customer_id,
                transaction_date,
                payment_method,
                total_amount
            )
            SELECT
                transaction_id,
                customer_id,
                transaction_date,
                payment_method,
                total_amount
            FROM staging.transactions;
            """
        )

        # TRANSACTION ITEMS
        cur.execute("""
INSERT INTO production.transaction_items (
    item_id,
    transaction_id,
    product_id,
    quantity,
    unit_price,
    total_price,
    loaded_at
)
SELECT
    item_id,
    transaction_id,
    product_id,
    quantity,
    unit_price,
    total_price,
    loaded_at
FROM staging.transaction_items;
""")

        conn.commit()
        print("ETL completed successfully")

    except Exception as e:
        conn.rollback()
        print("ETL failed:", e)

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run_etl()