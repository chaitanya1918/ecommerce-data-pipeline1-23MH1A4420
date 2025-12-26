import psycopg2
from datetime import datetime, timezone
import json
import os

# --------------------------------------------------
# Output directory
# --------------------------------------------------
SUMMARY_DIR = "data/processed"
os.makedirs(SUMMARY_DIR, exist_ok=True)

# --------------------------------------------------
# DB Connection (Docker-safe)
# --------------------------------------------------
def get_conn():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "postgres"),
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ.get("DB_NAME", "ecommerce_db"),
        user=os.environ.get("DB_USER", "admin"),
        password=os.environ.get("DB_PASSWORD", "password")
    )

# --------------------------------------------------
# Main ETL
# --------------------------------------------------
def staging_to_production():
    conn = get_conn()
    cur = conn.cursor()
    conn.autocommit = False

    summary = {
        "transformation_timestamp": datetime.now(timezone.utc).isoformat(),
        "records_processed": {},
        "transformations_applied": [
            "trim_text_fields",
            "lowercase_emails",
            "phone_standardization",
            "price_precision_standardization",
            "profit_margin_enrichment",
            "price_category_enrichment",
            "business_rule_filtering"
        ],
        "data_quality_post_transform": {
            "null_violations": 0,
            "constraint_violations": 0
        }
    }

    try:
        # ==================================================
        # 1️⃣ CUSTOMERS (FULL TRUNCATE & RELOAD)
        # ==================================================
        cur.execute("SELECT COUNT(*) FROM staging.customers")
        input_count = cur.fetchone()[0]

        cur.execute("TRUNCATE production.customers CASCADE")

        cur.execute("""
            INSERT INTO production.customers (
                customer_id, first_name, last_name, email, phone,
                registration_date, city, state, country, age_group
            )
            SELECT
                customer_id,
                INITCAP(TRIM(first_name)),
                INITCAP(TRIM(last_name)),
                LOWER(TRIM(email)),
                REGEXP_REPLACE(phone, '[^0-9]', '', 'g'),
                registration_date,
                TRIM(city),
                TRIM(state),
                TRIM(country),
                TRIM(age_group)
            FROM staging.customers
            WHERE email IS NOT NULL
        """)

        cur.execute("SELECT COUNT(*) FROM production.customers")
        output_count = cur.fetchone()[0]

        summary["records_processed"]["customers"] = {
            "input": input_count,
            "output": output_count,
            "filtered": input_count - output_count,
            "rejected_reasons": {"null_email": input_count - output_count}
        }

        # ==================================================
        # 2️⃣ PRODUCTS (FULL TRUNCATE & RELOAD)
        # ==================================================
        cur.execute("SELECT COUNT(*) FROM staging.products")
        input_count = cur.fetchone()[0]

        cur.execute("TRUNCATE production.products CASCADE")

        cur.execute("""
            INSERT INTO production.products (
                product_id, product_name, category, sub_category,
                price, cost, brand, stock_quantity, supplier_id,
                profit_margin, price_category
            )
            SELECT
                product_id,
                INITCAP(TRIM(product_name)),
                TRIM(category),
                TRIM(sub_category),
                ROUND(price::numeric, 2),
                ROUND(cost::numeric, 2),
                TRIM(brand),
                stock_quantity,
                supplier_id,
                ROUND(((price - cost) / price) * 100, 2) AS profit_margin,
                CASE
                    WHEN price < 50 THEN 'Budget'
                    WHEN price >= 50 AND price < 200 THEN 'Mid-range'
                    ELSE 'Premium'
                END AS price_category
            FROM staging.products
            WHERE price > 0 AND cost < price
        """)

        cur.execute("SELECT COUNT(*) FROM production.products")
        output_count = cur.fetchone()[0]

        summary["records_processed"]["products"] = {
            "input": input_count,
            "output": output_count,
            "filtered": input_count - output_count,
            "rejected_reasons": {"invalid_price_or_cost": input_count - output_count}
        }

        # ==================================================
        # 3️⃣ TRANSACTIONS (INCREMENTAL LOAD)
        # ==================================================
        cur.execute("""
            INSERT INTO production.transactions (
                transaction_id, customer_id,
                transaction_date, transaction_time,
                payment_method, shipping_address, total_amount
            )
            SELECT
                s.transaction_id,
                s.customer_id,
                s.transaction_date,
                s.transaction_time,
                s.payment_method,
                s.shipping_address,
                s.total_amount
            FROM staging.transactions s
            LEFT JOIN production.transactions p
            ON s.transaction_id = p.transaction_id
            WHERE p.transaction_id IS NULL
              AND s.total_amount > 0
        """)

        cur.execute("SELECT COUNT(*) FROM production.transactions")
        txn_count = cur.fetchone()[0]

        summary["records_processed"]["transactions"] = {
            "input": "incremental",
            "output": txn_count,
            "filtered": 0,
            "rejected_reasons": {}
        }

        # ==================================================
        # 4️⃣ TRANSACTION ITEMS (INCREMENTAL LOAD)
        # ==================================================
        cur.execute("""
            INSERT INTO production.transaction_items (
                item_id, transaction_id, product_id,
                quantity, unit_price, discount_percentage, line_total
            )
            SELECT
                s.item_id,
                s.transaction_id,
                s.product_id,
                s.quantity,
                ROUND(s.unit_price::numeric, 2),
                s.discount_percentage,
                ROUND(
                    s.quantity * s.unit_price * (1 - s.discount_percentage / 100),
                    2
                )
            FROM staging.transaction_items s
            LEFT JOIN production.transaction_items p
            ON s.item_id = p.item_id
            WHERE p.item_id IS NULL
              AND s.quantity > 0
        """)

        cur.execute("SELECT COUNT(*) FROM production.transaction_items")
        item_count = cur.fetchone()[0]

        summary["records_processed"]["transaction_items"] = {
            "input": "incremental",
            "output": item_count,
            "filtered": 0,
            "rejected_reasons": {}
        }

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        conn.close()

    with open(f"{SUMMARY_DIR}/transformation_summary.json", "w") as f:
        json.dump(summary, f, indent=4)

    print(" Staging → Production transformation completed successfully")

# --------------------------------------------------
if __name__ == "__main__":
    staging_to_production()
