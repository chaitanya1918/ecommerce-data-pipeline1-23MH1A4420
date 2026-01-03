import psycopg2
import csv
import os
import yaml

# -------------------------------
# Load DB config
# -------------------------------
with open("/app/config/config.yaml") as f:
    DB = yaml.safe_load(f)["database"]

DATA_DIR = "/app/data/raw"


# -------------------------------
# DB Connection
# -------------------------------
def get_conn():
    return psycopg2.connect(
        host=DB["host"],        # postgres (docker service name)
        port=DB["port"],
        dbname=DB["name"],
        user=DB["user"],
        password=DB["password"]
    )


# -------------------------------
# Ingestion Logic
# -------------------------------
def ingest():
    conn = get_conn()
    cur = conn.cursor()

    print("Starting staging ingestion")

    # ---------- CLEAN TABLES ----------
    cur.execute("TRUNCATE staging.transaction_items")
    cur.execute("TRUNCATE staging.transactions")
    cur.execute("TRUNCATE staging.products")
    cur.execute("TRUNCATE staging.customers")
    conn.commit()

    # ---------- CUSTOMERS ----------
    print("Loading staging.customers (1000 rows)")
    with open(f"{DATA_DIR}/customers.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [
            (
                r["customer_id"],
                r["first_name"],
                r["last_name"],
                r["email"],
                r["phone"],
                r["registration_date"],
                r["city"],
                r["state"],
                r["country"],
                r["age_group"]
            )
            for r in reader
        ]

    cur.executemany(
        """
        INSERT INTO staging.customers
        (customer_id, first_name, last_name, email, phone,
         registration_date, city, state, country, age_group)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        rows
    )

    # ---------- PRODUCTS ----------
    print("Loading staging.products (500 rows)")
    with open(f"{DATA_DIR}/products.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [
            (
                r["product_id"],
                r["product_name"],
                r["category"],
                r["sub_category"],
                float(r["cost_price"]),
                float(r["selling_price"]),
                r["supplier_name"],
                int(r["stock_quantity"]),
                r["supplier_id"]
            )
            for r in reader
        ]

    cur.executemany(
        """
        INSERT INTO staging.products
        (product_id, product_name, category, subcategory,
         cost_price, selling_price, supplier_name,
         stock_quantity, supplier_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        rows
    )

    # ---------- TRANSACTIONS ----------
    print("Loading staging.transactions (10000 rows)")
    with open(f"{DATA_DIR}/transactions.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [
            (
                r["transaction_id"],
                r["customer_id"],
                r["transaction_date"],
                r["payment_method"]
            )
            for r in reader
        ]

    cur.executemany(
        """
        INSERT INTO staging.transactions
        (transaction_id, customer_id, transaction_date, payment_method)
        VALUES (%s,%s,%s,%s)
        """,
        rows
    )

    # ---------- TRANSACTION ITEMS ----------
    print("Loading staging.transaction_items (30000+ rows)")
    with open(f"{DATA_DIR}/transaction_items.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [
            (
                r["item_id"],
                r["transaction_id"],
                r["product_id"],
                int(r["quantity"]),
                float(r["unit_price"]),
                float(r["line_total"])
            )
            for r in reader
        ]

    cur.executemany(
        """
        INSERT INTO staging.transaction_items
        (item_id, transaction_id, product_id,
         quantity, unit_price, line_total)
        VALUES (%s,%s,%s,%s,%s,%s)
        """,
        rows
    )

    conn.commit()
    cur.close()
    conn.close()

    print("STAGING INGESTION COMPLETED SUCCESSFULLY")


# -------------------------------
# Run
# -------------------------------
if __name__ == "__main__":
    ingest()