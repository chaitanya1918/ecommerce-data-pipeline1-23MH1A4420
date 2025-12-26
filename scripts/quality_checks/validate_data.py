import json
import psycopg2
from datetime import datetime, timezone
import os
import yaml

# --------------------------------------------------
# Load config
# --------------------------------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

DB = config["database"]

OUTPUT_DIR = "data/quality"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --------------------------------------------------
# Database connection (FINAL SAFE VERSION)
# --------------------------------------------------
def get_conn():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ.get("DB_NAME", "ecommerce_db"),
        user=os.environ.get("DB_USER", "admin"),
        password=os.environ.get("DB_PASSWORD", "password")
    )

# --------------------------------------------------
# Helper
# --------------------------------------------------
def scalar(cur, q):
    cur.execute(q)
    return cur.fetchone()[0]

# --------------------------------------------------
# Main quality checks
# --------------------------------------------------
def run_quality_checks():
    conn = get_conn()
    cur = conn.cursor()

    report = {
        "check_timestamp": datetime.now(timezone.utc).isoformat(),
        "checks_performed": {},
        "overall_quality_score": 0,
        "quality_grade": ""
    }

    # ---------------- Completeness ----------------
    nulls = {
        "customers.email": scalar(cur, "SELECT COUNT(*) FROM staging.customers WHERE email IS NULL"),
        "products.price": scalar(cur, "SELECT COUNT(*) FROM staging.products WHERE price IS NULL"),
        "transactions.customer_id": scalar(cur, "SELECT COUNT(*) FROM staging.transactions WHERE customer_id IS NULL")
    }
    null_count = sum(nulls.values())

    report["checks_performed"]["null_checks"] = {
        "status": "passed" if null_count == 0 else "failed",
        "null_violations": null_count,
        "details": nulls
    }

    # ---------------- Uniqueness ----------------
    dup_emails = scalar(cur, """
        SELECT COUNT(*) FROM (
            SELECT email FROM staging.customers
            GROUP BY email HAVING COUNT(*) > 1
        ) x
    """)

    report["checks_performed"]["duplicate_checks"] = {
        "status": "passed" if dup_emails == 0 else "failed",
        "duplicates_found": dup_emails
    }

    # ---------------- Referential Integrity ----------------
    orphan_txn = scalar(cur, """
        SELECT COUNT(*) FROM staging.transactions t
        LEFT JOIN staging.customers c
        ON t.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """)

    orphan_items_txn = scalar(cur, """
        SELECT COUNT(*) FROM staging.transaction_items ti
        LEFT JOIN staging.transactions t
        ON ti.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL
    """)

    orphan_items_prod = scalar(cur, """
        SELECT COUNT(*) FROM staging.transaction_items ti
        LEFT JOIN staging.products p
        ON ti.product_id = p.product_id
        WHERE p.product_id IS NULL
    """)

    orphan_total = orphan_txn + orphan_items_txn + orphan_items_prod

    report["checks_performed"]["referential_integrity"] = {
        "status": "passed" if orphan_total == 0 else "failed",
        "orphan_records": orphan_total
    }

    # ---------------- Consistency ----------------
    line_mismatch = scalar(cur, """
        SELECT COUNT(*) FROM staging.transaction_items
        WHERE ABS(
            line_total - (quantity * unit_price * (1 - discount_percentage / 100))
        ) > 0.01
    """)

    txn_total_mismatch = scalar(cur, """
        SELECT COUNT(*) FROM (
            SELECT t.transaction_id, t.total_amount, SUM(ti.line_total) calc
            FROM staging.transactions t
            JOIN staging.transaction_items ti
            ON t.transaction_id = ti.transaction_id
            GROUP BY t.transaction_id, t.total_amount
        ) x
        WHERE ABS(total_amount - calc) > 0.01
    """)

    report["checks_performed"]["data_consistency"] = {
        "status": "passed" if (line_mismatch + txn_total_mismatch) == 0 else "failed",
        "mismatches": line_mismatch + txn_total_mismatch
    }

    # ---------------- Business Rules ----------------
    cost_price = scalar(cur, "SELECT COUNT(*) FROM staging.products WHERE cost >= price")
    future_txn = scalar(cur, "SELECT COUNT(*) FROM staging.transactions WHERE transaction_date > CURRENT_DATE")

    report["checks_performed"]["business_rules"] = {
        "status": "passed" if (cost_price + future_txn) == 0 else "failed",
        "violations": cost_price + future_txn
    }

    # ---------------- Scoring ----------------
    score = 100
    score -= orphan_total * 10
    score -= null_count * 5
    score -= dup_emails * 5
    score -= (line_mismatch + txn_total_mismatch) * 3
    score -= (cost_price + future_txn) * 2

    score = max(score, 0)
    report["overall_quality_score"] = score
    report["quality_grade"] = "A" if score >= 90 else "B" if score >= 80 else "C" if score >= 70 else "D"

    conn.close()

    with open(f"{OUTPUT_DIR}/data_quality_report.json", "w") as f:
        json.dump(report, f, indent=4)

    print(" Data quality checks completed successfully")

# --------------------------------------------------
if __name__ == "__main__":
    run_quality_checks()
