import psycopg2
import pandas as pd
import os
import json
import time
from datetime import datetime

OUTPUT_DIR = "data/processed/analytics"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_conn():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "postgres"),
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ.get("DB_NAME", "ecommerce_db"),
        user=os.environ.get("DB_USER", "admin"),
        password=os.environ.get("DB_PASSWORD", "password")
    )

QUERIES = {
    "query1_top_products": """
    SELECT dp.product_name, dp.category,
           SUM(fs.line_total) AS total_revenue,
           SUM(fs.quantity) AS units_sold,
           AVG(fs.unit_price) AS avg_price
    FROM warehouse.fact_sales fs
    JOIN warehouse.dim_products dp ON fs.product_key = dp.product_key
    GROUP BY dp.product_name, dp.category
    ORDER BY total_revenue DESC
    LIMIT 10;
    """,

    "query2_monthly_trend": """
    SELECT d.year, d.month,
           SUM(fs.line_total) AS total_revenue,
           COUNT(DISTINCT fs.transaction_id) AS total_transactions,
           AVG(fs.line_total) AS average_order_value,
           COUNT(DISTINCT fs.customer_key) AS unique_customers
    FROM warehouse.fact_sales fs
    JOIN warehouse.dim_date d ON fs.date_key = d.date_key
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month;
    """,

    "query3_customer_segmentation": """
    WITH customer_totals AS (
        SELECT customer_key, SUM(line_total) AS total_spent
        FROM warehouse.fact_sales
        GROUP BY customer_key
    )
    SELECT
        CASE
            WHEN total_spent < 1000 THEN '$0-$1,000'
            WHEN total_spent < 5000 THEN '$1,000-$5,000'
            WHEN total_spent < 10000 THEN '$5,000-$10,000'
            ELSE '$10,000+'
        END AS spending_segment,
        COUNT(*) AS customer_count,
        SUM(total_spent) AS total_revenue,
        AVG(total_spent) AS avg_transaction_value
    FROM customer_totals
    GROUP BY spending_segment;
    """,

    "query4_category_performance": """
    SELECT dp.category,
           SUM(fs.line_total) AS total_revenue,
           SUM(fs.profit) AS total_profit,
           (SUM(fs.profit)/SUM(fs.line_total))*100 AS profit_margin_pct,
           SUM(fs.quantity) AS units_sold
    FROM warehouse.fact_sales fs
    JOIN warehouse.dim_products dp ON fs.product_key = dp.product_key
    GROUP BY dp.category;
    """,

    "query5_payment_distribution": """
    SELECT pm.payment_method_name,
           COUNT(*) AS transaction_count,
           SUM(fs.line_total) AS total_revenue,
           COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct_of_transactions,
           SUM(fs.line_total) * 100.0 / SUM(SUM(fs.line_total)) OVER () AS pct_of_revenue
    FROM warehouse.fact_sales fs
    JOIN warehouse.dim_payment_method pm
         ON fs.payment_method_key = pm.payment_method_key
    GROUP BY pm.payment_method_name;
    """
}

def main():
    conn = get_conn()
    summary = {
        "generation_timestamp": datetime.utcnow().isoformat(),
        "queries_executed": len(QUERIES),
        "query_results": {}
    }

    start_all = time.time()

    for name, sql in QUERIES.items():
        start = time.time()
        df = pd.read_sql(sql, conn)
        duration = (time.time() - start) * 1000

        output_file = f"{OUTPUT_DIR}/{name}.csv"
        df.to_csv(output_file, index=False)

        summary["query_results"][name] = {
            "rows": len(df),
            "columns": len(df.columns),
            "execution_time_ms": round(duration, 2)
        }

    summary["total_execution_time_seconds"] = round(time.time() - start_all, 2)

    with open(f"{OUTPUT_DIR}/analytics_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print("Analytics generated successfully")

    conn.close()

if __name__ == "__main__":
    main()
