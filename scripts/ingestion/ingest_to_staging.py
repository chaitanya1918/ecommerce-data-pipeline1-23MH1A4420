import os
import json
import time
import logging
from datetime import datetime
import pandas as pd
import psycopg2
import yaml

# --------------------------------------------------
# Load configuration
# --------------------------------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

DB_CONFIG = config["database"]

RAW_DATA_PATH = "data/raw"
SUMMARY_PATH = "data/staging"
LOG_PATH = "logs"

os.makedirs(SUMMARY_PATH, exist_ok=True)
os.makedirs(LOG_PATH, exist_ok=True)

# --------------------------------------------------
# Logging configuration
# --------------------------------------------------
log_file = f"{LOG_PATH}/staging_ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --------------------------------------------------
# Database connection
# --------------------------------------------------
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", DB_CONFIG["host"]),
        port=os.getenv("DB_PORT", DB_CONFIG["port"]),
        dbname=os.getenv("DB_NAME", DB_CONFIG["name"]),
        user=os.getenv("DB_USER", DB_CONFIG["user"]),
        password=os.getenv("DB_PASSWORD", DB_CONFIG["password"])
    )

# --------------------------------------------------
# Bulk load using COPY
# --------------------------------------------------
def copy_csv(cursor, table_name, file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        cursor.copy_expert(
            sql=f"COPY {table_name} FROM STDIN WITH CSV HEADER",
            file=f
        )

# --------------------------------------------------
# Validation function
# --------------------------------------------------
def validate_staging_load(cursor, table, csv_rows):
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    db_rows = cursor.fetchone()[0]
    return db_rows == csv_rows, db_rows

# --------------------------------------------------
# Main ingestion
# --------------------------------------------------
def ingest_to_staging():
    start_time = time.time()
    summary = {
        "ingestion_timestamp": datetime.utcnow().isoformat(),
        "tables_loaded": {},
        "total_execution_time_seconds": 0
    }

    tables = {
        "staging.customers": "customers.csv",
        "staging.products": "products.csv",
        "staging.transactions": "transactions.csv",
        "staging.transaction_items": "transaction_items.csv"
    }

    conn = None

    try:
        conn = get_connection()
        cursor = conn.cursor()
        conn.autocommit = False  # BEGIN TRANSACTION

        logging.info("Starting staging ingestion")

        # Truncate tables first (idempotent)
        for table in tables.keys():
            cursor.execute(f"TRUNCATE TABLE {table}")
            logging.info(f"Truncated {table}")

        # Load data
        for table, file_name in tables.items():
            file_path = os.path.join(RAW_DATA_PATH, file_name)

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"{file_name} not found")

            df = pd.read_csv(file_path)
            rows = len(df)

            copy_csv(cursor, table, file_path)

            valid, db_rows = validate_staging_load(cursor, table, rows)

            if not valid:
                raise ValueError(
                    f"Row count mismatch for {table}: CSV={rows}, DB={db_rows}"
                )

            summary["tables_loaded"][table] = {
                "rows_loaded": db_rows,
                "status": "success"
            }

            logging.info(f"Loaded {db_rows} rows into {table}")

        conn.commit()
        logging.info("Staging ingestion committed successfully")

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Ingestion failed: {str(e)}")

        for table in tables.keys():
            if table not in summary["tables_loaded"]:
                summary["tables_loaded"][table] = {
                    "rows_loaded": 0,
                    "status": "failed",
                    "error_message": str(e)
                }

    finally:
        if conn:
            conn.close()

    summary["total_execution_time_seconds"] = round(
        time.time() - start_time, 2
    )

    with open(f"{SUMMARY_PATH}/ingestion_summary.json", "w") as f:
        json.dump(summary, f, indent=4)

    print("Staging ingestion completed. Check logs and summary.")

# --------------------------------------------------
# Run
# --------------------------------------------------
if __name__ == "__main__":
    ingest_to_staging()
