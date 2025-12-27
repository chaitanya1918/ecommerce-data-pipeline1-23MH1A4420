import psycopg2
import json
import os
from datetime import datetime, timezone
import statistics
import time

OUTPUT_FILE = "data/processed/monitoring_report.json"

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": os.environ.get("DB_PORT", "5432"),
    "dbname": os.environ.get("DB_NAME", "ecommerce_db"),
    "user": os.environ.get("DB_USER", "admin"),
    "password": os.environ.get("DB_PASSWORD", "password"),
}

# ---------- HELPERS ----------

def utc_now():
    return datetime.now(timezone.utc)

def hours_diff(ts):
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (utc_now() - ts).total_seconds() / 3600

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

# ---------- CHECKS ----------

def check_last_execution():
    try:
        with open("data/processed/pipeline_execution_report.json") as f:
            report = json.load(f)
            last_run = datetime.fromisoformat(report["end_time"])
    except Exception:
        return {
            "status": "critical",
            "last_run": None,
            "hours_since_last_run": None,
            "threshold_hours": 25
        }

    hrs = hours_diff(last_run)
    status = "ok" if hrs <= 25 else "critical"

    return {
        "status": status,
        "last_run": last_run.isoformat(),
        "hours_since_last_run": round(hrs, 2),
        "threshold_hours": 25
    }

def check_data_freshness(conn):
    cur = conn.cursor()

    cur.execute("""
        SELECT
            (SELECT MAX(created_at) FROM production.customers),
            (SELECT MAX(created_at) FROM production.transactions),
            (
                SELECT MAX(d.full_date)
                FROM warehouse.fact_sales f
                JOIN warehouse.dim_date d
                ON f.date_key = d.date_key
            );
    """)

    prod_ts, trans_ts, wh_date = cur.fetchone()
    cur.close()

    # convert DATE to datetime for comparison
    if wh_date:
        wh_ts = datetime.combine(wh_date, datetime.min.time(), tzinfo=timezone.utc)
    else:
        wh_ts = None

    lags = []
    for ts in [prod_ts, trans_ts, wh_ts]:
        if ts:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            lags.append((utc_now() - ts).total_seconds() / 3600)

    max_lag = max(lags) if lags else None

    status = "ok"
    if max_lag is None or max_lag > 24:
        status = "critical"
    elif max_lag > 2:
        status = "warning"

    return {
        "status": status,
        "production_latest_record": prod_ts.isoformat() if prod_ts else None,
        "warehouse_latest_record": wh_ts.isoformat() if wh_ts else None,
        "max_lag_hours": round(max_lag, 2) if max_lag else None
    }

def check_volume_anomalies(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT DATE(t.transaction_date), COUNT(*)
        FROM production.transactions t
        GROUP BY DATE(t.transaction_date)
        ORDER BY DATE(t.transaction_date) DESC
        LIMIT 30;
    """)
    rows = cur.fetchall()
    cur.close()

    if len(rows) < 5:
        return {"status": "ok", "anomaly_detected": False}

    counts = [r[1] for r in rows]
    today_count = counts[0]
    mean = statistics.mean(counts)
    std = statistics.stdev(counts)

    anomaly = today_count > mean + 3*std or today_count < mean - 3*std

    return {
        "status": "anomaly_detected" if anomaly else "ok",
        "expected_range": f"{int(mean-3*std)}-{int(mean+3*std)}",
        "actual_count": today_count,
        "anomaly_detected": anomaly,
        "anomaly_type": "spike" if today_count > mean else "drop" if anomaly else None
    }

def check_data_quality(conn):
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*)
        FROM production.transaction_items ti
        LEFT JOIN production.transactions t
        ON ti.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL;
    """)
    orphan_items = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM production.customers
        WHERE email IS NULL;
    """)
    nulls = cur.fetchone()[0]

    cur.close()

    score = 100
    if orphan_items > 0:
        score -= 30
    if nulls > 0:
        score -= 20

    status = "ok" if score >= 95 else "degraded"

    return {
        "status": status,
        "quality_score": score,
        "orphan_records": orphan_items,
        "null_violations": nulls
    }

def check_db_health(conn):
    start = time.time()
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    cur.fetchone()

    cur.execute("""
        SELECT COUNT(*) FROM pg_stat_activity;
    """)
    connections = cur.fetchone()[0]
    cur.close()

    response_ms = (time.time() - start) * 1000

    return {
        "status": "ok",
        "response_time_ms": round(response_ms, 2),
        "connections_active": connections
    }

# ---------- MAIN ----------

def main():
    conn = get_conn()
    alerts = []

    last_exec = check_last_execution()
    freshness = check_data_freshness(conn)
    volume = check_volume_anomalies(conn)
    quality = check_data_quality(conn)
    db = check_db_health(conn)

    if last_exec["status"] == "critical":
        alerts.append({
            "severity": "critical",
            "check": "last_execution",
            "message": "Pipeline has not run in last 25 hours",
            "timestamp": utc_now().isoformat()
        })

    if volume.get("anomaly_detected"):
        alerts.append({
            "severity": "warning",
            "check": "data_volume",
            "message": "Transaction volume anomaly detected",
            "timestamp": utc_now().isoformat()
        })

    overall = "healthy"
    if alerts:
        overall = "critical" if any(a["severity"] == "critical" for a in alerts) else "degraded"

    report = {
        "monitoring_timestamp": utc_now().isoformat(),
        "pipeline_health": overall,
        "checks": {
            "last_execution": last_exec,
            "data_freshness": freshness,
            "data_volume_anomalies": volume,
            "data_quality": quality,
            "database_connectivity": db
        },
        "alerts": alerts,
        "overall_health_score": quality["quality_score"]
    }

    os.makedirs("data/processed", exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(report, f, indent=4)

    conn.close()
    print("Monitoring report generated successfully")

if __name__ == "__main__":
    main()