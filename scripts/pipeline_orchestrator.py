import time
import json
import logging
from datetime import datetime
import subprocess
import sys

# ---------------- LOGGING SETUP ----------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = f"logs/pipeline_orchestrator_{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

# ---------------- HELPER FUNCTION ----------------
def run_step(name, command, retries=3):
    logging.info(f"STARTING STEP: {name}")
    start = time.time()

    for attempt in range(1, retries + 1):
        try:
            subprocess.run(command, check=True)
            duration = round(time.time() - start, 2)
            logging.info(f"COMPLETED STEP: {name} in {duration}s")
            return {
                "status": "success",
                "duration_seconds": duration,
                "retry_attempts": attempt - 1
            }
        except Exception as e:
            logging.error(f"{name} failed (attempt {attempt}): {e}")
            if attempt < retries:
                wait = 2 ** (attempt - 1)
                time.sleep(wait)
            else:
                return {
                    "status": "failed",
                    "duration_seconds": round(time.time() - start, 2),
                    "retry_attempts": retries,
                    "error_message": str(e)
                }

# ---------------- PIPELINE ----------------
def main():
    pipeline_start = time.time()
    report = {
        "pipeline_execution_id": f"PIPE_{timestamp}",
        "start_time": datetime.utcnow().isoformat(),
        "steps_executed": {},
        "errors": [],
        "warnings": []
    }

    steps = [
        ("data_generation", ["python", "scripts/data_generation/generate_data.py"]),
        ("data_ingestion", ["python", "scripts/ingestion/ingest_to_staging.py"]),
        ("data_quality", ["python", "scripts/quality_checks/validate_data.py"]),
        ("staging_to_production", ["python", "scripts/transformation/staging_to_production.py"]),
        ("warehouse_load", ["python", "scripts/transformation/load_warehouse.py"]),
        ("analytics_generation", ["python", "scripts/transformation/generate_analytics.py"])
    ]

    for name, cmd in steps:
        result = run_step(name, cmd)
        report["steps_executed"][name] = result

        if result["status"] == "failed":
            report["status"] = "failed"
            report["errors"].append(f"{name} failed")
            break

    report["end_time"] = datetime.utcnow().isoformat()
    report["total_duration_seconds"] = round(time.time() - pipeline_start, 2)
    report.setdefault("status", "success")

    # Save report
    with open("data/processed/pipeline_execution_report.json", "w") as f:
        json.dump(report, f, indent=4)

    logging.info("PIPELINE EXECUTION FINISHED")

if __name__ == "__main__":
    main()
