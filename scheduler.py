import schedule
import time
import subprocess
import logging
from datetime import datetime

logging.basicConfig(
    filename="logs/scheduler_activity.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def run_pipeline():
    logging.info("Starting scheduled pipeline execution")
    try:
        subprocess.run(
            ["python", "scripts/pipeline_orchestrator.py"],
            check=True
        )
        logging.info("Pipeline completed successfully")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")

schedule.every().day.at("10:00").do(run_pipeline)

logging.info("Scheduler started")

while True:
    schedule.run_pending()
    time.sleep(60)