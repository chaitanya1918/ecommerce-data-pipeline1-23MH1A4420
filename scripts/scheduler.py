import schedule
import time
import subprocess
import logging
import os

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/scheduler_activity.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def run_pipeline():
    logging.info("Scheduled pipeline started")
    try:
        subprocess.run(
            ["python", "scripts/pipeline_orchestrator.py"],
            check=True
        )
        logging.info("Pipeline finished successfully")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")

schedule.every().day.at("10:00").do(run_pipeline)

logging.info("Scheduler is running")

while True:
    schedule.run_pending()
    time.sleep(60)