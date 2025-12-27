import os
import time
import logging

RETENTION_DAYS = 7
FOLDERS = ["data/raw", "data/staging", "logs"]

logging.basicConfig(
    filename="logs/scheduler_activity.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

now = time.time()
cutoff = now - (RETENTION_DAYS * 86400)

for folder in FOLDERS:
    if not os.path.exists(folder):
        continue

    for root, dirs, files in os.walk(folder):
        for file in files:
            if "report" in file or "summary" in file:
                continue

            path = os.path.join(root, file)
            if os.path.getmtime(path) < cutoff:
                os.remove(path)
                logging.info(f"Deleted old file: {path}")