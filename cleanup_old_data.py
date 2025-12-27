import os
import time
import logging
from datetime import datetime, timedelta

RETENTION_DAYS = 7
BASE_DIRS = ["data/raw", "data/staging", "logs"]

logging.basicConfig(
    filename="logs/scheduler_activity.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

cutoff = time.time() - (RETENTION_DAYS * 86400)

for base in BASE_DIRS:
    if not os.path.exists(base):
        continue

    for root, dirs, files in os.walk(base):
        for file in files:
            path = os.path.join(root, file)
            if "report" in file or "summary" in file:
                continue
            if os.path.getmtime(path) < cutoff:
                os.remove(path)
                logging.info(f"Deleted old file: {path}")