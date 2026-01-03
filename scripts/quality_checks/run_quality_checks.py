import psycopg2
import yaml
import json
from datetime import datetime

from validate_data import (
    check_null_values,
    check_duplicates,
    check_referential_integrity,
    check_data_ranges,
    calculate_quality_score
)

# ---------------------------------------
# LOAD CONFIG
# ---------------------------------------
with open("config/config.yaml") as f:
    DB = yaml.safe_load(f)["database"]

# ---------------------------------------
# DATABASE CONNECTION
# ---------------------------------------
connection = psycopg2.connect(
    host=DB["host"],
    port=DB["port"],
    dbname=DB["name"],
    user=DB["user"],
    password=DB["password"]
)

# ---------------------------------------
# RUN QUALITY CHECKS
# ---------------------------------------
results = {
    "nulls": check_null_values(connection, "staging"),
    "duplicates": check_duplicates(connection, "staging"),
    "referential_issues": check_referential_integrity(connection, "staging"),
    "range_issues": check_data_ranges(connection, "staging")
}

results["quality_score"] = calculate_quality_score(results)
results["checked_at"] = datetime.now().isoformat()

connection.close()

# ---------------------------------------
# SAVE REPORT
# ---------------------------------------
output_path = "data/staging/quality_report.json"
with open(output_path, "w") as f:
    json.dump(results, f, indent=4)

print("QUALITY CHECK COMPLETED")
print(f"Report saved to: {output_path}")