import psycopg2
import yaml

with open("config/config.yaml") as f:
    db = yaml.safe_load(f)["database"]

conn = psycopg2.connect(
    host=db["host"],
    port=db["port"],
    dbname=db["name"],
    user=db["user"],
    password=db["password"]
)

print("DATABASE CONNECTION SUCCESSFUL")
conn.close()