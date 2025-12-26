import os
import json
import random
from datetime import datetime, timezone
from faker import Faker
import pandas as pd
import yaml

# --------------------------------------------------
# Setup
# --------------------------------------------------
fake = Faker()

with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

RAW_PATH = "data/raw"
os.makedirs(RAW_PATH, exist_ok=True)

CUSTOMER_COUNT = config["data_generation"]["customers"]["record_count"]
PRODUCT_COUNT = config["data_generation"]["products"]["record_count"]
TRANSACTION_COUNT = config["data_generation"]["orders"]["record_count"]

# --------------------------------------------------
# Generate Customers
# --------------------------------------------------
def generate_customers():
    customers = []

    for i in range(1, CUSTOMER_COUNT + 1):
        customers.append({
            "customer_id": f"CUST{i:04d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "phone": fake.phone_number(),
            "registration_date": fake.date_between(start_date="-3y", end_date="today"),
            "city": fake.city(),
            "state": fake.state(),
            "country": fake.country(),
            "age_group": random.choice(["18-25", "26-35", "36-45", "46-60", "60+"])
        })

    df = pd.DataFrame(customers)
    df.to_csv(f"{RAW_PATH}/customers.csv", index=False)
    return df

# --------------------------------------------------
# Generate Products
# --------------------------------------------------
def generate_products():
    categories = {
        "Electronics": (5000, 50000),
        "Clothing": (500, 5000),
        "Home & Kitchen": (800, 15000),
        "Books": (200, 2000),
        "Sports": (600, 12000),
        "Beauty": (300, 8000)
    }

    products = []

    for i in range(1, PRODUCT_COUNT + 1):
        category = random.choice(list(categories.keys()))
        price = round(random.uniform(*categories[category]), 2)
        cost = round(price * random.uniform(0.6, 0.85), 2)

        products.append({
            "product_id": f"PROD{i:04d}",
            "product_name": fake.word().title(),
            "category": category,
            "sub_category": fake.word().title(),
            "price": price,
            "cost": cost,
            "brand": fake.company(),
            "stock_quantity": random.randint(10, 500),
            "supplier_id": f"SUP{random.randint(1,50):03d}"
        })

    df = pd.DataFrame(products)
    df.to_csv(f"{RAW_PATH}/products.csv", index=False)
    return df

# --------------------------------------------------
# Generate Transactions & Items
# --------------------------------------------------
def generate_transactions(customers_df, products_df):
    transactions = []
    items = []

    customer_ids = customers_df["customer_id"].tolist()
    product_ids = products_df["product_id"].tolist()
    product_price = dict(zip(products_df["product_id"], products_df["price"]))

    item_counter = 1

    for i in range(1, TRANSACTION_COUNT + 1):
        txn_id = f"TXN{i:05d}"
        txn_total = 0.0

        for _ in range(random.randint(1, 5)):
            product_id = random.choice(product_ids)
            quantity = random.randint(1, 5)
            discount = random.choice([0, 5, 10, 15])
            unit_price = product_price[product_id]

            line_total = round(
                quantity * unit_price * (1 - discount / 100), 2
            )
            txn_total += line_total

            items.append({
                "item_id": f"ITEM{item_counter:05d}",
                "transaction_id": txn_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_percentage": discount,
                "line_total": line_total
            })

            item_counter += 1

        transactions.append({
            "transaction_id": txn_id,
            "customer_id": random.choice(customer_ids),
            "transaction_date": fake.date_this_year(),
            "transaction_time": fake.time(),
            "payment_method": random.choice([
                "Credit Card", "Debit Card", "UPI",
                "Cash on Delivery", "Net Banking"
            ]),
            "shipping_address": fake.address().replace("\n", ", "),
            "total_amount": round(txn_total, 2)
        })

    pd.DataFrame(transactions).to_csv(f"{RAW_PATH}/transactions.csv", index=False)
    pd.DataFrame(items).to_csv(f"{RAW_PATH}/transaction_items.csv", index=False)

    return transactions, items

# --------------------------------------------------
# Validation
# --------------------------------------------------
def validate_referential_integrity(customers, products, transactions, items):
    issues = 0

    if not set(t["customer_id"] for t in transactions).issubset(
        set(customers["customer_id"])
    ):
        issues += 1

    if not set(i["product_id"] for i in items).issubset(
        set(products["product_id"])
    ):
        issues += 1

    if not set(i["transaction_id"] for i in items).issubset(
        set(t["transaction_id"] for t in transactions)
    ):
        issues += 1

    score = 100 if issues == 0 else max(0, 100 - issues * 20)

    return {
        "orphan_records": issues,
        "constraint_violations": issues,
        "data_quality_score": score
    }

# --------------------------------------------------
# Main
# --------------------------------------------------
if __name__ == "__main__":
    customers_df = generate_customers()
    products_df = generate_products()
    transactions, items = generate_transactions(customers_df, products_df)

    validation = validate_referential_integrity(
        customers_df, products_df, transactions, items
    )

    metadata = {
        "generation_timestamp": datetime.now(timezone.utc).isoformat(),
        "record_counts": {
            "customers": len(customers_df),
            "products": len(products_df),
            "transactions": len(transactions),
            "transaction_items": len(items)
        },
        "data_quality": validation
    }

    with open(f"{RAW_PATH}/generation_metadata.json", "w") as f:
        json.dump(metadata, f, indent=4)

    print("Data generation completed successfully")
