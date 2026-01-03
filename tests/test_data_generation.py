import os

def test_raw_data_files_exist():
    files = [
        "data/raw/customers.csv",
        "data/raw/products.csv",
        "data/raw/transactions.csv",
        "data/raw/transaction_items.csv"
    ]

    for file in files:
        assert os.path.exists(file), f"{file} does not exist"


def test_raw_data_not_empty():
    files = [
        "data/raw/customers.csv",
        "data/raw/products.csv",
        "data/raw/transactions.csv",
        "data/raw/transaction_items.csv"
    ]

    for file in files:
        assert os.path.getsize(file) > 0, f"{file} is empty"