CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_code VARCHAR(20),
    full_name TEXT,
    email TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN
);

CREATE TABLE IF NOT EXISTS warehouse.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_code VARCHAR(20),
    product_name TEXT,
    category TEXT,
    subcategory TEXT,
    selling_price NUMERIC,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN
);

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    quarter INT
);

CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    customer_key INT REFERENCES warehouse.dim_customers(customer_key),
    product_key INT REFERENCES warehouse.dim_products(product_key),
    date_key DATE REFERENCES warehouse.dim_date(date_key),
    quantity INT,
    total_amount NUMERIC
);