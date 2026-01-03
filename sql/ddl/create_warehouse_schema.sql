CREATE SCHEMA IF NOT EXISTS warehouse;

-- =========================
-- DIMENSION TABLES
-- =========================

CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id INT,
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
    product_id INT,
    product_name TEXT,
    category TEXT,
    price NUMERIC,
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

CREATE TABLE IF NOT EXISTS warehouse.dim_payment_method (
    payment_key SERIAL PRIMARY KEY,
    payment_method TEXT
);

-- =========================
-- FACT TABLE
-- =========================

CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    sales_key SERIAL PRIMARY KEY,
    date_key DATE REFERENCES warehouse.dim_date(date_key),
    customer_key INT REFERENCES warehouse.dim_customers(customer_key),
    product_key INT REFERENCES warehouse.dim_products(product_key),
    payment_key INT REFERENCES warehouse.dim_payment_method(payment_key),
    quantity INT,
    total_amount NUMERIC
);

-- =========================
-- AGGREGATE TABLES
-- =========================

CREATE TABLE IF NOT EXISTS warehouse.agg_daily_sales (
    date_key DATE,
    total_sales NUMERIC,
    total_orders INT
);

CREATE TABLE IF NOT EXISTS warehouse.agg_product_performance (
    product_key INT,
    total_quantity INT,
    total_revenue NUMERIC
);

CREATE TABLE IF NOT EXISTS warehouse.agg_customer_metrics (
    customer_key INT,
    total_orders INT,
    lifetime_value NUMERIC
);