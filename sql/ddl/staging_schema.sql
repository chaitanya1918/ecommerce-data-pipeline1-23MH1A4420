CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id VARCHAR(20),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    phone VARCHAR(50),
    registration_date DATE,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    age_group VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id VARCHAR(20),
    product_name VARCHAR(200),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    cost_price NUMERIC(10,2),
    selling_price NUMERIC(10,2),
    supplier_name VARCHAR(200),
    stock_quantity INT,
    supplier_id VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id VARCHAR(30),
    customer_id VARCHAR(20),
    transaction_date DATE,
    payment_method VARCHAR(50),
    total_amount NUMERIC(12,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.transaction_items (
    transaction_item_id SERIAL,
    transaction_id VARCHAR(30),
    product_id VARCHAR(20),
    quantity INT,
    price NUMERIC(10,2),
    line_total NUMERIC(12,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);