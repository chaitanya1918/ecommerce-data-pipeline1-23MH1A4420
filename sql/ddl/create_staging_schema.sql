CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.customers (
  customer_id VARCHAR(20),
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  email VARCHAR(100),
  phone VARCHAR(30),
  registration_date DATE,
  city VARCHAR(50),
  state VARCHAR(50),
  country VARCHAR(100),
  age_group VARCHAR(20),
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.products (
  product_id VARCHAR(20),
  product_name VARCHAR(100),
  category VARCHAR(50),
  sub_category VARCHAR(50),
  price DECIMAL(10,2),
  cost DECIMAL(10,2),
  brand VARCHAR(50),
  stock_quantity INT,
  supplier_id VARCHAR(20),
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.transactions (
  transaction_id VARCHAR(20),
  customer_id VARCHAR(20),
  transaction_date DATE,
  transaction_time TIME,
  payment_method VARCHAR(50),
  shipping_address TEXT,
  total_amount DECIMAL(12,2),
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.transaction_items (
  item_id VARCHAR(20),
  transaction_id VARCHAR(20),
  product_id VARCHAR(20),
  quantity INT,
  unit_price DECIMAL(10,2),
  discount_percentage DECIMAL(5,2),
  line_total DECIMAL(12,2),
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
