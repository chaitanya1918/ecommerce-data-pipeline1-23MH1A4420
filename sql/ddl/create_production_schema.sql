CREATE SCHEMA IF NOT EXISTS production;

CREATE TABLE IF NOT EXISTS production.customers (
  customer_id VARCHAR(20) PRIMARY KEY,
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  email VARCHAR(100) UNIQUE,
  phone VARCHAR(30),
  registration_date DATE,
  city VARCHAR(50),
  state VARCHAR(50),
  country VARCHAR(100),
  age_group VARCHAR(20),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS production.products (
  product_id VARCHAR(20) PRIMARY KEY,
  product_name VARCHAR(100),
  category VARCHAR(50),
  sub_category VARCHAR(50),
  price DECIMAL(10,2),
  cost DECIMAL(10,2),
  brand VARCHAR(50),
  stock_quantity INT,
  supplier_id VARCHAR(20),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS production.transactions (
  transaction_id VARCHAR(20) PRIMARY KEY,
  customer_id VARCHAR(20) REFERENCES production.customers(customer_id),
  transaction_date DATE,
  transaction_time TIME,
  payment_method VARCHAR(50),
  shipping_address TEXT,
  total_amount DECIMAL(12,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS production.transaction_items (
  item_id VARCHAR(20) PRIMARY KEY,
  transaction_id VARCHAR(20) REFERENCES production.transactions(transaction_id),
  product_id VARCHAR(20) REFERENCES production.products(product_id),
  quantity INT,
  unit_price DECIMAL(10,2),
  discount_percentage DECIMAL(5,2),
  line_total DECIMAL(12,2)
);
