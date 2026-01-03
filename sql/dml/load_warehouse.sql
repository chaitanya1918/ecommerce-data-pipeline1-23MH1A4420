-- DIM CUSTOMERS
INSERT INTO warehouse.dim_customers (
    customer_code, full_name, email, city, state, country,
    start_date, is_current
)
SELECT DISTINCT
    customer_id,
    first_name || ' ' || last_name,
    email,
    city,
    state,
    country,
    NOW(),
    TRUE
FROM production.customers;

-- DIM PRODUCTS
INSERT INTO warehouse.dim_products (
    product_code, product_name, category, subcategory,
    price, start_date, is_current
)
SELECT DISTINCT
    product_id,
    product_name,
    category,
    sub_category,
    selling_price,
    NOW(),
    TRUE
FROM production.products;

-- DIM DATE
INSERT INTO warehouse.dim_date (date_key, year, month, day, quarter)
SELECT DISTINCT
    transaction_date,
    EXTRACT(YEAR FROM transaction_date),
    EXTRACT(MONTH FROM transaction_date),
    EXTRACT(DAY FROM transaction_date),
    EXTRACT(QUARTER FROM transaction_date)
FROM production.transactions;

-- FACT SALES
INSERT INTO warehouse.fact_sales (
    customer_key, product_key, date_key,
    quantity, total_amount
)
SELECT
    dc.customer_key,
    dp.product_key,
    dd.date_key,
    ti.quantity,
    ti.total_price
FROM production.transaction_items ti
JOIN production.transactions t
    ON ti.transaction_id = t.transaction_id
JOIN warehouse.dim_customers dc
    ON dc.customer_code = t.customer_id AND dc.is_current = TRUE
JOIN warehouse.dim_products dp
    ON dp.product_code = ti.product_id AND dp.is_current = TRUE
JOIN warehouse.dim_date dd
    ON dd.date_key = t.transaction_date;