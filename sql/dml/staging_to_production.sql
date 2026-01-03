-- Customers
INSERT INTO production.customers (
    customer_id, first_name, last_name, email, phone,
    registration_date, city, state, country, age_group
)
SELECT
    customer_id, first_name, last_name, email, phone,
    registration_date, city, state, country, age_group
FROM staging.customers
ON CONFLICT (customer_id) DO NOTHING;

-- Products
INSERT INTO production.products (
    product_id, product_name, category, sub_category,
    cost_price, selling_price, supplier_name, stock_quantity, supplier_id
)
SELECT
    product_id, product_name, category, sub_category,
    cost_price, selling_price, supplier_name, stock_quantity, supplier_id
FROM staging.products
ON CONFLICT (product_id) DO NOTHING;

-- Transactions
INSERT INTO production.transactions (
    transaction_id, customer_id, transaction_date, payment_method
)
SELECT
    transaction_id, customer_id, transaction_date, payment_method
FROM staging.transactions
ON CONFLICT (transaction_id) DO NOTHING;

-- Transaction items
INSERT INTO production.transaction_items (
    item_id, transaction_id, product_id,
    quantity, unit_price, discount_percentage, total_price
)
SELECT
    item_id, transaction_id, product_id,
    quantity, unit_price, discount, total_price
FROM staging.transaction_items
ON CONFLICT (item_id) DO NOTHING;