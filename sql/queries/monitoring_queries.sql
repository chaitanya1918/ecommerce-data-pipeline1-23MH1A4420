-- ==============================
-- 1. DATA FRESHNESS
-- ==============================
SELECT
    MAX(loaded_at) AS latest_staging
FROM staging.customers;

SELECT
    MAX(created_at) AS latest_production
FROM production.transactions;

SELECT
    MAX(created_at) AS latest_warehouse
FROM warehouse.fact_sales;

-- ==============================
-- 2. DAILY TRANSACTION VOLUME (Last 30 Days)
-- ==============================
SELECT
    DATE(transaction_date) AS day,
    COUNT(*) AS transaction_count
FROM production.transactions
WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(transaction_date)
ORDER BY day;

-- ==============================
-- 3. DATA QUALITY CHECKS
-- ==============================
-- Orphan transaction_items
SELECT COUNT(*) AS orphan_items
FROM production.transaction_items ti
LEFT JOIN production.transactions t
ON ti.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- Null violations
SELECT COUNT(*) AS null_customers
FROM production.customers
WHERE customer_id IS NULL OR email IS NULL;

-- ==============================
-- 4. DATABASE STATS
-- ==============================
SELECT
    COUNT(*) AS active_connections
FROM pg_stat_activity;

SELECT
    pg_database_size(current_database()) AS database_size_bytes;
