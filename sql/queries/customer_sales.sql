SELECT
    c.full_name,
    COUNT(*) AS total_orders,
    COALESCE(SUM(f.total_amount), 0) AS total_spent
FROM warehouse.fact_sales f
JOIN warehouse.dim_customers c
    ON f.customer_key = c.customer_key
GROUP BY c.full_name
ORDER BY total_spent DESC;