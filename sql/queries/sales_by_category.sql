SELECT
    p.category,
    COALESCE(SUM(f.total_amount), 0) AS total_sales
FROM warehouse.fact_sales f
JOIN warehouse.dim_products p
    ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_sales DESC;