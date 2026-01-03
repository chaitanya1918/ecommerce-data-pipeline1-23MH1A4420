SELECT
    d.year,
    d.month,
    COALESCE(SUM(f.total_amount), 0) AS total_sales
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d
    ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;