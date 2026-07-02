-- Pre-aggregated revenue per product category.
-- Replaces the pandas groupBy in Layer 3 / Layer 4.
SELECT
    p.category,
    COUNT(f.order_id)              AS order_count,
    ROUND(SUM(f.total_amount), 2)  AS total_revenue,
    ROUND(AVG(f.total_amount), 2)  AS avg_order_value,
    CURRENT_DATE                   AS as_of_date
FROM {{ ref('fact_orders') }} f
LEFT JOIN {{ ref('dim_product') }} p USING (product_key)
GROUP BY p.category
