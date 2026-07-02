MODEL (
    name pipeline.revenue_by_category,
    kind FULL,
    description 'Pre-aggregated revenue per product category',
    audits (not_null(columns = (category,)))
);

SELECT
    p.category,
    COUNT(f.order_id)              AS order_count,
    ROUND(SUM(f.total_amount), 2)  AS total_revenue,
    ROUND(AVG(f.total_amount), 2)  AS avg_order_value,
    CURRENT_DATE                   AS as_of_date
FROM pipeline.fact_orders AS f
LEFT JOIN pipeline.dim_product AS p USING (product_key)
GROUP BY p.category
