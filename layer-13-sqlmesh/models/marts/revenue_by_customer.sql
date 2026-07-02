MODEL (
    name pipeline.revenue_by_customer,
    kind FULL,
    description 'Pre-aggregated revenue per customer',
    audits (not_null(columns = (customer_name,)))
);

SELECT
    c.customer_name,
    c.country,
    COUNT(f.order_id)              AS order_count,
    ROUND(SUM(f.total_amount), 2)  AS total_revenue,
    ROUND(AVG(f.total_amount), 2)  AS avg_order_value,
    CURRENT_DATE                   AS as_of_date
FROM pipeline.fact_orders AS f
LEFT JOIN pipeline.dim_customer AS c USING (customer_key)
GROUP BY c.customer_name, c.country
