-- Pre-aggregated revenue per customer.
SELECT
    c.customer_name,
    c.country,
    COUNT(f.order_id)              AS order_count,
    ROUND(SUM(f.total_amount), 2)  AS total_revenue,
    ROUND(AVG(f.total_amount), 2)  AS avg_order_value,
    CURRENT_DATE                   AS as_of_date
FROM {{ ref('fact_orders') }} f
LEFT JOIN {{ ref('dim_customer') }} c USING (customer_key)
GROUP BY c.customer_name, c.country
