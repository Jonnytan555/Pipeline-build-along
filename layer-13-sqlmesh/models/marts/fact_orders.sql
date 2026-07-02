MODEL (
    name pipeline.fact_orders,
    kind FULL,
    description 'One row per order — FK references to dim_customer and dim_product',
    audits (
        not_null(columns = (order_id, customer_key, product_key, total_amount)),
        unique_values(columns = (order_id,)),
        assert_positive_amounts
    )
);

SELECT
    o.order_id,
    c.customer_key,
    p.product_key,
    o.quantity,
    o.unit_price,
    o.total_amount,
    o.status,
    o.order_date
FROM pipeline.stg_orders_enriched AS o
LEFT JOIN pipeline.dim_customer AS c USING (customer_name)
LEFT JOIN pipeline.dim_product  AS p USING (product_name)
