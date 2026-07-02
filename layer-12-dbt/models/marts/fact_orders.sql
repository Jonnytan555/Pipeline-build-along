-- Fact table: one row per order, FK references to dim tables.
-- Replaces the hand-written SQL in layer-08-warehouse/build_facts.py.
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders_enriched') }}
),
customers AS (
    SELECT * FROM {{ ref('dim_customer') }}
),
products AS (
    SELECT * FROM {{ ref('dim_product') }}
)

SELECT
    o.order_id,
    c.customer_key,
    p.product_key,
    o.quantity,
    o.unit_price,
    o.total_amount,
    o.status,
    o.order_date
FROM orders o
LEFT JOIN customers c USING (customer_name)
LEFT JOIN products  p USING (product_name)
