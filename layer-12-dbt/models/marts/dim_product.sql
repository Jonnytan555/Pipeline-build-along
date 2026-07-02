-- Dimension: one row per unique product.
WITH source AS (
    SELECT DISTINCT product_name, category
    FROM {{ ref('stg_orders_enriched') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY product_name)  AS product_key,
    product_name,
    category
FROM source
