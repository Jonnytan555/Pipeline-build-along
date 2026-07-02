MODEL (
    name pipeline.dim_product,
    kind FULL,
    description 'One row per unique product — surrogate key + name + category',
    audits (
        not_null(columns = (product_key, product_name)),
        unique_values(columns = (product_key,))
    )
);

WITH source AS (
    SELECT DISTINCT product_name, category
    FROM pipeline.stg_orders_enriched
)

SELECT
    ROW_NUMBER() OVER (ORDER BY product_name)  AS product_key,
    product_name,
    category
FROM source
