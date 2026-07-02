MODEL (
    name pipeline.dim_customer,
    kind FULL,                          -- full refresh on every run
    description 'One row per unique customer — surrogate key + name + country',
    audits (
        not_null(columns = (customer_key, customer_name)),
        unique_values(columns = (customer_key,))
    )
);

WITH source AS (
    SELECT DISTINCT customer_name, country
    FROM pipeline.stg_orders_enriched
)

SELECT
    ROW_NUMBER() OVER (ORDER BY customer_name)  AS customer_key,
    customer_name,
    country
FROM source
