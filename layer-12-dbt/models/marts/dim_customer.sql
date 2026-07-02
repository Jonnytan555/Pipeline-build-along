-- Dimension: one row per unique customer.
-- ROW_NUMBER gives a stable surrogate key within each full-refresh run.
WITH source AS (
    SELECT DISTINCT customer_name, country
    FROM {{ ref('stg_orders_enriched') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY customer_name)  AS customer_key,
    customer_name,
    country
FROM source
