-- Staging model: light cleanup only — rename, cast, filter nulls.
-- No business logic here; that lives in the mart models.
WITH source AS (
    SELECT * FROM {{ source('pipeline', 'orders_enriched') }}
)

SELECT
    order_id::INT                            AS order_id,
    customer_name::VARCHAR                   AS customer_name,
    COALESCE(country, 'Unknown')::VARCHAR    AS country,
    product_name::VARCHAR                    AS product_name,
    category::VARCHAR                        AS category,
    quantity::INT                            AS quantity,
    unit_price::NUMERIC(10, 2)               AS unit_price,
    total_amount::NUMERIC(10, 2)             AS total_amount,
    LOWER(status)::VARCHAR                   AS status,
    order_date::DATE                         AS order_date
FROM source
WHERE order_id IS NOT NULL
