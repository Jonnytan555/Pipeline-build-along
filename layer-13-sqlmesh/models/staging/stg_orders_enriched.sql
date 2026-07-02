-- SQLMesh staging model.
-- MODEL block declares metadata; SELECT below is the actual transform.
MODEL (
    name pipeline.stg_orders_enriched,
    kind VIEW,                          -- materialised as a database view
    description 'Cleaned orders — nulls filtered, types cast, status lowercased',
    audits (
        not_null(columns = (order_id, customer_name, total_amount)),
        unique_values(columns = (order_id,))
    )
);

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
FROM public.orders_enriched
WHERE order_id IS NOT NULL
