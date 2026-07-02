-- Singular test: fails if ANY row has total_amount <= 0.
-- dbt fails the test if this query returns any rows.
SELECT order_id, total_amount
FROM {{ ref('fact_orders') }}
WHERE total_amount <= 0
