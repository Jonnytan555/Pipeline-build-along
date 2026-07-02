-- Custom audit: fails if any order has a non-positive amount.
-- SQLMesh audits run after the model is built; any returned rows = failure.
AUDIT (name assert_positive_amounts);

SELECT order_id, total_amount
FROM @this_model
WHERE total_amount <= 0
