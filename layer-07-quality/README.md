# Layer 7 — Data Quality Validation

Validates data in PostgreSQL before it is promoted to the warehouse. Acts as a quality gate — bad data is caught here and blocked from propagating to downstream reports and ML models.

## What it does

- Checks row counts, null rates, business-rule violations, and referential integrity
- Validates data freshness (rejects stale batches)
- Provides two implementations: a custom pandas checker and a Great Expectations suite

## Files

| File | Purpose |
|---|---|
| `validate.py` | Custom validation engine (no extra dependencies) |
| `validate_ge.py` | Great Expectations integration (structured results + HTML reports) |

## Run

```bash
make l7-run
# or: python layer-07-quality/validate.py
```

## Checks performed

### `validate.py`

| Check | Tables | Rule |
|---|---|---|
| Completeness | all | At least 1 row |
| Null keys | `orders_enriched` | `order_id`, `customer_name` must not be null |
| Non-negative amounts | `orders_enriched`, `revenue_*` | `total_amount >= 0`, `quantity >= 0` |
| Allowed status values | `orders_enriched` | `pending`, `shipped`, `delivered` only |
| Referential integrity | `orders_enriched` | No orphaned rows (customer missing from dim) |
| Freshness | `orders_enriched` | `order_date` no older than 365 days |

### `validate_ge.py`

Uses Great Expectations (GE) for the same checks but with:
- Structured `ExpectationSuite` stored as JSON (reusable, version-controllable)
- Auto-generated HTML **Data Docs** report per run
- Typed `CheckpointResult` output suitable for Airflow task state

## Tables validated

- `orders_enriched`
- `revenue_by_customer`
- `revenue_by_category`

## Pipeline position

```
Layer 3/4 (Spark/Airflow writes to PostgreSQL)
      ↓
[Layer 7: validate]  ← blocks promotion on failure
      ↓
Layer 8 (star schema warehouse load)
```

In the Airflow DAG, the validate task uses `TriggerRule.ALL_SUCCESS` — if validation fails, the downstream load tasks are skipped automatically.
