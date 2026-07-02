# Layer 13 — SQLMesh

Same star schema as [Layer 12 (dbt)](../layer-12-dbt/) but built with SQLMesh — a state-aware transformation framework that only re-runs models that have actually changed, with built-in virtual environments and column-level lineage.

## What it does

- Builds the same 7-model pipeline (staging → dims → fact → aggregates) as Layer 12
- Tracks model state: unchanged models are skipped automatically
- Declares audits inside the `MODEL` block — no separate test YAML
- Supports zero-copy promotion from dev → prod via virtual environments

## Files

```
layer-13-sqlmesh/
  config.py                          PostgreSQL connection (reads env vars)
  models/
    staging/
      stg_orders_enriched.sql        clean + cast orders (VIEW)
    marts/
      dim_customer.sql               unique customers (FULL)
      dim_product.sql                unique products (FULL)
      fact_orders.sql                order facts with FKs (FULL)
      revenue_by_category.sql        aggregated by category (FULL)
      revenue_by_customer.sql        aggregated by customer (FULL)
  audits/
    assert_positive_amounts.sql      custom audit — fails if amount <= 0
  run_sqlmesh.py                     walkthrough script
```

## Run

```bash
cd layer-13-sqlmesh/
pip install sqlmesh

# Apply changes to a dev environment (shows what will change first)
sqlmesh plan dev

# Execute scheduled models
sqlmesh run

# Run all audits
sqlmesh audit

# Browser UI with column-level lineage DAG
sqlmesh ui
```

## SQLMesh vs dbt (Layer 12)

| | dbt (Layer 12) | SQLMesh (Layer 13) |
|---|---|---|
| Dependency resolution | `ref()` + YAML | `MODEL` block + SQL refs |
| Change detection | Reruns everything | State-aware — only changed models |
| Tests / audits | Separate YAML + SQL | Declared inside `MODEL` block |
| Environments | Requires full copy | Virtual (zero data duplication) |
| Column lineage | Plugin (dbt-column-lineage) | Built-in |
| Python models | Experimental | First-class |
| Incremental logic | You write the WHERE | Framework handles partitioning |
| Maturity | Industry standard (2016) | Newer, growing fast (2022) |

## Key SQLMesh concepts

### MODEL block (replaces dbt schema.yml)
```sql
MODEL (
    name pipeline.fact_orders,
    kind FULL,
    audits (
        not_null(columns = (order_id,)),
        assert_positive_amounts       -- custom audit defined in audits/
    )
);
SELECT ...
```

### `sqlmesh plan` (like `terraform plan` for data)
```
$ sqlmesh plan dev
Summary of differences:
  Models:
    + pipeline.stg_orders_enriched (added)
    + pipeline.fact_orders (added)
  Audits: 3 audits will run after apply
Apply? [y/N]
```

### Virtual environments
```bash
sqlmesh plan dev    # dev schema — isolated from prod
sqlmesh plan prod   # promote to prod — ZERO data copy for unchanged models
```

### Column-level lineage
```bash
sqlmesh lineage pipeline.fact_orders total_amount
# → total_amount comes from stg_orders_enriched.total_amount
#   which comes from public.orders_enriched.total_amount
```

## Config (from `.env`)

| Variable | Default |
|---|---|
| `POSTGRES_HOST` | `localhost` |
| `POSTGRES_PORT` | `5432` |
| `POSTGRES_DB` | `processed_db` |
| `POSTGRES_USER` | `pipeline` |
| `POSTGRES_PASSWORD` | `pipeline_pass` |

## Pipeline position

```
Layer 3/4 (Spark/Airflow writes orders_enriched to PostgreSQL)
      ↓
[Layer 13: SQLMesh transforms → dim_customer, dim_product, fact_orders, revenue_*]
      ↓
Layer 9 (FastAPI reads from pipeline.* schema)
Layer 11 (MLflow trains on pipeline.fact_orders)
```
