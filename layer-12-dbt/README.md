# Layer 12 — dbt (data build tool)

Replaces the hand-written SQL transforms in [layer-08-warehouse/build_facts.py](../layer-08-warehouse/build_facts.py) with declarative dbt models. Each model is a plain SQL `SELECT`; dbt handles the `CREATE TABLE / VIEW`, dependency ordering, and testing.

## What it does

- Defines 7 models covering the full star schema (staging → dimensions → facts → aggregates)
- Adds schema tests (not_null, unique, accepted_values, referential integrity)
- Produces auto-generated HTML documentation with a lineage DAG

## Files

```
layer-12-dbt/
  dbt_project.yml              project config (materialisation strategy)
  profiles.yml                 connection (reads from env vars)
  models/
    sources.yml                declares orders_enriched as a raw source
    staging/
      stg_orders_enriched.sql  clean + cast raw orders
      stg_orders_enriched.yml  not_null / unique / accepted_values tests
    marts/
      dim_customer.sql         unique customers with surrogate key
      dim_product.sql          unique products with surrogate key
      fact_orders.sql          order facts with FK joins to dims
      revenue_by_category.sql  pre-aggregated revenue per category
      revenue_by_customer.sql  pre-aggregated revenue per customer
      marts.yml                FK relationship tests
  tests/
    assert_positive_amounts.sql  singular test — fails if amount <= 0
  run_dbt.py                   walkthrough script
```

## Model DAG

```
source: orders_enriched
        │
        ▼
stg_orders_enriched  [view]
        │
   ┌────┴────────────┐
   ▼                 ▼
dim_customer    dim_product    [table]
   │                 │
   └────────┬────────┘
            ▼
        fact_orders             [table]
        │           │
        ▼           ▼
revenue_by_      revenue_by_   [table]
customer         category
```

## Run

```bash
cd layer-12-dbt/
pip install dbt-postgres

# Use the profiles.yml in this directory
export DBT_PROFILES_DIR=.

# Test connection
dbt debug

# Build all models
dbt run

# Run all tests
dbt test

# Browse lineage + column docs
dbt docs generate && dbt docs serve
```

## dbt vs raw SQL (Layer 8)

| | Layer 8 (`build_facts.py`) | Layer 12 (dbt) |
|---|---|---|
| Language | Python wrapping SQL | Pure SQL |
| Dependency order | Manual | Auto-resolved via `ref()` |
| Tests | None | Generic + singular |
| Documentation | None | Auto-generated HTML |
| Lineage | None | Built-in DAG |
| Incremental loads | Re-runs everything | `materialized: incremental` |

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
[Layer 12: dbt transforms → dim_customer, dim_product, fact_orders, revenue_*]
      ↓
Layer 9 (FastAPI reads from mart tables)
Layer 11 (MLflow trains on fact_orders)
```
