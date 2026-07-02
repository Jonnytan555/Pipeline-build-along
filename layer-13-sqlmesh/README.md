# Layer 13 — SQLMesh

## Why add SQLMesh?

SQLMesh solves the same core problem as dbt (Layer 12) — declarative SQL transforms with testing and lineage — but it fixes several pain points that dbt users hit in production. If dbt is the industry standard, SQLMesh is what engineers build after they've run dbt in production for a year and know what it gets wrong.

**Problem 1 — dbt reruns everything, every time**
In dbt, `dbt run` rebuilds every model on every execution. For a warehouse with 200 models and a table that hasn't changed in a week, dbt still recreates it. At scale this wastes hours of compute and warehouse credits.

SQLMesh tracks a fingerprint (hash) of every model definition. On each run it compares fingerprints and only executes models whose SQL has actually changed. Rename a column in `dim_customer`? SQLMesh rebuilds `dim_customer` and everything downstream. Touch nothing? Nothing runs. This is called **state-aware execution** and it's the biggest practical difference.

**Problem 2 — dbt has no safe development environment**
In dbt, running `dbt run` in dev writes to a dev schema, but if you're testing a change that affects 5 downstream models, you have to rebuild all 5 to see the effect — duplicating all the data. Large tables can take hours.

SQLMesh's **virtual environments** are zero-copy. When you run `sqlmesh plan dev`, unchanged models are shared with production as snapshots — no data is duplicated. Only the models you actually changed are rebuilt in the dev schema. You can test a change to `fact_orders` in seconds without copying terabytes of data.

**Problem 3 — dbt has no change preview**
`dbt run` just runs. You find out what changed by inspecting the database afterwards. SQLMesh has `sqlmesh plan`, which works like `terraform plan` — it shows you exactly what will be created, modified, or dropped before you apply, and asks for confirmation.

```
$ sqlmesh plan dev

Summary of differences:
  Models:
    ~ pipeline.fact_orders  (column total_amount: DECIMAL(10,2) -> NUMERIC(12,4))
  Audits: 3 audits will run after apply
  Downstream models that will be rebuilt: revenue_by_category, revenue_by_customer

Apply? [y/N]
```

**Problem 4 — dbt tests are separate from models**
In dbt, model logic is in `.sql` files and tests are in separate `.yml` files. Over time these drift — someone updates the SQL but forgets to update the tests. SQLMesh puts audits directly in the `MODEL` block, so the test specification travels with the model definition.

**Problem 5 — dbt has no column-level lineage**
dbt tells you which *models* depend on which models. SQLMesh tracks which *columns* flow through the pipeline — you can ask "where does `total_amount` in `fact_orders` originally come from?" and get a column-level trace back to the source table. This is critical for GDPR (which field contains PII?) and for debugging incorrect aggregations.

## What SQLMesh adds over dbt

| dbt (Layer 12) | SQLMesh (Layer 13) |
|---|---|
| Reruns all models every time | State-aware: only changed models run |
| Dev schema requires full data copy | Virtual environments — zero data duplication |
| No preview before running | `sqlmesh plan` shows diff before applying |
| Tests in separate YAML files | Audits declared inside `MODEL` block |
| Model-level lineage | Column-level lineage built in |
| Python macros are complex | Python models are first-class |
| Incremental: you write the WHERE | INCREMENTAL_BY_TIME_RANGE: framework handles it |

## What SQLMesh adds to this pipeline

```
Layer 3/4 (Spark/Airflow writes raw orders_enriched to PostgreSQL)
      ↓
[Layer 13: SQLMesh — state-aware transforms, zero-copy dev env, column lineage]
      ↓
Layer 9  (FastAPI reads from pipeline.* schema)
Layer 11 (MLflow trains on pipeline.fact_orders)
```

In a real team setting, SQLMesh means:
- A junior engineer can safely test a model change without touching prod data
- CI/CD can validate a PR's impact before merge with `sqlmesh plan`
- The data lineage report for a GDPR audit takes minutes, not days

## Model DAG

```
public.orders_enriched  (raw source — written by Spark)
        │
        ▼
pipeline.stg_orders_enriched    VIEW   — cast types, filter nulls
        │
   ┌────┴────────────────┐
   ▼                     ▼
pipeline.dim_customer  pipeline.dim_product    FULL
   │                         │
   └──────────┬──────────────┘
              ▼
      pipeline.fact_orders    FULL  — with assert_positive_amounts audit
      │                   │
      ▼                   ▼
pipeline.revenue_      pipeline.revenue_       FULL
by_customer            by_category
```

## Files

```
layer-13-sqlmesh/
  config.py                              PostgreSQL connection (reads .env)
  models/
    staging/
      stg_orders_enriched.sql            VIEW with MODEL block + audits inline
    marts/
      dim_customer.sql                   FULL — unique customers + surrogate key
      dim_product.sql                    FULL — unique products + surrogate key
      fact_orders.sql                    FULL — FK joins + assert_positive_amounts audit
      revenue_by_category.sql            FULL — aggregated by category
      revenue_by_customer.sql            FULL — aggregated by customer
  audits/
    assert_positive_amounts.sql          custom audit: fails if amount <= 0
  run_sqlmesh.py                         walkthrough + command runner
```

## Run

```bash
pip install sqlmesh

cd layer-13-sqlmesh/

sqlmesh plan dev          # preview changes, apply to dev schema
sqlmesh run               # execute models on schedule
sqlmesh audit             # run all audits across all models
sqlmesh ui                # browser UI: lineage DAG + column-level trace
```

Promote dev to prod (zero data copy for unchanged models):

```bash
sqlmesh plan prod
```

Column-level lineage trace:

```bash
sqlmesh lineage pipeline.fact_orders total_amount
# → pipeline.stg_orders_enriched.total_amount
#   → public.orders_enriched.total_amount
```

## Config (from `.env`)

| Variable | Default |
|---|---|
| `POSTGRES_HOST` | `localhost` |
| `POSTGRES_PORT` | `5432` |
| `POSTGRES_DB` | `processed_db` |
| `POSTGRES_USER` | `pipeline` |
| `POSTGRES_PASSWORD` | `pipeline_pass` |

## When to choose SQLMesh over dbt

Choose **dbt** when:
- You're joining a team that already uses it
- You want the largest community + most plugins
- Your warehouse is small enough that full rebuilds are fast

Choose **SQLMesh** when:
- You have hundreds of models and full rebuilds are slow or expensive
- You need safe dev environments without duplicating prod data
- Column-level lineage matters (GDPR, compliance, debugging)
- You want `terraform plan`-style confidence before running transforms

Both are free and open source. Both work with PostgreSQL, Snowflake, BigQuery, Databricks, and DuckDB.
