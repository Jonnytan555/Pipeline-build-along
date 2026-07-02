# Layer 12 — dbt (data build tool)

## Why add dbt?

Without dbt, SQL transforms live inside Python scripts (see [layer-08-warehouse/build_facts.py](../layer-08-warehouse/build_facts.py)). That works, but it creates a set of problems that every data team hits as the pipeline grows:

**Problem 1 — You can't see what depends on what**
`build_facts.py` runs its SQL statements in the order you wrote them. If someone adds a new model that depends on `fact_orders`, they have to read the source code to figure that out. dbt builds a dependency graph automatically from `{{ ref() }}` calls — run `dbt docs serve` and you get a visual DAG showing every model and its upstream/downstream dependencies.

**Problem 2 — You don't know if the data is actually correct**
Layer 8 loads data with no automated checks. dbt adds schema tests directly to the model definitions — `not_null`, `unique`, `accepted_values`, and referential integrity between tables. Every `dbt test` run catches broken data before it reaches dashboards or the API.

**Problem 3 — New team members can't understand the warehouse**
"What does `revenue_by_category` contain? Where did it come from? Who owns it?" With raw SQL, the answer is "read the code and ask someone." dbt auto-generates an HTML documentation site from the model definitions and descriptions — `dbt docs serve` gives you a searchable data dictionary linked to the lineage graph.

**Problem 4 — SQL is buried inside Python**
When an analyst wants to change a revenue calculation, they have to understand Python, find the right `.py` file, and hope they don't break the runner logic. With dbt, every transform is a plain `.sql` file. Analysts can edit models directly without touching any Python.

**Problem 5 — Full rebuilds are wasteful**
Layer 8 truncates and reloads every table on every run. For 9 rows this is fine. For 50 million orders it takes hours. dbt's `incremental` materialisation inserts only new rows — the framework generates the `WHERE id > last_loaded_id` logic for you.

## What dbt adds to this pipeline

| Before (Layer 8) | After (Layer 12 — dbt) |
|---|---|
| Python wraps SQL | Pure SQL `.sql` files |
| Manual dependency order | Auto-resolved via `ref()` |
| No tests | `not_null`, `unique`, FK checks |
| No documentation | Auto-generated HTML docs + data dictionary |
| No lineage graph | Visual DAG in `dbt docs serve` |
| Truncate + reload everything | `materialized: incremental` for large tables |
| Analysts need Python to make changes | Analysts edit `.sql` directly |

## How it fits in the pipeline

```
Layer 3/4 (Spark/Airflow writes raw orders_enriched to PostgreSQL)
      ↓
[Layer 12: dbt reads orders_enriched, builds the star schema]
      ↓
Layer 9  (FastAPI reads from dbt mart tables)
Layer 11 (MLflow trains on dbt.fact_orders)
Layer 7  (quality checks run on dbt outputs)
```

dbt sits between the raw load and the downstream consumers — it's the single place where all business logic lives.

## Model DAG

```
source: orders_enriched  (raw — written by Spark)
        │
        ▼
stg_orders_enriched      view  — cast types, filter nulls, lowercase status
        │
   ┌────┴────────────┐
   ▼                 ▼
dim_customer    dim_product    table  — one row per unique customer / product
   │                 │
   └────────┬────────┘
            ▼
        fact_orders            table  — one row per order, FK joins to dims
        │           │
        ▼           ▼
revenue_by_      revenue_by_   table  — pre-aggregated, read by FastAPI + Redis
customer         category
```

## Files

```
layer-12-dbt/
  dbt_project.yml                        project config + materialisation strategy
  profiles.yml                           connection (reads from .env via env_var())
  models/
    sources.yml                          declares orders_enriched as a raw source
    staging/
      stg_orders_enriched.sql            clean + cast (VIEW)
      stg_orders_enriched.yml            tests: not_null, unique, accepted_values
    marts/
      dim_customer.sql                   unique customers with surrogate key (TABLE)
      dim_product.sql                    unique products with surrogate key (TABLE)
      fact_orders.sql                    order facts with FK joins (TABLE)
      revenue_by_category.sql            aggregated by category (TABLE)
      revenue_by_customer.sql            aggregated by customer (TABLE)
      marts.yml                          FK relationship tests
  tests/
    assert_positive_amounts.sql          singular test: fails if amount <= 0
  run_dbt.py                             walkthrough + command runner
```

## Run

```bash
pip install dbt-postgres

cd layer-12-dbt/
export DBT_PROFILES_DIR=.    # use the profiles.yml in this directory

dbt debug                    # test the database connection
dbt run                      # build all models in dependency order
dbt test                     # run all schema + singular tests
dbt docs generate            # build the HTML documentation site
dbt docs serve               # open browser: lineage DAG + data dictionary
```

## Config (from `.env`)

| Variable | Default |
|---|---|
| `POSTGRES_HOST` | `localhost` |
| `POSTGRES_PORT` | `5432` |
| `POSTGRES_DB` | `processed_db` |
| `POSTGRES_USER` | `pipeline` |
| `POSTGRES_PASSWORD` | `pipeline_pass` |

## Industry context

dbt is the most widely adopted transformation tool in data engineering — it appears in virtually every modern data stack (Databricks, Snowflake, BigQuery, Redshift). If you work at a company with a data warehouse, there is a high probability dbt is already there or being evaluated. Learning it here means you can read and contribute to production dbt projects immediately.

dbt Core is free and open source. dbt Cloud adds a scheduler, CI/CD, and a hosted docs site.
