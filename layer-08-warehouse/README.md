# Layer 8 — Warehouse (Star Schema)

Builds a dimensional model in SQL Server from the enriched orders data produced by Spark. Transforms normalised OLTP rows into a star schema optimised for BI queries and ML feature extraction.

## What it does

- Creates dimension and fact tables (DDL) if they don't already exist
- Loads dimensions idempotently using `INSERT … WHERE NOT EXISTS`
- Truncates and reloads `fact_orders` in a single transaction
- Exposes BI queries: revenue ranking, top customers, category/country heatmap, status breakdown

## Files

| File | Purpose |
|---|---|
| `build_facts.py` | DDL creation + dimension/fact loader |
| `queries.py` | BI analytical queries against the star schema |

## Run

```bash
# Load the star schema
make l8-run
# or: python -m layer-08-warehouse.build_facts

# Run BI queries
python -m layer-08-warehouse.queries
```

**Note:** Run with `python -m` (not `python layer-08-warehouse/build_facts.py`) to keep the project root in `sys.path`.

## Star schema

```
            dim_customer
           (customer_key PK)
                 │ FK
                 ▼
orders_enriched ──► fact_orders ◄── dim_product
                  (order_id PK)    (product_key PK)
```

### Tables

| Table | Key columns | Notes |
|---|---|---|
| `dim_customer` | `customer_key` (IDENTITY PK), `customer_name`, `country` | UNIQUE on `customer_name` |
| `dim_product` | `product_key` (IDENTITY PK), `product_name`, `category` | UNIQUE on `product_name` |
| `fact_orders` | `order_id` (PK), `customer_key` (FK), `product_key` (FK) | Full-refresh on each load |

### Load pattern

1. **Dimensions** — `INSERT INTO dim_customer SELECT … WHERE NOT EXISTS (…)` — idempotent, safe to re-run
2. **Fact** — `TRUNCATE fact_orders` then `INSERT … JOIN dim_customer JOIN dim_product` — inside a single transaction

## BI queries (`queries.py`)

| Query | Technique |
|---|---|
| Revenue by category | `RANK()` window function |
| Top customers by spend | `ORDER BY total DESC` |
| Revenue heatmap (category × country) | `PIVOT` / cross-tabulation |
| Order status breakdown | CTE + `GROUP BY` |

## Pipeline position

```
Layer 7 (quality gate)
      ↓
[Layer 8: star schema]  →  Layer 9 (FastAPI serves fact/dim data)
                        →  Layer 11 (MLflow trains on orders_enriched)
```
