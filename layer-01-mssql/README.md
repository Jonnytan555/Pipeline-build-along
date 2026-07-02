# Layer 1 — MSSQL Source System

Queries the OLTP SQL Server source database and audits its contents. This is the entry point of the pipeline — raw transactional data lives here before any extraction or transformation.

## What it does

- Connects to SQL Server (`source_db`) using SQLAlchemy + pyodbc
- Runs row-count audits across all source tables
- Demonstrates JOIN-based queries (orders → customers → products)
- Produces revenue aggregations and operational order-status reports

## Files

| File | Purpose |
|---|---|
| `query.py` | Main script — audit queries and sample reports |

## Run

```bash
python layer-01-mssql/query.py
```

Requires the SQL Server container to be running:

```bash
docker compose up -d mssql
```

## Source schema

| Table | Description |
|---|---|
| `customers` | Customer master data |
| `products` | Product catalogue |
| `orders` | Raw OLTP order transactions |

## Config (from `.env`)

| Variable | Default |
|---|---|
| `DB_HOST` | `localhost` |
| `DB_PORT` | `1433` |
| `DB_NAME` | `source_db` |
| `DB_USER` | `sa` |

## Pipeline position

```
[Layer 1: MSSQL source]  →  Layer 2 (extract to MinIO)
```

Data flows from here into Layer 2, which extracts full tables and lands them in the raw data lake.
