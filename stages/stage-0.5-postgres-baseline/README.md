# Stage 0.5 — PostgreSQL Baseline Audit

Snapshots the current state of the PostgreSQL warehouse before any migration or schema change. Run this before Stage 1 (Snowflake migration) to produce a diff-able baseline.

## What it does

1. **Schema inventory** — lists all tables and their column counts
2. **Row counts** — counts rows in each warehouse table
3. **Data freshness** — reads `pg_stat_user_tables` for last analyse / vacuum timestamps
4. **Referential integrity** — detects orphaned rows in `fact_orders` (missing customer/product dims)
5. **Null rates** — measures null percentage in key columns

## Files

| File | Purpose |
|---|---|
| `baseline_audit.py` | Runs all audit checks and prints a structured report |

## Run

```bash
# Capture baseline before migration
python -m stages.stage-0.5-postgres-baseline.baseline_audit > baseline_before.txt

# After migration, capture again and diff
python -m stages.stage-0.5-postgres-baseline.baseline_audit > baseline_after.txt
diff baseline_before.txt baseline_after.txt
```

**Note:** The script connects to the Docker-internal PostgreSQL. If running on Windows with a local Postgres on port 5432, run inside the Docker network:

```bash
docker run --rm --network pipeline-build-along_pipeline \
  -e POSTGRES_HOST=postgres \
  -v "$(pwd):/app" python:3.11-slim \
  python /app/stages/stage-0.5-postgres-baseline/baseline_audit.py
```

## Tables audited

- `dim_customer`
- `dim_product`
- `fact_orders`
- `orders_enriched`
- `revenue_by_customer`
- `revenue_by_category`

## Config (from `.env`)

| Variable | Default |
|---|---|
| `POSTGRES_HOST` | `localhost` |
| `POSTGRES_PORT` | `5432` |
| `POSTGRES_DB` | `processed_db` |
| `POSTGRES_USER` | `pipeline` |

## Sample output

```
── Row counts ─────────────────────────────────────
  dim_customer         :      0
  dim_product          :      0
  fact_orders          :      0
  orders_enriched      :      9
  revenue_by_customer  :      5
  revenue_by_category  :      3
  TOTAL                :     17

── Referential integrity ──────────────────────────
  Orphaned fact_orders :      0  CLEAN
```

## When to run

| Moment | Purpose |
|---|---|
| Before Stage 1 | Capture pre-migration baseline |
| After Stage 1 | Verify row counts match |
| After any schema change | Detect unintended data loss |
