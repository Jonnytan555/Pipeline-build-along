# Stage 1 — Snowflake Migration

Migrates the warehouse from PostgreSQL to Snowflake. Demonstrates the key API and SQL differences between the two platforms and explains the architectural shift from shared-storage to multi-cluster cloud analytics.

## What it does

- Walks through the PostgreSQL → Snowflake translation for every major pattern
- Shows `write_pandas` bulk load (DataFrame → Parquet → PUT → COPY INTO)
- Demonstrates MERGE (Snowflake upsert), CLUSTER BY, TASK, and TIME TRAVEL
- Provides side-by-side feature comparison table

## Files

| File | Purpose |
|---|---|
| `snowflake_migration.py` | Walkthrough script — concepts, code examples, comparison table |

## Prerequisites

```bash
pip install snowflake-connector-python snowflake-sqlalchemy
```

Set these environment variables (or add to `.env`):

| Variable | Example |
|---|---|
| `SNOWFLAKE_ACCOUNT` | `xy12345.eu-west-1` |
| `SNOWFLAKE_USER` | `pipeline_user` |
| `SNOWFLAKE_PASSWORD` | *(your password)* |
| `SNOWFLAKE_WAREHOUSE` | `PIPELINE_WH` |
| `SNOWFLAKE_DATABASE` | `PIPELINE_DB` |
| `SNOWFLAKE_SCHEMA` | `ANALYTICS` |

## Run

```bash
python -m stages.stage-1-snowflake.snowflake_migration
```

The script gracefully prints concept walkthroughs even without a Snowflake account configured.

## Key concept translations

| PostgreSQL | Snowflake | Notes |
|---|---|---|
| `SERIAL` | `AUTOINCREMENT` | Auto-incrementing PK |
| `TIMESTAMP` | `TIMESTAMP_NTZ` | No timezone stored |
| `ON CONFLICT … DO UPDATE` | `MERGE INTO … WHEN MATCHED` | Upsert pattern |
| `CREATE INDEX` | `CLUSTER BY` | Micro-partition pruning instead of B-tree |
| `pg_cron` | `TASK` | Native scheduled SQL transforms |
| `generate_series()` | `GENERATOR(ROWCOUNT=>N)` | Sequence generation |

## Snowflake-only features covered

### `write_pandas` (bulk load)
```python
from snowflake.connector.pandas_tools import write_pandas
write_pandas(conn, df, "FACT_ORDERS")
# Internally: DataFrame → Parquet → PUT to internal stage → COPY INTO table
# Much faster than row-by-row INSERT for large batches
```

### TIME TRAVEL (audit / debug)
```sql
SELECT * FROM fact_orders AT (OFFSET => -3600);  -- 1 hour ago
SELECT * FROM fact_orders BEFORE (TIMESTAMP => '2024-01-15 09:00:00');
```

### TASK (native scheduling)
```sql
CREATE TASK refresh_aggregations
  WAREHOUSE = PIPELINE_WH
  SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
  INSERT OVERWRITE INTO agg_daily_orders SELECT ...;
```

## Architecture shift

| | PostgreSQL | Snowflake |
|---|---|---|
| Storage | Local disk | S3 (micro-partitions) |
| Compute | Shared with storage | Virtual warehouses (scale independently) |
| Scaling | Vertical (bigger server) | Horizontal (more clusters) |
| Idle cost | Running even when idle | Auto-suspend + auto-resume |

## Pipeline position

```
Stage 0.5 (baseline snapshot)
      ↓
[Stage 1: migrate to Snowflake]  ←  replaces PostgreSQL as the warehouse target
      ↓
Stages 2-5 (infrastructure, K8s, quality, governance)
```
