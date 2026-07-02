"""
Stage 0.5 — PostgreSQL Baseline Audit
======================================
CONCEPT: Before migrating anything to Snowflake, snapshot what you currently have.

Without a baseline you have no way to prove the migration succeeded.
A common mistake: migrate, declare victory, then discover 3 months later that
3% of rows were silently dropped because a type coercion failed.

This script connects to the existing PostgreSQL warehouse (processed_db) and
produces a report covering:
  1. Schema inventory     — what tables exist, their columns, constraints
  2. Row counts           — how many rows in each fact/dim table
  3. Data freshness       — when was each table last written to
  4. Referential health   — fact rows with no matching dimension key (orphans)
  5. Null rates           — key columns that should never be null

Run BEFORE Stage 1 and save the output:
  python stages/stage-0.5-postgres-baseline/baseline_audit.py > baseline_$(date +%Y%m%d).txt

Run AGAIN after Snowflake migration and diff:
  diff baseline_20240101.txt baseline_post_migration.txt

The Star Schema in PostgreSQL (processed_db):
  ┌─────────────┐     ┌──────────────┐
  │ dim_customers│     │  dim_devices │
  └──────┬──────┘     └──────┬───────┘
         │                   │
         ▼                   ▼
  ┌─────────────┐     ┌──────────────────────┐
  │ fact_orders  │     │ fact_sensor_readings  │
  └──────┬──────┘     └──────────────────────┘
         │
  ┌──────▼──────┐
  │   dim_date   │
  └─────────────┘

Key PostgreSQL features used here that DON'T exist in Snowflake:
  - generate_series() for date spine       → Snowflake uses GENERATOR(ROWCOUNT=>N)
  - SERIAL / BIGSERIAL for auto-increment  → Snowflake uses AUTOINCREMENT
  - ON CONFLICT (DO NOTHING / DO UPDATE)   → Snowflake uses MERGE
  - Partial indexes (WHERE clause)         → Snowflake doesn't support these
  - TIMESTAMP (with timezone)              → Snowflake prefers TIMESTAMP_NTZ
"""

import os
import sys
from datetime import datetime

import pandas as pd
import sqlalchemy as sa

# ── Connection ────────────────────────────────────────────────────────────────

PG_HOST   = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT   = os.getenv("POSTGRES_PORT", "5432")
PG_DB     = os.getenv("POSTGRES_DB",   "processed_db")
PG_USER   = os.getenv("POSTGRES_USER", "pipeline")
PG_PASS   = os.getenv("POSTGRES_PASSWORD", "pipeline123")


def get_engine() -> sa.Engine:
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return sa.create_engine(url)


# ── Audit queries ─────────────────────────────────────────────────────────────

_SCHEMA_INVENTORY = sa.text("""
    SELECT
        table_name,
        COUNT(*) AS column_count
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name NOT LIKE 'pg_%'
    GROUP BY table_name
    ORDER BY table_name
""")

_ROW_COUNTS = """
    SELECT '{table}' AS table_name, COUNT(*) AS row_count FROM {table}
"""

_FRESHNESS = sa.text("""
    SELECT
        schemaname,
        relname AS table_name,
        n_live_tup AS estimated_rows,
        last_analyze,
        last_autoanalyze,
        last_vacuum
    FROM pg_stat_user_tables
    WHERE schemaname = 'public'
    ORDER BY relname
""")

_ORPHAN_ORDERS = sa.text("""
    SELECT COUNT(*) AS orphan_orders
    FROM fact_orders fo
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_customer dc WHERE dc.customer_key = fo.customer_key
    )
""")

_NULL_RATES = sa.text("""
    SELECT
        'fact_orders' AS table_name,
        COUNT(*) AS total_rows,
        SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
        SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS null_customer_key,
        SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) AS null_total_amount
    FROM fact_orders
""")

# Tables to count — matching actual PostgreSQL schema
_TABLES = [
    "dim_customer", "dim_product",
    "fact_orders", "orders_enriched",
    "revenue_by_customer", "revenue_by_category",
]


# ── Report ────────────────────────────────────────────────────────────────────

def _header(title: str) -> None:
    width = 60
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width)


def run_audit(engine: sa.Engine) -> dict:
    report = {"generated_at": datetime.utcnow().isoformat(), "tables": {}}

    _header("1. SCHEMA INVENTORY")
    inventory = pd.read_sql(_SCHEMA_INVENTORY, engine)
    print(inventory.to_string(index=False))

    _header("2. ROW COUNTS")
    for table in _TABLES:
        try:
            df = pd.read_sql(sa.text(f"SELECT COUNT(*) AS n FROM {table}"), engine)
            n = int(df["n"].iloc[0])
            report["tables"][table] = n
            print(f"  {table:<35} {n:>10,} rows")
        except Exception as e:
            print(f"  {table:<35} ERROR: {e}")
            report["tables"][table] = -1

    _header("3. DATA FRESHNESS (pg_stat_user_tables)")
    try:
        freshness = pd.read_sql(_FRESHNESS, engine)
        print(freshness[["table_name", "estimated_rows", "last_analyze"]].to_string(index=False))
    except Exception as e:
        print(f"  Could not read pg_stat_user_tables: {e}")

    _header("4. REFERENTIAL INTEGRITY (orphan rows)")
    try:
        orphans = pd.read_sql(_ORPHAN_ORDERS, engine)
        n = int(orphans["orphan_orders"].iloc[0])
        status = "✓ CLEAN" if n == 0 else f"⚠  {n} ORPHANS FOUND"
        print(f"  fact_orders → dim_customers: {status}")
        report["orphan_orders"] = n
    except Exception as e:
        print(f"  Could not check referential integrity: {e}")

    _header("5. NULL RATES (key columns)")
    try:
        nulls = pd.read_sql(_NULL_RATES, engine)
        print(nulls.to_string(index=False))
        report["null_rates"] = nulls.to_dict("records")
    except Exception as e:
        print(f"  Could not check null rates: {e}")

    _header("BASELINE SUMMARY")
    total = sum(v for v in report["tables"].values() if v >= 0)
    print(f"  Total rows across {len(report['tables'])} tables: {total:,}")
    print(f"  Report generated: {report['generated_at']}")
    print(f"\n  Save this output now — you'll diff it against the post-migration report.")

    return report


if __name__ == "__main__":
    print(f"Connecting to PostgreSQL at {PG_HOST}:{PG_PORT}/{PG_DB}...")
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        print("Connected.\n")
        run_audit(engine)
    except Exception as e:
        print(f"\nConnection failed: {e}")
        print("Make sure docker compose is running: docker compose up -d")
        sys.exit(1)
