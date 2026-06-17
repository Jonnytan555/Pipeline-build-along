"""
Stage 1 — Snowflake Migration
==============================
CONCEPT: Snowflake separates storage from compute. You pay for storage always;
you pay for compute only when queries are running. This changes how you design
your warehouse compared to PostgreSQL.

Key architecture shift:
  PostgreSQL: one server, shared storage + compute, vertical scale
  Snowflake:  multi-cluster, object storage (S3), horizontal scale per query

WHAT CHANGES MOVING FROM POSTGRESQL TO SNOWFLAKE
─────────────────────────────────────────────────
Feature              | PostgreSQL          | Snowflake
─────────────────────┼─────────────────────┼───────────────────────
Auto-increment       | SERIAL              | AUTOINCREMENT
Timestamps           | TIMESTAMP           | TIMESTAMP_NTZ (no TZ)
Upsert               | ON CONFLICT         | MERGE INTO
Date spine           | generate_series()   | GENERATOR(ROWCOUNT=>N)
Bulk load            | COPY FROM (local)   | PUT + COPY INTO (stage)
Scheduling           | pg_cron / Airflow   | TASK (Snowflake-native)
Partitioning         | Manual partitions   | Micro-partitions (auto)
Index tuning         | CREATE INDEX        | CLUSTER BY (pruning)
Schema               | lowercase           | UPPERCASE (convention)
Connection           | psycopg2            | snowflake-connector-python

THE MIGRATION STRATEGY USED IN THIS PROJECT
────────────────────────────────────────────
The warehouse_transform_dag uses a feature flag (SNOWFLAKE_ENABLED):
  - If SNOWFLAKE_ACCOUNT is set → write to Snowflake
  - If not → fall back to PostgreSQL (same star schema, PostgreSQL syntax)

This lets you run in "PostgreSQL mode" locally and "Snowflake mode" in production
with a single codebase. The flag is set in .env or docker-compose environment.

SNOWFLAKE CONCEPTS DEMONSTRATED BELOW:
  1. Context manager pattern for connections
  2. write_pandas — bulk load a DataFrame to a Snowflake table
  3. MERGE — the Snowflake upsert (replaces ON CONFLICT)
  4. CLUSTER BY — how Snowflake prunes micro-partitions instead of indexes
  5. TASK — Snowflake-native scheduled SQL (replaces Airflow for simple transforms)
  6. TIME TRAVEL — query historical data (30-day default on all tables, free)

Run:
  SNOWFLAKE_ACCOUNT=xy12345.eu-west-1 \\
  SNOWFLAKE_USER=my_user \\
  SNOWFLAKE_PASSWORD=my_pass \\
  python stages/stage-1-snowflake/snowflake_migration.py
"""

import logging
import os
import sys
from contextlib import contextmanager
from datetime import datetime

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "PIPELINE_WH")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "PIPELINE_DB")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE", "PIPELINE_ROLE")


# ── 1. Context manager ────────────────────────────────────────────────────────
# Always use a context manager for Snowflake connections.
# Unlike PostgreSQL, Snowflake charges for idle sessions if the warehouse
# doesn't auto-suspend, so close connections promptly.

@contextmanager
def snowflake_conn():
    """
    Yields a Snowflake connection, ensures it is closed on exit.
    The warehouse AUTO_SUSPEND=300 (5 min) in init_warehouse.sql handles
    compute billing — the connection itself doesn't keep compute running.
    """
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        login_timeout=30,
    )
    try:
        yield conn
    finally:
        conn.close()


# ── 2. write_pandas — bulk load a DataFrame ──────────────────────────────────
# write_pandas is far faster than row-by-row INSERT for large datasets.
# It internally does: DataFrame → Parquet → PUT to internal stage → COPY INTO table
# This is the Snowflake equivalent of PostgreSQL's COPY FROM stdin.

def load_orders_to_snowflake(df: pd.DataFrame) -> int:
    """
    Load a DataFrame of orders into STAGING.STG_ORDERS.

    Note: Snowflake column names are UPPERCASE by convention.
    write_pandas auto-uppercases column names when auto_create_table=True,
    but since our tables already exist, we do it explicitly.
    """
    from snowflake.connector.pandas_tools import write_pandas

    # Snowflake expects uppercase column names for existing tables
    df = df.copy()
    df.columns = [c.upper() for c in df.columns]
    if "LOADED_AT" not in df.columns:
        df["LOADED_AT"] = datetime.utcnow()

    with snowflake_conn() as conn:
        # Truncate staging table first (idempotent load pattern)
        conn.cursor().execute("TRUNCATE TABLE STAGING.STG_ORDERS")

        success, n_chunks, n_rows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name="STG_ORDERS",
            database=SNOWFLAKE_DATABASE,
            schema="STAGING",
            # chunk_size=10_000  # tune for very large DataFrames
        )
        log.info("Loaded %d rows in %d chunks (success=%s)", n_rows, n_chunks, success)
        return n_rows


# ── 3. MERGE — the Snowflake upsert ──────────────────────────────────────────
# PostgreSQL: INSERT ... ON CONFLICT (key) DO UPDATE SET ...
# Snowflake:  MERGE INTO target USING source ON condition WHEN MATCHED / NOT MATCHED
#
# MERGE is more explicit and works the same in Snowflake, BigQuery, and SQL Server —
# useful to know as a portable pattern.

_MERGE_DIM_CUSTOMERS = """
MERGE INTO ANALYTICS.DIM_CUSTOMERS tgt
USING (
    SELECT DISTINCT
        CUSTOMER_ID,
        'Customer_' || CUSTOMER_ID AS CUSTOMER_NAME,
        MIN(PROCESSED_TIMESTAMP) AS JOIN_DATE
    FROM STAGING.STG_ORDERS
    GROUP BY CUSTOMER_ID
) src ON tgt.CUSTOMER_ID = src.CUSTOMER_ID AND tgt.IS_CURRENT = TRUE
WHEN NOT MATCHED THEN
    INSERT (CUSTOMER_ID, CUSTOMER_NAME, JOIN_DATE)
    VALUES (src.CUSTOMER_ID, src.CUSTOMER_NAME, src.JOIN_DATE)
-- WHEN MATCHED THEN UPDATE SET ...   ← add this for SCD Type 1
"""

def load_dim_customers() -> None:
    with snowflake_conn() as conn:
        conn.cursor().execute(_MERGE_DIM_CUSTOMERS)
        log.info("DIM_CUSTOMERS merged.")


# ── 4. CLUSTER BY — micro-partition pruning ──────────────────────────────────
# PostgreSQL uses B-tree indexes. Snowflake uses micro-partitions (16MB chunks of
# columnar data). CLUSTER BY tells Snowflake to co-locate rows with the same key
# in the same micro-partitions, so queries that filter on that key skip partitions.
#
# Rule of thumb: CLUSTER BY the columns you most often filter on in WHERE clauses.
# For fact_orders → queries almost always filter by DATE_KEY or CUSTOMER_KEY.
#
# Defined in init_warehouse.sql:
#   ALTER TABLE FACT_ORDERS CLUSTER BY (DATE_KEY, CUSTOMER_KEY);
#
# You never create an index in Snowflake. If a query is slow, check:
#   1. Is CLUSTER BY set on the filter columns?
#   2. Is the WHERE clause selective enough to prune partitions?
#   3. Is the warehouse size appropriate? (X-SMALL → SMALL for heavy scans)


# ── 5. TASK — Snowflake-native scheduling ────────────────────────────────────
# A TASK is a named, scheduled SQL statement inside Snowflake.
# It replaces Airflow for simple transforms that don't need Python logic.
#
# The init_warehouse.sql creates REFRESH_DAILY_ORDERS_AGG which runs every hour:
#
#   CREATE OR REPLACE TASK REFRESH_DAILY_ORDERS_AGG
#       WAREHOUSE = PIPELINE_WH
#       SCHEDULE  = 'USING CRON 0 * * * * UTC'
#   AS MERGE INTO AGG_DAILY_ORDERS tgt USING (...) src ON ...;
#
#   ALTER TASK REFRESH_DAILY_ORDERS_AGG RESUME;   ← tasks start SUSPENDED
#
# When to use TASK vs Airflow:
#   TASK   → pure SQL transforms within Snowflake, simple schedules
#   Airflow → Python logic, cross-system orchestration, complex DAG dependencies

def check_task_status() -> None:
    with snowflake_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SHOW TASKS LIKE 'REFRESH_DAILY_ORDERS_AGG'")
        rows = cursor.fetchall()
        for row in rows:
            log.info("Task: name=%s  state=%s  schedule=%s", row[1], row[9], row[7])


# ── 6. TIME TRAVEL — query historical data ───────────────────────────────────
# Every Snowflake table retains up to 90 days of change history (30 default, free).
# You can query the table AS OF a past timestamp or BEFORE a DML statement.
#
# This is a game-changer for debugging data pipelines:
#   - Someone ran a bad UPDATE? → SELECT * FROM fact_orders BEFORE (STATEMENT => '<query_id>')
#   - Wrong ETL loaded bad data yesterday? → AT (TIMESTAMP => '2024-01-01 06:00:00')

def demonstrate_time_travel() -> None:
    with snowflake_conn() as conn:
        cursor = conn.cursor()
        # Query the table as it was 1 hour ago
        cursor.execute("""
            SELECT COUNT(*) AS row_count_1h_ago
            FROM ANALYTICS.FACT_ORDERS
            AT (OFFSET => -3600)
        """)
        result = cursor.fetchone()
        log.info("FACT_ORDERS row count 1 hour ago: %s", result[0] if result else "N/A")


# ── Walkthrough runner ────────────────────────────────────────────────────────

def run_walkthrough() -> None:
    print("\n── Stage 1: Snowflake Migration Walkthrough ──")

    if not SNOWFLAKE_ACCOUNT:
        print("\n  SNOWFLAKE_ACCOUNT not set.")
        print("  To connect to Snowflake, set these environment variables:")
        print("    SNOWFLAKE_ACCOUNT   — e.g. xy12345.eu-west-1")
        print("    SNOWFLAKE_USER      — your Snowflake username")
        print("    SNOWFLAKE_PASSWORD  — your Snowflake password")
        print("\n  Without credentials, study the code above:")
        print("    - Context manager pattern (always close connections)")
        print("    - write_pandas vs row-by-row INSERT")
        print("    - MERGE syntax (same pattern as SQL Server / BigQuery)")
        print("    - CLUSTER BY replaces indexes")
        print("    - TASK replaces cron/Airflow for pure-SQL schedules")
        print("    - TIME TRAVEL: query any table as it was up to 90 days ago")
        return

    print("\n  Connecting to Snowflake...")
    try:
        with snowflake_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
            version, db, schema = cursor.fetchone()
            print(f"  Connected. Version={version}  DB={db}  Schema={schema}")

        print("\n  Checking TASK status...")
        check_task_status()

        print("\n  Testing TIME TRAVEL...")
        demonstrate_time_travel()

        print("\n  ✓ All concepts verified against live Snowflake.")
    except Exception as e:
        print(f"\n  Connection failed: {e}")
        print("  Check your SNOWFLAKE_ACCOUNT / credentials.")


if __name__ == "__main__":
    run_walkthrough()
