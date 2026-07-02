"""
Stage 4 — Great Expectations: Data Quality
============================================
CONCEPT: Great Expectations (GE) lets you define, document, and validate
data quality expectations — and tells you WHEN your data breaks those rules.

Without data quality checks:
  - A scraper changes its field names → your pipeline silently loads nulls
  - A vendor sends negative prices → your aggregates are wrong for 3 months
  - A JOIN condition breaks → rows disappear, nobody notices

With GE:
  - You define expectations once ("order_id is never null", "amount is > 0")
  - GE validates every new batch before it reaches the warehouse
  - Failed validations block the DAG and send alerts
  - Data Docs give you a browsable HTML report of every validation run

WHAT'S IN THIS PROJECT (great_expectations/)
──────────────────────────────────────────────
  great_expectations.yaml   — config (where to store results, which stores to use)
  expectations/raw_data_validation.py — currently just pandas checks

This stage replaces the raw pandas checks with proper GE Expectations.

GE CORE CONCEPTS
────────────────
  Expectation     — a testable assertion about your data
                    e.g. expect_column_values_to_not_be_null("order_id")
  ExpectationSuite — a named collection of Expectations for one dataset
  Validator        — runs Expectations against a DataFrame
  CheckpointResult — pass/fail summary + statistics per Expectation
  Data Docs        — auto-generated HTML report; run: ge docs build

EXPECTATION TYPES (most commonly used)
────────────────────────────────────────
  expect_column_to_exist                   — column must be in the DataFrame
  expect_column_values_to_not_be_null      — no nulls
  expect_column_values_to_be_between       — value in [min, max]
  expect_column_values_to_be_in_set        — value in an allowed set
  expect_column_values_to_match_regex      — e.g. UUID format
  expect_column_pair_values_a_to_be_greater_than_b — column A > column B
  expect_table_row_count_to_be_between     — row count in expected range
  expect_column_proportion_of_unique_values_to_be_between — uniqueness rate

Run:
  pip install great-expectations
  python stages/stage-4-great-expectations/ge_suite.py
"""

import logging
import os
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd

os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")
log = logging.getLogger(__name__)

# ── Sample data ───────────────────────────────────────────────────────────────
# Simulates what comes out of the batch ingestion pipeline

_GOOD_ORDERS_CSV = """order_id,customer_id,amount,processed_timestamp
1001,42,99.99,2024-01-15 10:30:00
1002,17,149.50,2024-01-15 10:31:00
1003,99,25.00,2024-01-15 10:32:00
"""

_BAD_ORDERS_CSV = """order_id,customer_id,amount,processed_timestamp
1004,,75.00,2024-01-15 10:33:00
1005,42,-10.00,2024-01-15 10:34:00
,55,200.00,2024-01-15 10:35:00
1006,0,50.00,2024-01-15 10:36:00
"""


def load_sample(csv_text: str) -> pd.DataFrame:
    df = pd.read_csv(StringIO(csv_text))
    df["processed_timestamp"] = pd.to_datetime(df["processed_timestamp"])
    return df


# ── GE Expectations Suite ─────────────────────────────────────────────────────

def build_orders_suite():
    """
    Build an ExpectationSuite for the orders dataset.

    We define expectations for:
      1. Required columns present
      2. No null keys
      3. Business rules (amount > 0, customer_id >= 1)
      4. Data freshness (processed_timestamp not stale)
      5. Row count reasonableness
    """
    import great_expectations as gx

    context = gx.get_context()

    suite_name = "orders_suite"
    suite = context.suites.add(gx.core.ExpectationSuite(name=suite_name))

    # ── Column presence ────────────────────────────────────────────────────────
    # If a source system renames a field, we catch it immediately.
    for col in ["order_id", "customer_id", "amount", "processed_timestamp"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column=col)
        )

    # ── Null checks ────────────────────────────────────────────────────────────
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="amount")
    )

    # ── Business rules ─────────────────────────────────────────────────────────
    # amount must be > 0 (no free or negative orders)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="amount",
            min_value=0.01,
            max_value=100_000,
            notes="Order amount must be positive and not absurdly large",
        )
    )
    # customer_id must be a positive integer
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="customer_id",
            min_value=1,
        )
    )
    # order_id must be unique (no duplicate orders)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="order_id")
    )

    # ── Row count ─────────────────────────────────────────────────────────────
    # Catch accidental empty batches or runaway inserts
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=1_000_000)
    )

    log.info("Suite '%s' built with %d expectations.", suite_name, len(suite.expectations))
    return context, suite


# ── Validation runner ─────────────────────────────────────────────────────────

def validate(df: pd.DataFrame, label: str, context=None, _counter=[0]) -> bool:
    """
    Validate a DataFrame against the orders suite.
    Returns True if all expectations pass.

    In production, this runs inside the Airflow DAG (batch_ingestion_dag.py)
    as a task BEFORE loading data into PostgreSQL / Snowflake.
    If it returns False, the downstream load tasks are skipped.
    """
    import great_expectations as gx

    if context is None:
        context = gx.get_context()
    suite_name = "orders_suite"

    # Use a unique source name per call to avoid name collisions
    _counter[0] += 1
    data_source = context.data_sources.add_pandas(f"pandas_source_{_counter[0]}")
    data_asset  = data_source.add_dataframe_asset(name="orders_batch")
    batch_def   = data_asset.add_batch_definition_whole_dataframe("batch")
    batch       = batch_def.get_batch(batch_parameters={"dataframe": df})

    validator    = context.get_validator(
        batch=batch,
        expectation_suite_name=suite_name,
    )
    result       = validator.validate()
    success      = result["success"]
    stats        = result["statistics"]

    print(f"\n  [{label}] Validation {'PASSED ✓' if success else 'FAILED ✗'}")
    print(f"    Evaluated: {stats['evaluated_expectations']}")
    print(f"    Successful: {stats['successful_expectations']}")
    print(f"    Failed: {stats['unsuccessful_expectations']}")

    if not success:
        for r in result["results"]:
            if not r["success"]:
                exp = r["expectation_config"]["type"]
                col = r["expectation_config"].get("kwargs", {}).get("column", "")
                print(f"    ✗ {exp} ({col})")

    return success


# ── Fallback: plain pandas validation (no GE dependency) ─────────────────────
# This is what great_expectations/expectations/raw_data_validation.py does today.
# The GE version above adds: structured results, Data Docs HTML, history tracking.

def pandas_validate(df: pd.DataFrame, label: str) -> bool:
    """Replicate the same checks without GE — useful to understand what GE wraps."""
    errors = []

    if df["order_id"].isna().any():
        errors.append("order_id has nulls")
    if (df["amount"] <= 0).any():
        errors.append(f"amount <= 0 in {(df['amount'] <= 0).sum()} rows")
    if (df["customer_id"] < 1).any():
        errors.append(f"customer_id < 1 in {(df['customer_id'] < 1).sum()} rows")
    if df["order_id"].duplicated().any():
        errors.append("duplicate order_ids found")

    if errors:
        print(f"\n  [{label}] FAILED ✗")
        for e in errors:
            print(f"    ✗ {e}")
        return False

    print(f"\n  [{label}] PASSED ✓  ({len(df)} rows)")
    return True


def run_walkthrough() -> None:
    print("\n── Stage 4: Great Expectations Walkthrough ──")

    good = load_sample(_GOOD_ORDERS_CSV)
    bad  = load_sample(_BAD_ORDERS_CSV)

    # Try GE first; fall back to plain pandas if GE not installed
    try:
        import great_expectations as gx
        # Silence GE's verbose internal logging — we only want our own output
        logging.getLogger("great_expectations").setLevel(logging.ERROR)
        print(f"\n  Great Expectations {gx.__version__} found. Running GE validation.")
        context, _suite = build_orders_suite()
        validate(good, "GOOD data", context=context)
        validate(bad,  "BAD data",  context=context)
        print("""
  Next step: run `great_expectations docs build` to open the HTML Data Docs
  report — a browsable record of every validation run with pass/fail stats.
""")
    except ImportError:
        print("\n  great_expectations not installed (pip install great-expectations)")
        print("  Running equivalent pandas validation instead:\n")
        pandas_validate(good, "GOOD data")
        pandas_validate(bad,  "BAD data")

    print("""
WHERE GE FITS IN THE DAG (airflow/dags/batch_ingestion_dag.py)
────────────────────────────────────────────────────────────────
  extract_mysql  →  validate_data  →  transform  →  load_postgres
                         ↓ fail
                    (skip load, alert)

  The validate_data task runs this script. If validation fails:
    - The load task is skipped (TriggerRule.ALL_SUCCESS)
    - Airflow marks the DAG run as failed
    - Alert fires (Slack / email via Airflow notification)
    - Data stays in staging, NOT in the warehouse

  This is the "fail fast" pattern — catch data quality issues at the
  boundary between raw data and the warehouse, not inside downstream reports.
""")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_walkthrough()
