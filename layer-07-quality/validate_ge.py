"""
LAYER 7b — Great Expectations: Industry-Standard Data Validation
=================================================================

What you will learn:
  - Great Expectations (GE): the industry standard for pipeline
    data quality — used at Airbnb, Spotify, and most data teams
  - Expectation Suites: a named collection of rules about your data
  - Validators: attach a suite to a DataFrame and run checks
  - Data Docs: auto-generated HTML quality reports
  - How GE differs from the custom validate.py:
      validate.py  → hand-rolled COUNT(*) queries, custom classes
      GE           → declarative expectations, shareable JSON suites,
                     HTML reports, Airflow/dbt integrations built-in

GE concepts:
    DataContext    → the root object — manages suites, checkpoints, docs
    ExpectationSuite → a named set of rules (stored as JSON)
    Validator      → binds a suite to a batch of data, runs the checks
    Checkpoint     → runs one or more (suite, data) pairs, saves results

Run:
    python layer-07-quality/validate_ge.py

Prerequisites:
    pip install great-expectations==0.18.8
    make l3-run or make l4-trigger  ← warehouse tables must exist
"""

import logging
import os

import great_expectations as gx
import pandas as pd

from utils.db import Database
from utils.logger import setup_log

setup_log(app="layer-07-ge", use_stream=True)
log = logging.getLogger(__name__)

db = Database(
    name="source_db",
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", "sa"),
    password=os.getenv("SA_PASSWORD", "december1"),
)


# ── Load data from warehouse ──────────────────────────────────
def load_table(table: str) -> pd.DataFrame:
    return db.query(f"SELECT * FROM {table}", cache=False)


# ── Build expectation suites ──────────────────────────────────
def add_orders_expectations(validator) -> None:
    """
    Declare what 'good' orders_enriched data looks like.

    These expectations are the GE equivalent of the checks in
    validate.py — but stored as a reusable JSON suite, shareable
    across teams, and renderable as HTML data docs.
    """
    # Completeness
    validator.expect_table_row_count_to_be_between(min_value=1)
    validator.expect_column_to_exist("order_id")
    validator.expect_column_values_to_not_be_null("order_id")
    validator.expect_column_values_to_not_be_null("customer_name")
    validator.expect_column_values_to_not_be_null("total_amount")

    # Validity
    validator.expect_column_values_to_be_between(
        "total_amount", min_value=0
    )
    validator.expect_column_values_to_be_between(
        "quantity", min_value=1
    )
    validator.expect_column_values_to_be_in_set(
        "status", {"pending", "shipped", "delivered"}
    )

    # Uniqueness
    validator.expect_column_values_to_be_unique("order_id")


def add_revenue_expectations(validator) -> None:
    """Expectations for revenue_by_customer."""
    validator.expect_table_row_count_to_be_between(min_value=1)
    validator.expect_column_values_to_not_be_null("customer_name")
    validator.expect_column_values_to_not_be_null("total_revenue")
    validator.expect_column_values_to_be_between(
        "total_revenue", min_value=0
    )
    validator.expect_column_values_to_be_between(
        "order_count", min_value=1
    )


# ── Run validations ───────────────────────────────────────────
def run_ge_validation() -> bool:
    """
    Run GE validation using an ephemeral (in-memory) DataContext.

    An ephemeral context requires no config files — perfect for
    learning and for running inside a pipeline task.  In production
    you would use a FileSystem or Cloud context so suites and
    validation results persist between runs.
    """
    log.info("Initialising Great Expectations context ...")
    context = gx.get_context()

    # ── Datasource: pandas DataFrames loaded from SQL Server ──
    datasource = context.sources.add_pandas("warehouse")

    all_passed = True

    # ── orders_enriched ───────────────────────────────────────
    log.info("Loading orders_enriched ...")
    orders_df = load_table("orders_enriched")
    log.info("  %d rows loaded", len(orders_df))

    orders_asset = datasource.add_dataframe_asset(
        name="orders_enriched",
        dataframe=orders_df,
    )
    batch_request = orders_asset.build_batch_request()

    suite_name = "orders_enriched_suite"
    suite = context.add_expectation_suite(suite_name)
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    add_orders_expectations(validator)
    validator.save_expectation_suite(discard_failed_expectations=False)

    results = validator.validate()
    _log_results("orders_enriched", results)
    if not results.success:
        all_passed = False

    # ── revenue_by_customer ───────────────────────────────────
    log.info("Loading revenue_by_customer ...")
    rev_df = load_table("revenue_by_customer")
    log.info("  %d rows loaded", len(rev_df))

    rev_asset = datasource.add_dataframe_asset(
        name="revenue_by_customer",
        dataframe=rev_df,
    )
    batch_request = rev_asset.build_batch_request()

    suite_name = "revenue_by_customer_suite"
    suite = context.add_expectation_suite(suite_name)
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    add_revenue_expectations(validator)
    validator.save_expectation_suite(discard_failed_expectations=False)

    results = validator.validate()
    _log_results("revenue_by_customer", results)
    if not results.success:
        all_passed = False

    # ── Build data docs ───────────────────────────────────────
    log.info("Building Data Docs ...")
    context.build_data_docs()
    docs_path = context.get_docs_sites_urls()[0]["site_url"]
    log.info("Data Docs available at: %s", docs_path)

    return all_passed


def _log_results(table: str, results) -> None:
    """Log a summary of the validation results."""
    log.info("── %s ──", table)
    for result in results.results:
        status = "PASS" if result.success else "FAIL"
        expectation = result.expectation_config.expectation_type
        column = result.expectation_config.kwargs.get("column", "table")
        log.info("  [%s]  %s  →  %s", status, column, expectation)


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 60)
    log.info("LAYER 7b — Great Expectations Validation")
    log.info("=" * 60)

    passed = run_ge_validation()

    log.info("=" * 60)
    if passed:
        log.info("All GE validations passed.")
        log.info("Open Data Docs to see the HTML quality report.")
    else:
        log.error("One or more GE validations FAILED.")
        raise SystemExit(1)
    log.info("=" * 60)
