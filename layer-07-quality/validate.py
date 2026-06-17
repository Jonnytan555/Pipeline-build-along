"""
LAYER 7 — Data Quality Validation
===================================

What you will learn:
  - Why data quality checks belong inside the pipeline, not
    outside it: silent bad data is worse than a loud failure
  - The four pillars of data quality:
      1. Completeness  — no unexpected NULLs
      2. Validity      — values within allowed ranges / enums
      3. Consistency   — referential integrity across tables
      4. Freshness     — data was updated recently
  - The "stop-the-line" pattern: raise an exception on failure
    so Airflow marks the task as failed and fires an alert,
    rather than silently writing corrupt data downstream
  - How to collect all failures before raising (report-all mode)

Run:
    make l7-run
    # equivalent to: python layer-07-quality/validate.py

Prerequisites:
    make l3-run or make l4-trigger  ← populate the warehouse first
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from utils.db import Database
from utils.logger import setup_log

setup_log(app="layer-07-quality", use_stream=True)
log = logging.getLogger(__name__)

db = Database(
    name="source_db",
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", "sa"),
    password=os.getenv("SA_PASSWORD", "december1"),
)


# ── Result tracking ───────────────────────────────────────────
@dataclass
class CheckResult:
    name:    str
    passed:  bool
    message: str
    actual:  Any = None


@dataclass
class ValidationReport:
    results: list[CheckResult] = field(default_factory=list)

    def add(self, result: CheckResult) -> None:
        status = "PASS" if result.passed else "FAIL"
        log.info("  [%s]  %s: %s", status, result.name, result.message)
        self.results.append(result)

    @property
    def failed(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed]

    def assert_all_passed(self) -> None:
        if self.failed:
            names = [r.name for r in self.failed]
            raise AssertionError(
                f"{len(self.failed)} data quality check(s) failed: {names}"
            )


# ── Individual checks ─────────────────────────────────────────
def _scalar(sql: str) -> Any:
    """Run a COUNT-style query and return the single scalar result."""
    return db.query(sql, cache=False).iloc[0, 0]


def check_row_count(report: ValidationReport, table: str, min_rows: int) -> None:
    actual = _scalar(f"SELECT COUNT(*) FROM {table}")
    report.add(CheckResult(
        name    = f"{table}.row_count",
        passed  = actual >= min_rows,
        message = f"found {actual} rows (min={min_rows})",
        actual  = actual,
    ))


def check_no_nulls(report: ValidationReport, table: str, column: str) -> None:
    nulls = _scalar(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
    report.add(CheckResult(
        name    = f"{table}.{column}.no_nulls",
        passed  = nulls == 0,
        message = f"{nulls} NULL values (expected 0)",
        actual  = nulls,
    ))


def check_non_negative(report: ValidationReport, table: str, column: str) -> None:
    bad = _scalar(f"SELECT COUNT(*) FROM {table} WHERE {column} < 0")
    report.add(CheckResult(
        name    = f"{table}.{column}.non_negative",
        passed  = bad == 0,
        message = f"{bad} negative values (expected 0)",
        actual  = bad,
    ))


def check_allowed_values(
    report: ValidationReport, table: str, column: str, allowed: set
) -> None:
    # T-SQL IN list — values are controlled strings, not user input
    values = ", ".join(f"'{v}'" for v in allowed)
    bad = _scalar(
        f"SELECT COUNT(*) FROM {table} WHERE {column} NOT IN ({values})"
    )
    report.add(CheckResult(
        name    = f"{table}.{column}.allowed_values",
        passed  = bad == 0,
        message = f"{bad} rows with unexpected values (allowed: {sorted(allowed)})",
        actual  = bad,
    ))


def check_referential_integrity(
    report: ValidationReport,
    child_table: str, child_col: str,
    parent_table: str, parent_col: str,
) -> None:
    orphans = _scalar(f"""
        SELECT COUNT(*) FROM {child_table} c
        WHERE NOT EXISTS (
            SELECT 1 FROM {parent_table} p WHERE p.{parent_col} = c.{child_col}
        )
    """)
    report.add(CheckResult(
        name    = f"{child_table}.{child_col}.ref_integrity",
        passed  = orphans == 0,
        message = f"{orphans} orphaned rows (no matching {parent_table}.{parent_col})",
        actual  = orphans,
    ))


def check_freshness(
    report: ValidationReport,
    table: str, date_column: str, max_age_days: int = 2,
) -> None:
    """Warn if the most recent row is older than max_age_days."""
    # T-SQL: CAST(... AS DATE) instead of PostgreSQL ::date
    latest = _scalar(f"SELECT MAX(CAST({date_column} AS DATE)) FROM {table}")
    if latest is None:
        report.add(CheckResult(
            name    = f"{table}.{date_column}.freshness",
            passed  = False,
            message = "table is empty — no dates to check",
        ))
        return

    age = (date.today() - latest).days
    report.add(CheckResult(
        name    = f"{table}.{date_column}.freshness",
        passed  = age <= max_age_days,
        message = f"latest date is {latest} ({age} days old, max={max_age_days})",
        actual  = latest,
    ))


# ── Run all checks ────────────────────────────────────────────
def run_validation() -> ValidationReport:
    report = ValidationReport()

    log.info("── orders_enriched ──────────────────────────────────────")
    check_row_count(report, "orders_enriched", min_rows=1)
    check_no_nulls( report, "orders_enriched", "order_id")
    check_no_nulls( report, "orders_enriched", "customer_name")
    check_non_negative(report, "orders_enriched", "total_amount")
    check_non_negative(report, "orders_enriched", "quantity")
    check_allowed_values(
        report, "orders_enriched", "status",
        {"pending", "shipped", "delivered"},
    )
    check_freshness(report, "orders_enriched", "order_date", max_age_days=365)

    log.info("── revenue_by_customer ──────────────────────────────────")
    check_row_count(report, "revenue_by_customer", min_rows=1)
    check_no_nulls( report, "revenue_by_customer", "customer_name")
    check_non_negative(report, "revenue_by_customer", "total_revenue")
    check_freshness(report, "revenue_by_customer", "as_of_date", max_age_days=7)

    log.info("── revenue_by_category ──────────────────────────────────")
    check_row_count(report, "revenue_by_category", min_rows=1)
    check_no_nulls( report, "revenue_by_category", "category")
    check_non_negative(report, "revenue_by_category", "total_revenue")

    return report


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 60)
    log.info("LAYER 7 — Data Quality Validation")
    log.info("=" * 60)

    report = run_validation()

    log.info("=" * 60)
    passed = sum(1 for r in report.results if r.passed)
    total  = len(report.results)
    log.info("Results: %s/%s checks passed", passed, total)

    if report.failed:
        log.error("FAILED CHECKS:")
        for r in report.failed:
            log.error("  x  %s: %s", r.name, r.message)
        report.assert_all_passed()
    else:
        log.info("All checks passed.")
        log.info("Next -> Layer 8: build warehouse fact/dim tables and BI queries.")
