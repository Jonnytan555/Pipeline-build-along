"""
Layer 12 — dbt (data build tool)
==================================
CONCEPT: dbt is the T in ELT. Instead of writing Python that runs SQL,
you write plain SQL SELECT statements. dbt compiles them into the right
DDL (CREATE TABLE / CREATE VIEW) and runs them in dependency order.

WHY dbt OVER RAW SQL (layer-08-warehouse/build_facts.py)?
──────────────────────────────────────────────────────────
  Raw SQL (Layer 8)          dbt (Layer 12)
  ─────────────────────      ─────────────────────────────
  Python wraps SQL           Pure SQL — no Python needed
  Manual dependency order    Auto-resolved from {{ ref() }}
  No built-in tests          Schema tests + singular tests
  No documentation           Auto-generated docs site
  No lineage graph           Built-in DAG visualisation
  Re-runs everything         Incremental models (only new rows)

KEY dbt CONCEPTS
────────────────
  {{ source('pipeline', 'orders_enriched') }}
    → references a raw source table (defined in sources.yml)
    → dbt knows the upstream dependency is NOT a dbt model

  {{ ref('stg_orders_enriched') }}
    → references another dbt model
    → dbt builds a DAG from all ref() calls and runs in correct order

  Materialisation:
    view  — CREATE OR REPLACE VIEW (no storage cost, always fresh)
    table — CREATE TABLE AS SELECT (fast reads, stale until next run)
    incremental — INSERT only new rows (cheap for huge tables)
    ephemeral — CTE, never materialised (just inlined)

  Tests:
    Generic (schema tests): not_null, unique, accepted_values, relationships
    Singular (SQL tests):   any query that returns rows = FAIL

MODEL DAG (this project)
────────────────────────
  source: orders_enriched
        │
        ▼
  stg_orders_enriched  (view)
        │
   ┌────┴────────────┐
   ▼                 ▼
dim_customer    dim_product
   │                 │
   └────────┬────────┘
            ▼
        fact_orders  (table)
        │          │
        ▼          ▼
  revenue_by_   revenue_by_
  customer      category

Run:
  cd layer-12-dbt/
  pip install dbt-postgres
  export DBT_PROFILES_DIR=.        # use profiles.yml in this directory
  dbt debug                        # test connection
  dbt run                          # build all models
  dbt test                         # run all tests
  dbt docs generate && dbt docs serve   # open HTML lineage + docs
"""

import subprocess
import sys
from pathlib import Path

DBT_DIR = Path(__file__).parent


def dbt(args: list[str]) -> tuple[int, str]:
    try:
        result = subprocess.run(
            ["dbt"] + args,
            cwd=DBT_DIR,
            capture_output=True,
            text=True,
            env={**__import__("os").environ, "DBT_PROFILES_DIR": str(DBT_DIR)},
        )
        return result.returncode, result.stdout + result.stderr
    except FileNotFoundError:
        return 1, "dbt not found — run: pip install dbt-postgres"


def run_walkthrough() -> None:
    print("\n── Layer 12: dbt Walkthrough ──")
    print(__doc__)

    print("── Checking dbt installation ──")
    code, out = dbt(["--version"])
    if code != 0:
        print(f"  {out.strip()}")
        print("  Install: pip install dbt-postgres dbt-sqlserver")
        return

    print(f"  {out.splitlines()[0]}")

    print("\n── dbt debug (connection test) ──")
    code, out = dbt(["debug"])
    if code != 0:
        print("  Connection failed — check POSTGRES_HOST / POSTGRES_USER / POSTGRES_PASSWORD in .env")
        print(f"  {out[-500:]}")
        return
    print("  Connection OK")

    print("\n── dbt run (build all models) ──")
    code, out = dbt(["run"])
    for line in out.splitlines():
        if any(k in line for k in ["OK", "ERROR", "WARN", "Completed", "of"]):
            print(f"  {line}")

    print("\n── dbt test (run all tests) ──")
    code, out = dbt(["test"])
    for line in out.splitlines():
        if any(k in line for k in ["PASS", "FAIL", "WARN", "Completed", "of"]):
            print(f"  {line}")

    print("""
── Next steps ────────────────────────────────────────────────
  dbt docs generate && dbt docs serve   # browse lineage DAG + column docs
  dbt run --select fact_orders+         # run fact_orders and everything downstream
  dbt run --select +revenue_by_category # run revenue_by_category and all upstreams
  dbt run --models tag:marts            # run only mart-tagged models

  In production (Airflow):
    BashOperator(task_id='dbt_run', bash_command='dbt run --profiles-dir /opt/dbt')
""")


if __name__ == "__main__":
    run_walkthrough()
