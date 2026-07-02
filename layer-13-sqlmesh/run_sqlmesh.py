"""
Layer 13 — SQLMesh
===================
CONCEPT: SQLMesh is a next-generation transformation framework that improves
on dbt with state awareness — it knows exactly what changed between runs and
only re-executes what's necessary, without manual incremental logic.

HOW SQLMesh DIFFERS FROM dbt (Layer 12)
─────────────────────────────────────────
  dbt                              SQLMesh
  ─────────────────────────        ──────────────────────────────────
  Runs all models every time       State-aware: only changed models run
  No virtual environments          Dev environments are zero-copy snapshots
  Tests run separately             Audits are part of the model definition
  No column-level lineage          Built-in column-level lineage
  Python macros are complex        Python models are first-class
  CI: requires full prod copy      CI: virtual env — no data duplication

KEY SQLMesh CONCEPTS
────────────────────
  MODEL block (top of each .sql file):
    name        — fully-qualified: schema.model_name
    kind        — VIEW / FULL / INCREMENTAL_BY_TIME_RANGE / SCD_TYPE_2 / ...
    audits      — run after build; any returned rows = failure (like dbt tests)
    cron        — schedule (optional; Airflow or SQLMesh scheduler)

  State awareness:
    SQLMesh tracks a fingerprint of every model definition.
    If you change revenue_by_category.sql, it knows fact_orders hasn't
    changed and won't rebuild it — saving time and warehouse cost.

  Virtual environments:
    sqlmesh plan dev    ← applies changes to a "dev" schema
    sqlmesh plan prod   ← promotes to production with ZERO copy
    No temp table duplication. Dev and prod share unchanged snapshots.

  Incremental models (compare to dbt):
    dbt incremental:   you write the WHERE clause yourself
    SQLMesh INCREMENTAL_BY_TIME_RANGE:  framework handles partitioning,
      backfill, and gap-filling automatically.

MODEL DAG (this project — same as dbt layer)
────────────────────────────────────────────
  public.orders_enriched  (raw source)
        │
        ▼
  pipeline.stg_orders_enriched  [VIEW]
        │
   ┌────┴────────────────┐
   ▼                     ▼
pipeline.dim_customer  pipeline.dim_product  [FULL]
   │                         │
   └──────────┬──────────────┘
              ▼
      pipeline.fact_orders  [FULL]
      │                  │
      ▼                  ▼
pipeline.revenue_    pipeline.revenue_     [FULL]
by_customer          by_category

Run:
  cd layer-13-sqlmesh/
  pip install sqlmesh
  sqlmesh plan dev          # show what will change, apply to dev schema
  sqlmesh run               # execute scheduled models
  sqlmesh audit             # run all audits
  sqlmesh ui                # browser UI with lineage + column-level DAG
"""

import subprocess
import sys
from pathlib import Path

SQLMESH_DIR = Path(__file__).parent


def sqlmesh(args: list[str]) -> tuple[int, str]:
    try:
        result = subprocess.run(
            ["sqlmesh"] + args,
            cwd=SQLMESH_DIR,
            capture_output=True,
            text=True,
        )
        return result.returncode, result.stdout + result.stderr
    except FileNotFoundError:
        return 1, "sqlmesh not found — run: pip install sqlmesh"


def run_walkthrough() -> None:
    print("\n── Layer 13: SQLMesh Walkthrough ──")
    print(__doc__)

    print("── Checking SQLMesh installation ──")
    code, out = sqlmesh(["--version"])
    if code != 0:
        print(f"  {out.strip()}")
        print("  Install: pip install sqlmesh")
        return

    print(f"  SQLMesh {out.strip()}")

    print("\n── sqlmesh plan dev (diff + apply to dev environment) ──")
    print("  (This shows what SQLMesh would build — change detection in action)")
    code, out = sqlmesh(["plan", "dev", "--no-prompts", "--auto-apply"])
    for line in out.splitlines():
        if line.strip():
            print(f"  {line}")
    if code != 0:
        print("  Check POSTGRES_HOST / POSTGRES_USER / POSTGRES_PASSWORD in .env")
        return

    print("\n── sqlmesh audit (run all model audits) ──")
    code, out = sqlmesh(["audit", "--model", "pipeline.*"])
    for line in out.splitlines():
        if line.strip():
            print(f"  {line}")

    print("""
── SQLMesh vs dbt side-by-side ──────────────────────────────
  Same star schema. Key differences you'll see:

  1. MODEL block is in the SQL file (no separate YAML)
  2. Audits are declared inside MODEL, not in a schema.yml
  3. `sqlmesh plan` shows a diff — like terraform plan for data
  4. Column-level lineage: sqlmesh lineage pipeline.fact_orders total_amount
     → traces total_amount back to orders_enriched.total_amount through every model

── Next steps ───────────────────────────────────────────────
  sqlmesh ui                        # browser UI (lineage + column-level DAG)
  sqlmesh plan prod                 # promote dev → prod (zero data copy)
  sqlmesh run --model fact_orders   # run just one model + its upstreams
  sqlmesh lineage pipeline.fact_orders total_amount  # column lineage trace
""")


if __name__ == "__main__":
    run_walkthrough()
