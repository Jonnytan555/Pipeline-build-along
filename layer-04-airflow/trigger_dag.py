"""
LAYER 4 — Trigger the batch pipeline DAG via Airflow REST API
=============================================================

What you will learn:
  - Airflow exposes a REST API for programmatic DAG management
  - Triggering a DAG run with custom configuration (conf dict)
  - How to check the run status after triggering
  - This is the same mechanism CI/CD systems use to kick off
    pipelines after a code deploy or data arrival event

Run:
    make l4-trigger
    # equivalent to: python layer-04-airflow/trigger_dag.py

Prerequisites:
    make l4-up   ← starts Airflow (webserver + scheduler)
    Wait ~30 s for the webserver to be ready, then run this.

Airflow UI:  http://localhost:8082
"""

import logging
import os
import time

import requests

try:
    from utils.logger import setup_log
    setup_log(app="layer-04-trigger", use_stream=True)
except ImportError:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

AIRFLOW_URL  = os.getenv("AIRFLOW_URL",           "http://localhost:8082")
AIRFLOW_USER = os.getenv("AIRFLOW_ADMIN_USER",     "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_ADMIN_PASSWORD", "airflow_pass")
DAG_ID       = "batch_ingestion"

AUTH = (AIRFLOW_USER, AIRFLOW_PASS)


def trigger_dag(conf: dict | None = None) -> str:
    """
    POST to /api/v1/dags/{dag_id}/dagRuns to create a manual run.
    Returns the dag_run_id so we can poll its status.
    """
    url  = f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns"
    body = {"conf": conf or {}}

    resp = requests.post(url, json=body, auth=AUTH, timeout=30)
    resp.raise_for_status()

    run_id = resp.json()["dag_run_id"]
    log.info("DAG triggered — run_id: %s", run_id)
    return run_id


def poll_run(run_id: str, timeout_s: int = 120) -> str:
    """
    Poll the run state until it reaches a terminal state or
    the timeout expires.

    States:
      queued    → waiting for the scheduler to pick it up
      running   → at least one task is executing
      success   → all tasks succeeded
      failed    → at least one task failed
    """
    url = f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns/{run_id}"

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        resp  = requests.get(url, auth=AUTH, timeout=30)
        state = resp.json().get("state", "unknown")
        log.info("State: %s", state)

        if state in ("success", "failed"):
            return state
        time.sleep(5)

    return "timeout"


if __name__ == "__main__":
    log.info("=" * 50)
    log.info("Triggering DAG: %s", DAG_ID)
    log.info("=" * 50)

    run_id = trigger_dag()
    final  = poll_run(run_id)

    log.info("Final state: %s", final)
    if final != "success":
        log.error("DAG did not succeed. Check the UI: %s", AIRFLOW_URL)
        raise SystemExit(1)

    log.info("Done. Check the warehouse: python layer-08-warehouse/queries.py")
