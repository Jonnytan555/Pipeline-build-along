# Layer 4 — Airflow DAG Orchestration

Schedules the full ETL pipeline using Apache Airflow 2.x with the TaskFlow API. Each pipeline step is a decorated `@task` function; XCom passes state between tasks automatically.

## What it does

- Runs the end-to-end batch pipeline on a daily schedule
- Extracts from SQL Server → MinIO, validates, transforms, loads to PostgreSQL
- Provides retry logic, alerting, and a web UI for monitoring

## Files

| File | Purpose |
|---|---|
| `dags/batch_ingestion_dag.py` | Main DAG definition (TaskFlow API) |
| `trigger_dag.py` | CLI utility to trigger a DAG run manually |
| `Dockerfile` | Custom Airflow image with pipeline dependencies |
| `requirements-airflow.txt` | Extra Python packages for the Airflow environment |

## DAG structure

```
extract_to_lake          SQL Server → MinIO CSV
      ↓  (XCom: file keys)
validate_raw_data        Assert non-empty, basic schema checks
      ↓  (XCom passed through)
transform_to_warehouse   pandas join + aggregate → PostgreSQL
      ↓  (XCom: row counts)
report_summary           Log stats to Airflow task log
```

## Run

```bash
# Trigger via CLI
python layer-04-airflow/trigger_dag.py

# Or use the Makefile shortcut
make l4-trigger
```

Airflow web UI: [http://localhost:8082](http://localhost:8082) — `admin` / `airflow_pass`

Start the stack:

```bash
docker compose up -d airflow-webserver airflow-scheduler
```

## DAG config

| Setting | Value |
|---|---|
| DAG ID | `batch_ingestion` |
| Schedule | `@daily` |
| Start date | 2024-01-01 |
| Catchup | False |
| Retries | 2 |
| Retry delay | 5 minutes |

## Pipeline position

```
Layers 1-3 (MSSQL → MinIO → Spark)
      ↓
[Layer 4: Airflow schedules & orchestrates everything]
      ↓
Layers 5+ (Kafka, multi-store, quality, warehouse…)
```

Airflow is the scheduler — it calls the logic defined in the other layers rather than replacing it.
