"""
LAYER 4 — Airflow DAG: batch_ingestion
=======================================

What you will learn:
  - The TaskFlow API (@dag / @task decorators) — modern Airflow
  - How XCom passes data between tasks without storing files
  - Task dependencies declared through function call order
  - default_args: retry policy applied to all tasks in the DAG
  - schedule="@daily" — cron-based execution
  - Why Airflow is an orchestrator, not an executor: it manages
    WHEN tasks run and WHAT happens on failure, not the compute

Pipeline tasks (in order):
    extract_to_lake
        ↓  (XCom: file keys)
    validate_raw_data
        ↓
    transform_to_warehouse
        ↓  (XCom: row counts)
    report_summary

Data flow inside the DAG:
    SQL Server (mssql:1433)
        → pandas DataFrame
            → MinIO raw-data/ (CSV)
                → pandas join/aggregate
                    → PostgreSQL warehouse_db

Open the Airflow UI:  http://localhost:8082
  user: admin
  pass: airflow_pass  (set in .env)

Trigger from CLI:
    make l4-trigger
    # calls: python layer-04-airflow/trigger_dag.py
"""

import io
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
import pendulum
import psycopg2
import pymssql
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

# ── Connection configs (Docker network hostnames) ─────────────
#
# Inside a Docker network, services are reachable by their
# container_name, not "localhost".  These configs mirror the
# values in .env but use the Docker service names.

MSSQL_CONFIG = dict(
    server   = os.getenv("MSSQL_HOST", "host.docker.internal"),
    port     = 1433,
    user     = "sa",
    password = os.getenv("SA_PASSWORD", "december1"),
    database = "source_db",
)

S3 = boto3.client(
    "s3",
    endpoint_url         = os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    aws_access_key_id    = os.getenv("MINIO_ROOT_USER",     "minioadmin"),
    aws_secret_access_key= os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    region_name          = "us-east-1",
)

RAW_BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw-data")

PG_CONFIG = dict(
    host     = "postgres",
    port     = 5432,
    dbname   = "warehouse_db",
    user     = os.getenv("POSTGRES_USER",     "pipeline_user"),
    password = os.getenv("POSTGRES_PASSWORD", "pipeline_pass"),
)


# ── DAG definition ────────────────────────────────────────────

@dag(
    dag_id    = "batch_ingestion",
    schedule  = "@daily",
    start_date= pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup   = False,
    default_args = {
        "retries":       2,
        "retry_delay":   timedelta(minutes=5),
        "owner":         "pipeline",
    },
    tags = ["pipeline", "batch"],
    doc_md = __doc__,
)
def batch_ingestion():

    @task()
    def extract_to_lake() -> dict:
        """
        Pull every table from SQL Server and upload to MinIO.

        Returns a dict of {table: s3_key} so downstream tasks
        know exactly which file to read — this dict travels
        via XCom (Airflow's inter-task communication mechanism).

        XCom stores small values (strings, dicts) in the Airflow
        metadata database.  Do NOT store entire DataFrames here —
        use the file keys and re-read from MinIO instead.
        """
        today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        uploaded = {}

        with pymssql.connect(**MSSQL_CONFIG) as conn:
            for table in ("customers", "products", "orders"):
                df  = pd.read_sql(f"SELECT * FROM {table}", conn)
                buf = io.BytesIO()
                df.to_csv(buf, index=False)
                buf.seek(0)

                key = f"{table}/{today}/{table}.csv"
                S3.put_object(
                    Bucket      = RAW_BUCKET,
                    Key         = key,
                    Body        = buf.getvalue(),
                    ContentType = "text/csv",
                )
                uploaded[table] = key
                log.info("Uploaded %s rows -> s3://%s/%s", len(df), RAW_BUCKET, key)

        return uploaded

    @task()
    def validate_raw_data(file_keys: dict) -> None:
        """
        Assert that every uploaded file is non-empty.

        Failing here prevents a corrupt or empty file from
        propagating into the warehouse — a downstream write with
        0 rows would silently truncate good data.
        """
        for table, key in file_keys.items():
            obj = S3.get_object(Bucket=RAW_BUCKET, Key=key)
            df  = pd.read_csv(io.BytesIO(obj["Body"].read()))

            assert len(df) > 0, f"{table} CSV is empty — aborting pipeline"
            log.info("%s: %s rows  OK", table, len(df))

    @task()
    def transform_to_warehouse(file_keys: dict) -> dict:
        """
        Read from MinIO, join and aggregate with pandas,
        write clean tables to PostgreSQL.

        This task is the pandas equivalent of the Spark job
        in Layer 3.  Pandas is fine for datasets that fit in
        memory.  When data grows beyond ~10 GB, replace this
        task with a SparkSubmitOperator pointing at batch_job.py.

        WHY overwrite instead of append?
          This is a full-refresh daily pipeline.  Each run
          produces the authoritative current snapshot.
          The raw layer (MinIO) keeps the history.
        """
        def read_csv(table: str) -> pd.DataFrame:
            obj = S3.get_object(Bucket=RAW_BUCKET, Key=file_keys[table])
            return pd.read_csv(io.BytesIO(obj["Body"].read()))

        orders    = read_csv("orders")
        customers = read_csv("customers")
        products  = read_csv("products")

        # pymssql returns DECIMAL as Python Decimal; CSV round-trip can leave
        # numeric columns as object dtype — coerce so joins and aggregations work.
        _num = lambda df, cols: df.assign(
            **{c: pd.to_numeric(df[c], errors="coerce") for c in cols if c in df.columns}
        )
        orders    = _num(orders,    ["order_id", "customer_id", "product_id",
                                     "quantity", "unit_price", "total_amount"])
        customers = _num(customers, ["customer_id"])
        products  = _num(products,  ["product_id", "price"])

        # Join
        enriched = (
            orders
            .merge(customers[["customer_id", "name", "email", "country"]],
                   on="customer_id", how="left")
            .merge(products[["product_id", "name", "category"]].rename(
                       columns={"name": "product_name"}),
                   on="product_id", how="left")
            .query("status != 'cancelled'")
            .rename(columns={"name": "customer_name", "email": "customer_email"})
        )

        today_str = datetime.now(timezone.utc).date().isoformat()

        rev_by_customer = (
            enriched
            .groupby(["customer_name", "country"], as_index=False)
            .agg(
                order_count     =("order_id",     "count"),
                total_revenue   =("total_amount", "sum"),
                avg_order_value =("total_amount", "mean"),
            )
            .assign(as_of_date=today_str)
        )

        rev_by_category = (
            enriched
            .groupby("category", as_index=False)
            .agg(
                order_count     =("order_id",     "count"),
                total_revenue   =("total_amount", "sum"),
                avg_order_value =("total_amount", "mean"),
            )
            .assign(as_of_date=today_str)
        )

        # Load to PostgreSQL
        with psycopg2.connect(**PG_CONFIG) as conn:
            with conn.cursor() as cur:
                # Truncate then insert — simpler than UPSERT for daily full-refresh
                for table in ("orders_enriched", "revenue_by_customer", "revenue_by_category"):
                    cur.execute(f"TRUNCATE TABLE {table}")

            conn.commit()

        # Use pandas to_sql for the actual inserts
        import sqlalchemy
        engine = sqlalchemy.create_engine(
            f"postgresql+psycopg2://{PG_CONFIG['user']}:{PG_CONFIG['password']}"
            f"@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['dbname']}"
        )

        cols_enriched = [
            "order_id", "customer_name", "customer_email", "country",
            "product_name", "category", "quantity", "unit_price",
            "total_amount", "status", "order_date",
        ]
        enriched[cols_enriched].to_sql(
            "orders_enriched", engine, if_exists="append", index=False
        )
        rev_by_customer.to_sql(
            "revenue_by_customer", engine, if_exists="append", index=False
        )
        rev_by_category.to_sql(
            "revenue_by_category", engine, if_exists="append", index=False
        )

        stats = {
            "orders_enriched":    len(enriched),
            "revenue_by_customer": len(rev_by_customer),
            "revenue_by_category": len(rev_by_category),
        }
        log.info("Wrote: %s", stats)
        return stats

    @task()
    def report_summary(stats: dict) -> None:
        """
        Log the run summary.  In production: send a Slack message,
        write a metrics row, or call a monitoring webhook here.
        """
        log.info("=== Pipeline Run Summary ===")
        for table, count in stats.items():
            log.info("  %-30s  %s rows", table, count)
        log.info("Pipeline complete.")

    # ── Wire up task dependencies ─────────────────────────────
    # The TaskFlow API infers dependencies from function arguments.
    # Calling validate_raw_data(keys) means: run validate AFTER
    # extract, and pass the XCom return value as the argument.

    keys  = extract_to_lake()
    validate_raw_data(keys)
    stats = transform_to_warehouse(keys)
    report_summary(stats)


batch_ingestion()
