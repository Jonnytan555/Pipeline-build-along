"""
LAYER 3 — Spark Batch ETL
=========================

What you will learn:
  - Running a PySpark job inside a Spark standalone cluster (Docker)
  - Configuring Spark to read from MinIO using the S3A protocol
  - DataFrame transformations: join, filter, select, cast, groupBy
  - Writing Spark DataFrames to SQL Server via JDBC
  - The "processed zone" pattern: raw CSV in MinIO → clean rows in MSSQL

Data flow:
    MinIO raw-data/ (CSV)
        → Spark (join + aggregate)
            → SQL Server source_db
                → orders_enriched
                → revenue_by_customer
                → revenue_by_category

Run (containers must be up):
    make l3-run
    # which executes:
    # docker exec spark-master spark-submit \\
    #     --master spark://spark-master:7077 \\
    #     /opt/spark/jobs/batch_job.py

Prerequisites:
    make l2-run   ← uploads today's CSV files to MinIO first
    Run layer-03-spark/scripts/init_warehouse.sql in SSMS first
"""

import logging
import os
import tempfile
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

try:
    from utils.logger import setup_log
    setup_log(app="layer-03-spark", use_stream=True)
except ImportError:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",        "http://localhost:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER",      "minioadmin")
MINIO_PASS     = os.getenv("MINIO_ROOT_PASSWORD",  "minioadmin")
RAW_BUCKET     = os.getenv("MINIO_BUCKET_RAW",     "raw-data")

MSSQL_URL   = os.getenv("MSSQL_JDBC_URL", "jdbc:sqlserver://localhost:1433;databaseName=source_db;encrypt=false;integratedSecurity=false")
MSSQL_PROPS = {
    "user":     os.getenv("DB_USER",      "sa"),
    "password": os.getenv("SA_PASSWORD",  "december1"),
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# PostgreSQL — runs in Docker (postgres:16), port 5432 mapped to host.
# When Spark runs inside Docker, set POSTGRES_JDBC_URL=jdbc:postgresql://postgres:5432/warehouse_db
PG_URL   = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://localhost:5432/warehouse_db")
PG_PROPS = {
    "user":     os.getenv("POSTGRES_USER",     "pipeline_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "pipeline_pass"),
    "driver":   "org.postgresql.Driver",
}


# ── Spark Session ─────────────────────────────────────────────
def build_session() -> SparkSession:
    """
    Configure Spark to talk to MinIO via the S3A protocol.

    Key S3A settings for MinIO:
      endpoint         → MinIO's host:port instead of AWS
      path.style.access → MinIO requires path-style URLs
                          (http://host/bucket/key) not
                          virtual-hosted (http://bucket.host/key)
      impl             → tells Hadoop which FileSystem class to use

    In production: remove endpoint and path.style.access; set
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY as env vars.
    """
    return (
        SparkSession.builder
        .appName("BatchIngestion")
        .getOrCreate()
    )


# ── Extract ───────────────────────────────────────────────────
def _minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASS,
        config=Config(signature_version="s3v4"),
    )


def read_latest(spark: SparkSession, table: str):
    """
    Download today's CSV partition from MinIO via boto3, then read
    into a Spark DataFrame from a local temp file.

    WHY boto3 instead of S3A directly?
      S3A requires hadoop-aws + aws-java-sdk JARs on the classpath.
      In Docker those JARs are baked into the image (see Dockerfile).
      For local dev without Docker, boto3 uses Python's HTTP stack
      (no extra JARs needed) and achieves the same result.

    Path convention: <bucket>/<table>/<YYYY-MM-DD>/<table>.csv
    """
    today  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3_key = f"{table}/{today}/{table}.csv"
    tmp    = os.path.join(tempfile.gettempdir(), f"{table}_{today}.csv")

    log.info("Downloading s3://%s/%s -> %s", RAW_BUCKET, s3_key, tmp)
    _minio_client().download_file(RAW_BUCKET, s3_key, tmp)

    return spark.read.csv(tmp, header=True, inferSchema=True)


# ── Transform ─────────────────────────────────────────────────
def build_orders_enriched(orders, customers, products):
    """
    Join orders with the customer and product dimension tables.

    WHY pre-select columns on each side before joining?
      Both customers and products have a column called 'name'.
      Selecting and aliasing before the join avoids an ambiguous-
      column error and makes the SELECT list explicit.

    WHY filter cancelled orders?
      Cancelled orders are kept in the raw zone (MinIO) for auditing
      but should not inflate revenue metrics in the warehouse.
    """
    cust = customers.select(
        "customer_id",
        F.col("name").alias("customer_name"),
        "email",
        "country",
    )
    prod = products.select(
        "product_id",
        F.col("name").alias("product_name"),
        "category",
    )
    return (
        orders
        .join(cust, "customer_id", "left")
        .join(prod, "product_id",  "left")
        .filter(F.col("status") != "cancelled")
        .select(
            F.col("order_id").cast("int"),
            "customer_name",
            F.col("email").alias("customer_email"),
            "country",
            "product_name",
            "category",
            F.col("quantity").cast("int"),
            F.col("unit_price").cast(DecimalType(10, 2)),
            F.col("total_amount").cast(DecimalType(10, 2)),
            "status",
            F.col("order_date").cast("timestamp"),
        )
    )


def build_revenue_by_customer(enriched):
    """
    Pre-aggregate revenue per customer.

    Pre-aggregation is a core warehouse pattern: you pay the
    GROUP BY cost once at load time so every downstream query
    (API, BI tool) reads a tiny aggregated table instead of
    scanning all orders.
    """
    today_str = datetime.now(timezone.utc).date().isoformat()
    return (
        enriched
        .groupBy("customer_name", "country")
        .agg(
            F.count("order_id").alias("order_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        )
        .withColumn("as_of_date", F.lit(today_str))
        .orderBy(F.desc("total_revenue"))
    )


def build_revenue_by_category(enriched):
    today_str = datetime.now(timezone.utc).date().isoformat()
    return (
        enriched
        .groupBy("category")
        .agg(
            F.count("order_id").alias("order_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        )
        .withColumn("as_of_date", F.lit(today_str))
        .orderBy(F.desc("total_revenue"))
    )


# ── Load ──────────────────────────────────────────────────────
def write_to_mssql(df, table: str, mode: str = "overwrite") -> None:
    """
    Write a Spark DataFrame to SQL Server via JDBC.

    mode="overwrite" truncates and replaces the table on each run —
    fine for a daily full-refresh pipeline.
    """
    count = df.count()
    log.info("  Writing %s rows -> mssql/%s", f"{count:,}", table)
    df.write.jdbc(url=MSSQL_URL, table=table, mode=mode, properties=MSSQL_PROPS)


def write_to_pg(df, table: str, mode: str = "overwrite") -> None:
    """
    Write a Spark DataFrame to PostgreSQL via JDBC.

    WHY write to both SQL Server and PostgreSQL?
      - SQL Server = the existing enterprise source system
      - PostgreSQL = the portable Docker-friendly warehouse
      - Running both lets you compare DDL syntax and driver
        behaviour side by side (IDENTITY vs SERIAL, DATETIME2
        vs TIMESTAMP, T-SQL vs standard SQL etc.)

    When running the full stack in Docker, set:
      POSTGRES_JDBC_URL=jdbc:postgresql://postgres:5432/warehouse_db
    """
    count = df.count()
    log.info("  Writing %s rows -> postgres/%s", f"{count:,}", table)
    df.write.jdbc(url=PG_URL, table=table, mode=mode, properties=PG_PROPS)


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 60)
    log.info("LAYER 3 — Spark Batch ETL")
    log.info("=" * 60)

    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    log.info("[1] Extracting raw CSV from MinIO ...")
    orders    = read_latest(spark, "orders")
    customers = read_latest(spark, "customers")
    products  = read_latest(spark, "products")

    log.info("[2] Transforming ...")
    enriched        = build_orders_enriched(orders, customers, products)
    rev_by_customer = build_revenue_by_customer(enriched)
    rev_by_category = build_revenue_by_category(enriched)

    log.info("[3a] Loading to SQL Server ...")
    write_to_mssql(enriched,        "orders_enriched")
    write_to_mssql(rev_by_customer, "revenue_by_customer")
    write_to_mssql(rev_by_category, "revenue_by_category")

    log.info("[3b] Loading to PostgreSQL ...")
    write_to_pg(enriched,        "orders_enriched")
    write_to_pg(rev_by_customer, "revenue_by_customer")
    write_to_pg(rev_by_category, "revenue_by_category")

    log.info("[4] Preview — revenue by category:")
    rev_by_category.show(truncate=False)

    log.info("[5] Preview — top customers:")
    rev_by_customer.show(5, truncate=False)

    log.info("=" * 60)
    log.info("Spark ETL complete. Data written to SQL Server + PostgreSQL.")
    log.info("Next -> Layer 4: Airflow schedules this pipeline on a cron.")
    log.info("=" * 60)

    spark.stop()
