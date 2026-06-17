"""
LAYER 2 — Extract from SQL Server → Upload to MinIO (Data Lake)
===============================================================

What you will learn:
  - The "Extract" step of ETL: pull data from the source DB
  - Writing a pandas DataFrame to CSV in memory (no temp file)
  - boto3: the AWS/S3 SDK — works identically against MinIO
  - The "raw zone" pattern: store data exactly as it arrived
  - Date partitioning: organising files by date for fast queries

Data flow:
    SQL Server (source_db) → pandas DataFrame → CSV bytes → MinIO raw-data/

Run:
    python layer-02-minio/extract_and_upload.py
"""

import io
import logging
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from utils.db import Database
from utils.logger import setup_log

setup_log(app="layer-02-minio", use_stream=True)
log = logging.getLogger(__name__)

db = Database(
    name=os.getenv("DB_NAME", "source_db"),
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", ""),
    password=os.getenv("SA_PASSWORD", ""),
)

# boto3 talks to MinIO using the S3 protocol.
# The only difference from real AWS S3 is endpoint_url.
# In production: remove endpoint_url and set AWS credentials.
S3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    region_name="us-east-1",
)

RAW_BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw-data")


# ── Extract ───────────────────────────────────────────────────
def extract_table(table: str) -> pd.DataFrame:
    """
    Pull a full table from SQL Server into a pandas DataFrame.

    In production you would add:
        WHERE order_date > @last_watermark
    to only fetch new/changed rows (incremental extraction).
    Airflow (Layer 4) will manage the watermark automatically.
    """
    log.info("Extracting [%s] ...", table)
    df = db.select(table, limit=None)
    log.info("  -> %s rows", len(df))
    return df


# ── Upload ────────────────────────────────────────────────────
def upload_csv(df: pd.DataFrame, bucket: str, key: str) -> None:
    """
    Upload a DataFrame as CSV to MinIO/S3.

    Design decisions:
      1. io.BytesIO buffer — no temp file on disk.
      2. Date-partitioned key, e.g.:
             raw-data/orders/2024-01-15/orders.csv
         Allows querying "all orders from Jan" by listing a
         prefix rather than scanning one giant file.
      3. CSV here for readability. In production: Parquet
         (columnar, compressed, ~10x smaller, Spark-native).
    """
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)

    S3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(), ContentType="text/csv")
    log.info("  -> s3://%s/%s  (%.1f KB)", bucket, key, len(buf.getvalue()) / 1024)


# ── Verify ────────────────────────────────────────────────────
def list_bucket(bucket: str) -> None:
    log.info("Contents of bucket '%s':", bucket)
    try:
        objects = S3.list_objects_v2(Bucket=bucket).get("Contents", [])
        if not objects:
            log.info("  (empty)")
            return
        for obj in objects:
            ts = obj["LastModified"].strftime("%Y-%m-%d %H:%M")
            log.info("  %-55s  %6.1f KB  %s", obj['Key'], obj['Size'] / 1024, ts)
    except ClientError as e:
        log.error("Error listing bucket: %s", e)


def preview_file(bucket: str, key: str) -> None:
    """Download back from MinIO and preview — verifies the round-trip."""
    log.info("Preview -> s3://%s/%s", bucket, key)
    obj = S3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    log.info("\n%s", df.head(3).to_string(index=False))


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    log.info("=" * 60)
    log.info("LAYER 2 — Extract -> Data Lake (MinIO)")
    log.info("=" * 60)

    tables = ["customers", "products", "orders"]
    uploaded = {}

    for table in tables:
        log.info("[%s]", table)
        df = extract_table(table)
        key = f"{table}/{today}/{table}.csv"
        upload_csv(df, RAW_BUCKET, key)
        uploaded[table] = key

    list_bucket(RAW_BUCKET)
    preview_file(RAW_BUCKET, uploaded["orders"])

    log.info("=" * 60)
    log.info("Raw data is now in the data lake.")
    log.info("Next -> Layer 3: Spark reads from MinIO, transforms,")
    log.info("        and writes clean data to SQL Server.")
    log.info("=" * 60)
