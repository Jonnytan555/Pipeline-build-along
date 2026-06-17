# Layer 3 — Spark Batch ETL

## Why does this layer exist?

Layer 2 put raw CSV files in MinIO. Now you need to:

- Join the three tables together
- Filter out cancelled orders
- Aggregate revenue by customer and category
- Write clean, query-ready data to SQL Server

You could do that with pandas. But pandas runs on one machine, in memory. If orders grows to 50 million rows, pandas falls over. **Spark is the answer to "what if the data doesn't fit on one machine"** — it splits the work across a cluster of workers.

Even at small scale, Spark is worth learning here because it's the dominant batch processing engine in enterprise data engineering. Databricks (the most common managed Spark) is a billion-dollar business built entirely on it.

---

## What Spark actually is

```
Your script (batch_job.py)
        │
        ▼
  Spark Driver          ← coordinates the job, runs your Python code
        │
   ┌────┴────┐
   ▼         ▼
Worker 1   Worker 2    ← each gets a partition of the data to process
(orders     (orders
 rows 1-5M)  rows 5-10M)
```

### Key mental model

**Spark doesn't process data line by line — it describes a plan, then executes it all at once.**

When you write:

```python
df.filter(F.col("status") != "cancelled").select("order_id", "customer_name")
```

Nothing has actually run yet. Spark builds a **logical plan** of transformations. It only executes when you call an **action** — `count()`, `show()`, `write.jdbc(...)`. This is called **lazy evaluation** and it lets Spark optimise the entire chain before touching a single row.

---

## The custom Docker image — Dockerfile

This is the first layer that needs a custom-built image:

```dockerfile
FROM bitnami/spark:3.5

RUN curl -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar      # S3A connector
    curl -o /opt/bitnami/spark/jars/aws-java-sdk-bundle.jar    # AWS SDK
    curl -o /opt/bitnami/spark/jars/mssql-jdbc-12.4.2.jre11.jar  # JDBC driver
```

Spark is a JVM application. To connect to MinIO or SQL Server it needs **JARs** on its classpath. The vanilla `bitnami/spark` image has neither. We download them at build time so they're baked into the image.

| JAR | What it unlocks |
|---|---|
| `hadoop-aws` | The `s3a://` filesystem protocol — lets Spark read/write S3/MinIO |
| `aws-java-sdk-bundle` | Required by hadoop-aws to talk to the S3 API |
| `mssql-jdbc` | JDBC driver — lets Spark write to SQL Server via `.write.jdbc()` |

**Version pinning matters.** Spark 3.5 ships with Hadoop 3.3 internally. If you use `hadoop-aws-3.4.x`, the classpath versions clash and you get cryptic `NoSuchMethodError` exceptions.

---

## The batch job — jobs/batch_job.py

Four sections — E, T, T, L:

### 1. Build the SparkSession with S3A config

```python
SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint",          "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
```

Three settings that are non-obvious:

- `endpoint` — redirect S3A from AWS to your MinIO container
- `path.style.access` — MinIO requires `http://host/bucket/key` URLs. Without this, every request 404s
- `impl` — tells Hadoop which Java class handles `s3a://` URLs

In production against real AWS S3: remove all three and set `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` as env vars.

### 2. Read the date-partitioned CSVs from MinIO

```python
path = f"s3a://raw-data/{table}/{today}/{table}.csv"
spark.read.csv(path, header=True, inferSchema=True)
```

`inferSchema=True` tells Spark to sample the file and guess column types. Fine for development. In production define an explicit schema — `inferSchema` does a full scan costing an extra read pass.

### 3. Transform — join + filter + aggregate

```python
# Both customers and products have a column called 'name' — alias before joining
cust = customers.select("customer_id", F.col("name").alias("customer_name"), ...)
enriched = orders.join(cust, "customer_id", "left").join(prod, "product_id", "left")
```

The explicit `.alias()` before joining is required because both `customers` and `products` have a `name` column. Selecting and renaming **before** the join avoids an ambiguous column reference error — this trips everyone up the first time.

The `F.` prefix — `from pyspark.sql import functions as F` — is the standard import for Spark's built-in column functions: `F.col()`, `F.sum()`, `F.round()`, `F.lit()`.

### 4. Pre-aggregation — the warehouse pattern

```python
enriched.groupBy("customer_name", "country").agg(
    F.count("order_id").alias("order_count"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
)
```

This GROUP BY runs once at load time. Every subsequent API call or BI query reads 5 rows instead of scanning 50 million orders. This is the core performance argument for a data warehouse.

### 5. Write to SQL Server via JDBC

```python
df.write.jdbc(
    url="jdbc:sqlserver://mssql:1433;databaseName=source_db;encrypt=false",
    table="orders_enriched",
    mode="overwrite",
    properties={"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver", ...}
)
```

`mode="overwrite"` truncates and replaces the table on every run — correct for a daily full-refresh pipeline.

---

## The warehouse schema — scripts/init_warehouse.sql

**Run this in SSMS** against your local SQL Server instance after `init_db.sql`. It creates the warehouse tables inside `source_db`:

- `orders_enriched` — flat joined record per non-cancelled order
- `revenue_by_customer` — pre-aggregated revenue per customer
- `revenue_by_category` — pre-aggregated revenue per category
- `dim_customer`, `dim_product`, `fact_orders` — star schema (Layer 8)

Unlike PostgreSQL, SQL Server does not auto-execute scripts on container start — hence `mssql-init` in `docker-compose.yml` and the manual SSMS step locally.

---

## The standalone cluster in docker-compose.yml

```yaml
spark-master:
  environment:
    SPARK_MODE: master
  ports:
    - "8080:8080"   # Spark Web UI
    - "7077:7077"   # Spark master port (workers register here)

spark-worker:
  environment:
    SPARK_MODE: worker
    SPARK_MASTER_URL: spark://spark-master:7077
    SPARK_WORKER_MEMORY: 1g
```

One worker here — in production you'd have tens or hundreds. The Spark Web UI at http://localhost:8080 shows running jobs, completed stages, and the execution plan.

---

## Run it

```bash
make l3-up      # start SQL Server, MinIO, Spark
make l2-run     # upload today's CSV files to MinIO first
make l3-run     # submit the Spark job
```

`make l3-run` executes:

```bash
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/batch_job.py
```

**Expected output:**

```
============================================================
LAYER 3 — Spark Batch ETL
============================================================

[1] Extracting raw CSV from MinIO ...
  Reading s3a://raw-data/orders/2026-06-02/orders.csv
  Reading s3a://raw-data/customers/2026-06-02/customers.csv
  Reading s3a://raw-data/products/2026-06-02/products.csv

[2] Transforming ...

[3] Loading to SQL Server warehouse ...
  Writing 9 rows -> mssql/orders_enriched
  Writing 5 rows -> mssql/revenue_by_customer
  Writing 3 rows -> mssql/revenue_by_category

[4] Preview — revenue by category:
+-------------+-----------+-------------+---------------+----------+
|category     |order_count|total_revenue|avg_order_value|as_of_date|
+-------------+-----------+-------------+---------------+----------+
|Electronics  |6          |3558.95      |593.16         |2026-06-02|
|Furniture    |4          |2047.50      |511.88         |2026-06-02|
|Books        |3          |149.94       |49.98          |2026-06-02|
+-------------+-----------+-------------+---------------+----------+

Spark ETL complete.
Data is now in SQL Server source_db.
Next -> Layer 4: Airflow schedules this pipeline on a cron.
```

Spark is verbose — you'll see a wall of Java log output before the results. Look for the `LAYER 3` header.

**Also check:** http://localhost:8080 — the Spark UI should show one completed application called `BatchIngestion`.
