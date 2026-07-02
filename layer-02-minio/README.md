# Layer 2 — MinIO Data Lake (Extract)

Extracts tables from SQL Server and uploads them as CSV files to MinIO — an S3-compatible object store that acts as the raw data lake. No intermediate files are written to disk; data travels via in-memory BytesIO buffers.

## What it does

- Reads full tables from SQL Server using pandas
- Serialises each DataFrame to CSV in memory
- Uploads to MinIO under a date-partitioned path: `raw-data/{table}/YYYY-MM-DD/{table}.csv`
- Lists bucket contents and previews the uploaded files

## Files

| File | Purpose |
|---|---|
| `extract_and_upload.py` | Extract → upload pipeline |

## Run

```bash
python layer-02-minio/extract_and_upload.py
```

Requires MinIO and MSSQL containers:

```bash
docker compose up -d mssql minio
```

## MinIO config (from `.env`)

| Variable | Default |
|---|---|
| `MINIO_ENDPOINT` | `http://localhost:9000` |
| `MINIO_ROOT_USER` | `minioadmin` |
| `MINIO_ROOT_PASSWORD` | `minioadmin` |
| `RAW_BUCKET` | `raw-data` |

MinIO browser UI: [http://localhost:9001](http://localhost:9001)

## Output path pattern

```
raw-data/
  customers/
    2024-01-15/
      customers.csv
  products/
    2024-01-15/
      products.csv
  orders/
    2024-01-15/
      orders.csv
```

## Pipeline position

```
Layer 1 (MSSQL)  →  [Layer 2: MinIO extract]  →  Layer 3 (Spark ETL)
```

Layer 3 reads the date-partitioned CSVs from MinIO for batch processing.
