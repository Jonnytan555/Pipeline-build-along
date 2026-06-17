# ============================================================
# LAYER 4 — Custom Airflow image
#
# The official apache/airflow image does not include the
# project's data-engineering dependencies (pymssql, boto3,
# pandas, psycopg2).  We extend it with the extra packages so
# that DAG tasks can connect to MSSQL, MinIO, and PostgreSQL
# from inside the container.
#
# USER airflow is required — the base image drops privileges
# and pip installs must happen as that user (not root) to land
# in the correct site-packages.
# ============================================================

FROM apache/airflow:2.8.0

USER airflow

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt
