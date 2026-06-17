# ============================================================
# PIPELINE BUILD-ALONG — Makefile
# Usage: make <target>
# ============================================================

.PHONY: help up down down-v logs install \
        l1-up l1-shell l1-run \
        l2-up l2-run \
        l3-up l3-run \
        l4-up l4-trigger \
        l5-up l5-consume \
        l6-up l6-run \
        l7-run \
        l8-run \
        l9-up \
        l10-up \
        l11-up l11-run

help:
	@echo ""
	@echo "  make up           Start all services in docker-compose.yml"
	@echo "  make down         Stop containers (keeps volumes/data)"
	@echo "  make down-v       Stop containers AND wipe all volumes"
	@echo "  make logs         Tail logs for all services"
	@echo "  make install      pip install -r requirements.txt"
	@echo ""
	@echo "  Layer 1 — SQL Server source system"
	@echo "  make l1-up        Start SQL Server + run init script"
	@echo "  make l1-shell     Open a sqlcmd shell (manual inspection)"
	@echo "  make l1-run       Run the Layer 1 query script"
	@echo ""
	@echo "  Layer 2 — MinIO data lake"
	@echo "  make l2-up        Start SQL Server + MinIO"
	@echo "  make l2-run       Extract from MSSQL and upload to MinIO"
	@echo ""
	@echo "  Layer 3 — Spark batch ETL"
	@echo "  make l3-up        Start everything up to Spark + Postgres"
	@echo "  make l3-run       Submit Spark batch job"
	@echo ""
	@echo "  Layer 4 — Airflow orchestration"
	@echo "  make l4-up        Start everything including Airflow"
	@echo "  make l4-trigger   Trigger the batch ingestion DAG"
	@echo ""
	@echo "  Layer 5 — Kafka streaming"
	@echo "  make l5-up        Start Kafka + Zookeeper + producer"
	@echo "  make l5-consume   Run Kafka consumer script"
	@echo ""
	@echo "  Layer 6 — Multi-store sinks"
	@echo "  make l6-up        Start Redis, MongoDB, InfluxDB"
	@echo "  make l6-run       Run all three sink scripts"
	@echo ""
	@echo "  Layer 7 — Data quality"
	@echo "  make l7-run       Validate warehouse tables"
	@echo ""
	@echo "  Layer 8 — Warehouse star schema + BI queries"
	@echo "  make l8-run       Build fact/dim tables and run BI queries"
	@echo ""
	@echo "  Layer 9 — FastAPI"
	@echo "  make l9-up        Start the FastAPI container"
	@echo "  # API docs: http://localhost:8000/docs"
	@echo ""
	@echo "  Layer 10 — Monitoring"
	@echo "  make l10-up       Start Prometheus + Grafana"
	@echo "  # Grafana: http://localhost:3000   Prometheus: http://localhost:9090"
	@echo ""
	@echo "  Layer 11 — MLflow model tracking"
	@echo "  make l11-up       Start MLflow server"
	@echo "  make l11-run      Train and log model to MLflow"
	@echo "  # MLflow UI: http://localhost:5001"
	@echo ""

up:
	docker compose up -d

down:
	docker compose down

down-v:
	docker compose down -v

logs:
	docker compose logs -f

install:
	pip install -r requirements.txt

# ── Layer 1 — SQL Server ──────────────────────────────────────
l1-up:
	docker compose up -d mssql mssql-init

l1-shell:
	docker exec -it mssql /opt/mssql-tools18/bin/sqlcmd \
		-S localhost -U sa -P "december1" -C -d source_db

l1-run:
	python layer-01-mssql/query.py

# ── Layer 2 — MinIO ───────────────────────────────────────────
l2-up:
	docker compose up -d mssql mssql-init minio minio-init

l2-run:
	python layer-02-minio/extract_and_upload.py

# ── Layer 3 — Spark ───────────────────────────────────────────
l3-up:
	docker compose up -d mssql mssql-init minio minio-init \
		postgres spark-master spark-worker

l3-run:
	docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/jobs/batch_job.py

# ── Layer 4 — Airflow ─────────────────────────────────────────
l4-up:
	docker compose up -d mssql mssql-init minio minio-init \
		postgres spark-master spark-worker \
		airflow-init airflow-webserver airflow-scheduler

l4-trigger:
	python layer-04-airflow/trigger_dag.py

# ── Layer 5 — Kafka ───────────────────────────────────────────
l5-up:
	docker compose up -d zookeeper kafka kafka-producer

l5-consume:
	python layer-05-kafka/consumer.py

# ── Layer 6 — Multi-store sinks ───────────────────────────────
l6-up:
	docker compose up -d redis mongodb influxdb

l6-run:
	python layer-06-multistore/run_all_sinks.py

# ── Layer 7 — Data quality ────────────────────────────────────
l7-run:
	python layer-07-quality/validate.py

# ── Layer 8 — Warehouse star schema ───────────────────────────
l8-run:
	python layer-08-warehouse/build_facts.py && \
	python layer-08-warehouse/queries.py

# ── Layer 9 — FastAPI ─────────────────────────────────────────
l9-up:
	docker compose up -d pipeline-api

# ── Layer 10 — Monitoring ─────────────────────────────────────
l10-up:
	docker compose up -d prometheus grafana

# ── Layer 11 — MLflow ─────────────────────────────────────────
l11-up:
	docker compose up -d mlflow

l11-run:
	python layer-11-mlflow/train_model.py
