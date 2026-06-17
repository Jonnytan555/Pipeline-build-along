# ============================================================
# LAYER 9 — FastAPI container
#
# Uses utils/db.py (SQLAlchemy + pyodbc) to connect to SQL
# Server, so we need:
#   1. Microsoft ODBC Driver 17 for SQL Server (Linux)
#   2. unixodbc-dev  → pyodbc build dependency
#   3. The utils/ package copied into /app/utils/
#
# WHY python:3.11-slim and not alpine?
#   pyodbc requires gcc and unixodbc headers to compile.
#   slim has apt-get; alpine uses apk and has compatibility
#   issues with the Microsoft ODBC driver packages.
# ============================================================

FROM python:3.11-slim

WORKDIR /app

# Install Microsoft ODBC Driver 17 for SQL Server
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl gnupg2 unixodbc-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc \
        | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list \
        > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    fastapi==0.115.0 \
    uvicorn[standard]==0.30.6 \
    pydantic==2.9.2 \
    pyodbc==5.1.0 \
    sqlalchemy==2.0.23 \
    pandas==2.1.4 \
    redis==5.0.1 \
    prometheus-client==0.19.0 \
    concurrent-log-handler==0.9.25

# Copy shared utilities and the app
# Build context is the project root so paths are relative to that
COPY utils ./utils
COPY layer-09-api/main.py .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
