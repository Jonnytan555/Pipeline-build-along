"""
LAYER 1 — Querying the SQL Server source system
================================================

What you will learn:
  - Connecting Python to SQL Server via SQLAlchemy + pyodbc
  - Using the shared Database utility (utils/db.py)
  - T-SQL query patterns: JOIN, GROUP BY, HAVING
  - Why a pipeline reads from the source but never writes back
  - The difference between OLTP queries (this file) and the
    aggregated queries a warehouse pre-computes (Layer 8)

Install dependency:
    pip install sqlalchemy pyodbc pandas

Run (with containers up):
    python layer-01-mssql/query.py
"""

import logging
import os

from utils.db import Database
from utils.logger import setup_log

setup_log(app="layer-01-mssql", use_stream=True)
log = logging.getLogger(__name__)

db = Database(
    name=os.getenv("DB_NAME", "source_db"),
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", ""),
    password=os.getenv("SA_PASSWORD", ""),
)


# ── Queries ───────────────────────────────────────────────────

def show_row_counts():
    log.info("=== Row counts (extraction audit) ===")
    for table in ("customers", "products", "orders"):
        df = db.query(f"SELECT COUNT(*) AS cnt FROM {table}")
        log.info("  %-12s  %s rows", table, df['cnt'].iloc[0])


def show_all_orders():
    """
    Full order list with customer and product names joined in.

    In a real incremental pipeline you would add:
        WHERE o.order_date > @last_watermark
    to only pull new rows. Airflow (Layer 4) will manage
    that watermark automatically.
    """
    log.info("=== All orders ===")
    df = db.query("""
        SELECT
            o.order_id,
            c.name          AS customer,
            p.name          AS product,
            p.category,
            o.quantity,
            o.unit_price,
            o.total_amount,
            o.status,
            CONVERT(VARCHAR, o.order_date, 23) AS order_date
        FROM orders o
        JOIN customers c ON c.customer_id = o.customer_id
        JOIN products  p ON p.product_id  = o.product_id
        ORDER BY o.order_date
    """)
    for _, row in df.iterrows():
        log.info(
            "  #%2s  %-15s %-22s qty=%s  £%8.2f  [%s]",
            row['order_id'], row['customer'], row['product'],
            row['quantity'], float(row['total_amount']), row['status'],
        )


def show_revenue_by_category():
    """
    Aggregation query — the kind a BI analyst would run daily.

    The warehouse layer (Layer 8) will pre-compute this so the
    BI tool never hits the live OLTP database directly.
    That separation is the core purpose of a data warehouse.
    """
    log.info("=== Revenue by product category (excluding cancelled) ===")
    df = db.query("""
        SELECT
            p.category,
            COUNT(o.order_id)                     AS order_count,
            SUM(o.total_amount)                   AS total_revenue,
            AVG(CAST(o.total_amount AS FLOAT))    AS avg_order_value
        FROM orders o
        JOIN products p ON p.product_id = o.product_id
        WHERE o.status != 'cancelled'
        GROUP BY p.category
        ORDER BY total_revenue DESC
    """)
    for _, row in df.iterrows():
        log.info(
            "  %-15s  orders=%s  revenue=£%9.2f  avg=£%7.2f",
            row['category'], row['order_count'],
            float(row['total_revenue']), row['avg_order_value'],
        )


def show_pending_orders():
    """Orders still needing action — useful for operational alerting."""
    log.info("=== Orders needing action (pending or shipped) ===")
    df = db.query("""
        SELECT
            o.order_id,
            c.name      AS customer,
            c.email,
            p.name      AS product,
            o.status,
            CONVERT(VARCHAR, o.order_date, 23) AS order_date
        FROM orders o
        JOIN customers c ON c.customer_id = o.customer_id
        JOIN products  p ON p.product_id  = o.product_id
        WHERE o.status IN ('pending', 'shipped')
        ORDER BY o.order_date
    """)
    if df.empty:
        log.info("  None.")
        return
    for _, row in df.iterrows():
        log.info(
            "  #%s  %-15s  %-22s  [%s]  -> %s",
            row['order_id'], row['customer'], row['product'],
            row['status'], row['email'],
        )


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Connecting to SQL Server source database...")
    show_row_counts()
    show_all_orders()
    show_revenue_by_category()
    show_pending_orders()
    log.info("Done. Next step -> Layer 2: extract this data and land it in MinIO (data lake).")
