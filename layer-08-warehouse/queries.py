"""
LAYER 8 — Warehouse BI Queries
================================

What you will learn:
  - The kinds of analytical queries a warehouse enables:
      → Revenue trends, customer segments, product performance
  - Window functions: RANK, LAG, running totals
  - CTEs (Common Table Expressions) for readable multi-step SQL
  - The difference between OLTP queries (Layer 1) and OLAP
    queries (this file): OLAP pre-aggregates many rows into few

Run:
    make l8-run  (runs build_facts.py then this file)
    # or directly: python layer-08-warehouse/queries.py

Prerequisite:
    make l8-run with build_facts.py first
"""

import logging
import os

from utils.db import Database
from utils.logger import setup_log

setup_log(app="layer-08-queries", use_stream=True)
log = logging.getLogger(__name__)

db = Database(
    name="source_db",
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", "sa"),
    password=os.getenv("SA_PASSWORD", "december1"),
)


def section(title: str) -> None:
    log.info("─" * 60)
    log.info("  %s", title)
    log.info("─" * 60)


def q_revenue_by_category() -> None:
    section("Revenue by product category (descending)")
    df = db.query("""
        SELECT
            dp.category,
            COUNT(fo.order_id)               AS orders,
            SUM(fo.total_amount)             AS total_revenue,
            ROUND(AVG(fo.total_amount), 2)   AS avg_order,
            RANK() OVER (ORDER BY SUM(fo.total_amount) DESC) AS rank
        FROM fact_orders fo
        JOIN dim_product dp ON dp.product_key = fo.product_key
        GROUP BY dp.category
        ORDER BY total_revenue DESC
    """)
    for _, row in df.iterrows():
        log.info(
            "  #%s  %-15s  orders=%s  revenue=£%9.2f  avg=£%7.2f",
            row["rank"], row["category"], row["orders"],
            float(row["total_revenue"]), float(row["avg_order"]),
        )


def q_top_customers() -> None:
    section("Top customers by total spend")
    df = db.query("""
        SELECT
            dc.customer_name,
            dc.country,
            COUNT(fo.order_id)               AS orders,
            SUM(fo.total_amount)             AS total_spend,
            RANK() OVER (ORDER BY SUM(fo.total_amount) DESC) AS rank
        FROM fact_orders fo
        JOIN dim_customer dc ON dc.customer_key = fo.customer_key
        GROUP BY dc.customer_name, dc.country
        ORDER BY total_spend DESC
    """)
    for _, row in df.iterrows():
        log.info(
            "  #%s  %-15s  [%s]  orders=%s  spend=£%9.2f",
            row["rank"], row["customer_name"], row["country"],
            row["orders"], float(row["total_spend"]),
        )


def q_category_country_heatmap() -> None:
    section("Revenue heatmap: category x country")
    df = db.query("""
        SELECT
            dp.category,
            dc.country,
            SUM(fo.total_amount) AS revenue
        FROM fact_orders fo
        JOIN dim_customer dc ON dc.customer_key = fo.customer_key
        JOIN dim_product  dp ON dp.product_key  = fo.product_key
        GROUP BY dp.category, dc.country
        ORDER BY dp.category, revenue DESC
    """)
    for _, row in df.iterrows():
        bar = "█" * int(float(row["revenue"]) / 200)
        log.info(
            "  %-15s  %-5s  £%8.2f  %s",
            row["category"], row["country"], float(row["revenue"]), bar,
        )


def q_order_status_breakdown() -> None:
    section("Order status breakdown (% of non-cancelled orders)")
    # T-SQL CTE syntax is identical to PostgreSQL for this query
    df = db.query("""
        WITH totals AS (
            SELECT status, COUNT(*) AS cnt
            FROM   fact_orders
            GROUP  BY status
        ),
        grand AS (SELECT SUM(cnt) AS total FROM totals)
        SELECT t.status,
               t.cnt,
               ROUND(100.0 * t.cnt / g.total, 1) AS pct
        FROM totals t, grand g
        ORDER BY t.cnt DESC
    """)
    for _, row in df.iterrows():
        log.info("  %-12s  %s orders  (%s%%)", row["status"], row["cnt"], row["pct"])


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 60)
    log.info("LAYER 8 — Warehouse BI Queries")
    log.info("=" * 60)

    q_revenue_by_category()
    q_top_customers()
    q_category_country_heatmap()
    q_order_status_breakdown()

    log.info("=" * 60)
    log.info("Done. Next -> Layer 9: serve these results via FastAPI.")
    log.info("=" * 60)
