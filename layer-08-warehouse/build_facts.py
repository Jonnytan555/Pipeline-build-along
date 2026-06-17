"""
LAYER 8 — Warehouse: Build Fact & Dimension Tables
===================================================

What you will learn:
  - The star schema: one central fact table surrounded by
    dimension tables that describe the "who, what, where"
  - WHY a star schema?
      → BI tools generate simple two-table JOIN queries instead
        of complex multi-table ones
      → Aggregations are fast because fact rows are narrow
      → Dimensions can be updated independently (Type 1 SCD)
  - Surrogate keys: integer IDs managed by the warehouse,
    independent of the source system's natural keys
  - How to load a star schema from a denormalised flat table
    (orders_enriched) in a single transaction

T-SQL vs PostgreSQL DDL differences used here:
  IDENTITY(1,1)       → auto-increment (PostgreSQL uses SERIAL)
  IF NOT EXISTS       → T-SQL uses OBJECT_ID check, not IF NOT EXISTS for tables
  WHERE NOT EXISTS    → T-SQL's idempotent INSERT (PostgreSQL uses ON CONFLICT)
  TRUNCATE TABLE      → same in both, T-SQL requires the TABLE keyword

Tables produced:
    dim_customer (customer_name, country)
    dim_product  (product_name, category)
    fact_orders  (all measures, FK to both dims)

Run:
    make l8-run
    # equivalent to: python layer-08-warehouse/build_facts.py

Prerequisite:
    make l3-run or make l4-trigger  ← orders_enriched must exist
"""

import logging
import os

import sqlalchemy as sa

from utils.db import Database
from utils.logger import setup_log

setup_log(app="layer-08-build-facts", use_stream=True)
log = logging.getLogger(__name__)

db = Database(
    name="source_db",
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", "sa"),
    password=os.getenv("SA_PASSWORD", "december1"),
)


# ── DDL ───────────────────────────────────────────────────────
# T-SQL uses IDENTITY(1,1) for auto-increment and OBJECT_ID to
# check table existence — there is no CREATE TABLE IF NOT EXISTS.
CREATE_DIMS = [
    # dim_customer
    """
    IF OBJECT_ID('dbo.dim_customer', 'U') IS NULL
    CREATE TABLE dim_customer (
        customer_key  INT          IDENTITY(1,1) PRIMARY KEY,
        customer_name NVARCHAR(100) NOT NULL,
        country       NVARCHAR(50)  NOT NULL,
        CONSTRAINT uq_dim_customer UNIQUE (customer_name, country)
    )
    """,
    # dim_product
    """
    IF OBJECT_ID('dbo.dim_product', 'U') IS NULL
    CREATE TABLE dim_product (
        product_key  INT           IDENTITY(1,1) PRIMARY KEY,
        product_name NVARCHAR(200) NOT NULL,
        category     NVARCHAR(100) NOT NULL,
        CONSTRAINT uq_dim_product UNIQUE (product_name, category)
    )
    """,
    # fact_orders — no REFERENCES FK so TRUNCATE doesn't need cascade
    """
    IF OBJECT_ID('dbo.fact_orders', 'U') IS NULL
    CREATE TABLE fact_orders (
        order_id      INT            PRIMARY KEY,
        customer_key  INT,
        product_key   INT,
        quantity      INT,
        unit_price    DECIMAL(10,2),
        total_amount  DECIMAL(10,2),
        status        NVARCHAR(20),
        order_date    DATETIME2
    )
    """,
]


# ── Loaders ───────────────────────────────────────────────────
def build_star_schema(conn) -> dict:
    """
    Populate dim_customer, dim_product, and fact_orders from
    orders_enriched in a single database transaction.

    WHY use INSERT … WHERE NOT EXISTS for dimensions?
      The same customer or product may appear in many orders.
      We only want one dimension row per entity.
      T-SQL has no ON CONFLICT clause — WHERE NOT EXISTS is the
      idempotent equivalent: insert only if the row doesn't exist.
    """
    conn.execute(sa.text("""
        INSERT INTO dim_customer (customer_name, country)
        SELECT DISTINCT customer_name, country
        FROM   orders_enriched oe
        WHERE  NOT EXISTS (
            SELECT 1 FROM dim_customer dc
            WHERE  dc.customer_name = oe.customer_name
              AND  dc.country       = oe.country
        )
    """))
    n_cust = conn.execute(sa.text("SELECT COUNT(*) FROM dim_customer")).scalar()

    conn.execute(sa.text("""
        INSERT INTO dim_product (product_name, category)
        SELECT DISTINCT product_name, category
        FROM   orders_enriched oe
        WHERE  NOT EXISTS (
            SELECT 1 FROM dim_product dp
            WHERE  dp.product_name = oe.product_name
              AND  dp.category     = oe.category
        )
    """))
    n_prod = conn.execute(sa.text("SELECT COUNT(*) FROM dim_product")).scalar()

    conn.execute(sa.text("TRUNCATE TABLE fact_orders"))
    conn.execute(sa.text("""
        INSERT INTO fact_orders
          (order_id, customer_key, product_key,
           quantity, unit_price, total_amount, status, order_date)
        SELECT
            oe.order_id,
            dc.customer_key,
            dp.product_key,
            oe.quantity,
            oe.unit_price,
            oe.total_amount,
            oe.status,
            oe.order_date
        FROM  orders_enriched oe
        JOIN  dim_customer dc ON dc.customer_name = oe.customer_name
                             AND dc.country       = oe.country
        JOIN  dim_product  dp ON dp.product_name  = oe.product_name
                             AND dp.category      = oe.category
    """))
    n_fact = conn.execute(sa.text("SELECT COUNT(*) FROM fact_orders")).scalar()

    conn.commit()
    return {"dim_customer": n_cust, "dim_product": n_prod, "fact_orders": n_fact}


# ── Preview ───────────────────────────────────────────────────
def show_star_schema_query() -> None:
    """
    Demonstrate the canonical BI query against the star schema:
    revenue per category per country — a two-dimension slice.

    Notice: the query only joins two dimension tables, not the
    original three source tables.  That is the performance
    benefit of the star schema.
    """
    log.info("Revenue by category x country (star schema query):")
    df = db.query("""
        SELECT
            dp.category,
            dc.country,
            COUNT(fo.order_id)       AS orders,
            SUM(fo.total_amount)     AS revenue,
            AVG(fo.total_amount)     AS avg_order
        FROM  fact_orders fo
        JOIN  dim_customer dc ON dc.customer_key = fo.customer_key
        JOIN  dim_product  dp ON dp.product_key  = fo.product_key
        GROUP BY dp.category, dc.country
        ORDER BY revenue DESC
    """)
    for _, row in df.iterrows():
        log.info(
            "  %-15s  %-5s  orders=%s  revenue=£%8.2f  avg=£%7.2f",
            row["category"], row["country"], row["orders"],
            float(row["revenue"]), float(row["avg_order"]),
        )


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 60)
    log.info("LAYER 8 — Build Star Schema")
    log.info("=" * 60)

    with db.engine.begin() as conn:

        log.info("[1] Creating dimension + fact tables ...")
        for ddl in CREATE_DIMS:
            conn.execute(sa.text(ddl))

        log.info("[2] Loading data ...")
        counts = build_star_schema(conn)
        for table, n in counts.items():
            log.info("  %-20s  %s rows", table, n)

    log.info("[3] Sample BI query:")
    show_star_schema_query()

    log.info("=" * 60)
    log.info("Star schema built.")
    log.info("Run queries: python layer-08-warehouse/queries.py")
    log.info("Next -> Layer 9: expose the warehouse via FastAPI.")
    log.info("=" * 60)
