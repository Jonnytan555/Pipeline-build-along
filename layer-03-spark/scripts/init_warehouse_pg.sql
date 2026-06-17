-- ============================================================
-- LAYER 3 — Warehouse schema (PostgreSQL)
-- ============================================================
--
-- PostgreSQL vs T-SQL differences shown here:
--   SERIAL           → auto-increment (T-SQL uses IDENTITY(1,1))
--   VARCHAR          → T-SQL uses NVARCHAR for Unicode
--   IF NOT EXISTS    → T-SQL uses OBJECT_ID() check
--   TIMESTAMP        → T-SQL uses DATETIME2
--   ON CONFLICT      → T-SQL uses WHERE NOT EXISTS for idempotent inserts
--
-- Auto-executed by Postgres on first start via
--   /docker-entrypoint-initdb.d/
-- Run manually:
--   psql -U pipeline_user -d warehouse_db -f init_warehouse_pg.sql
-- ============================================================

-- ── Processed zone (written by Spark Layer 3) ─────────────────

CREATE TABLE IF NOT EXISTS orders_enriched (
    order_id        INTEGER PRIMARY KEY,
    customer_name   VARCHAR(200),
    customer_email  VARCHAR(200),
    country         VARCHAR(50),
    product_name    VARCHAR(200),
    category        VARCHAR(100),
    quantity        INTEGER,
    unit_price      DECIMAL(10,2),
    total_amount    DECIMAL(10,2),
    status          VARCHAR(20),
    order_date      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS revenue_by_customer (
    customer_name   VARCHAR(200),
    country         VARCHAR(50),
    order_count     INTEGER,
    total_revenue   DECIMAL(10,2),
    avg_order_value DECIMAL(10,2),
    as_of_date      DATE
);

CREATE TABLE IF NOT EXISTS revenue_by_category (
    category        VARCHAR(100),
    order_count     INTEGER,
    total_revenue   DECIMAL(10,2),
    avg_order_value DECIMAL(10,2),
    as_of_date      DATE
);

-- ── Star schema (built by Layer 8) ────────────────────────────

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key    SERIAL PRIMARY KEY,
    customer_name   VARCHAR(100) NOT NULL,
    country         VARCHAR(50)  NOT NULL,
    CONSTRAINT uq_dim_customer UNIQUE (customer_name, country)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key     SERIAL PRIMARY KEY,
    product_name    VARCHAR(200) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    CONSTRAINT uq_dim_product UNIQUE (product_name, category)
);

-- No FK constraints so TRUNCATE doesn't need CASCADE
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id        INTEGER PRIMARY KEY,
    customer_key    INTEGER,
    product_key     INTEGER,
    quantity        INTEGER,
    unit_price      DECIMAL(10,2),
    total_amount    DECIMAL(10,2),
    status          VARCHAR(20),
    order_date      TIMESTAMP
);
