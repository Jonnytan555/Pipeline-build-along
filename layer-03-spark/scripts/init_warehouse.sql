-- ============================================================
-- LAYER 3 — SQL Server Warehouse Schema (T-SQL)
--
-- Creates the warehouse tables inside source_db.
-- These are the "processed zone" tables written by the pipeline:
--
--   orders_enriched      → flat joined record per non-cancelled order
--   revenue_by_customer  → pre-aggregated revenue per customer
--   revenue_by_category  → pre-aggregated revenue per category
--
-- Run this in SSMS against your local SQL Server instance
-- after running layer-01-mssql/scripts/init_db.sql.
-- Idempotent — safe to run multiple times.
-- ============================================================

USE source_db;
GO

-- ── orders_enriched ───────────────────────────────────────────
-- Flat, joined record of every non-cancelled order.
-- Written by the Spark batch job (Layer 3) and the Airflow
-- pandas transform (Layer 4).
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'orders_enriched')
CREATE TABLE orders_enriched (
    order_id        INT             PRIMARY KEY,
    customer_name   NVARCHAR(100),
    customer_email  NVARCHAR(150),
    country         NVARCHAR(50),
    product_name    NVARCHAR(200),
    category        NVARCHAR(100),
    quantity        INT,
    unit_price      DECIMAL(10,2),
    total_amount    DECIMAL(10,2),
    status          NVARCHAR(20),
    order_date      DATETIME2
);
GO

-- ── revenue_by_customer ───────────────────────────────────────
-- Pre-aggregated per-customer revenue.
-- as_of_date marks which pipeline run produced the row.
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'revenue_by_customer')
CREATE TABLE revenue_by_customer (
    customer_name       NVARCHAR(100),
    country             NVARCHAR(50),
    order_count         INT,
    total_revenue       DECIMAL(12,2),
    avg_order_value     DECIMAL(10,2),
    as_of_date          DATE
);
GO

-- ── revenue_by_category ───────────────────────────────────────
-- Same pattern as above, grouped by product category.
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'revenue_by_category')
CREATE TABLE revenue_by_category (
    category            NVARCHAR(100),
    order_count         INT,
    total_revenue       DECIMAL(12,2),
    avg_order_value     DECIMAL(10,2),
    as_of_date          DATE
);
GO

-- ── star schema tables (Layer 8) ──────────────────────────────
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_customer')
CREATE TABLE dim_customer (
    customer_key    INT             PRIMARY KEY IDENTITY(1,1),
    customer_name   NVARCHAR(100)   NOT NULL,
    country         NVARCHAR(50)    NOT NULL,
    CONSTRAINT uq_dim_customer UNIQUE (customer_name, country)
);
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_product')
CREATE TABLE dim_product (
    product_key     INT             PRIMARY KEY IDENTITY(1,1),
    product_name    NVARCHAR(200)   NOT NULL,
    category        NVARCHAR(100)   NOT NULL,
    CONSTRAINT uq_dim_product UNIQUE (product_name, category)
);
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_orders')
CREATE TABLE fact_orders (
    order_id        INT             PRIMARY KEY,
    customer_key    INT             REFERENCES dim_customer(customer_key),
    product_key     INT             REFERENCES dim_product(product_key),
    quantity        INT,
    unit_price      DECIMAL(10,2),
    total_amount    DECIMAL(10,2),
    status          NVARCHAR(20),
    order_date      DATETIME2
);
GO
