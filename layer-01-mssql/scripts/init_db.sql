-- ============================================================
-- LAYER 1 — Source System Schema (T-SQL / SQL Server)
--
-- This is your OLTP database — the live system that generates
-- transactional data. A pipeline reads from here; it never
-- writes back.
--
-- T-SQL differences from MySQL you should know:
--   IDENTITY(1,1)      → auto-increment primary key
--   NVARCHAR           → Unicode string (safer than VARCHAR)
--   GETDATE()          → equivalent of MySQL NOW()
--   AS (...) PERSISTED → computed/generated column
--   No ENUM type       → use CHECK constraint instead
-- ============================================================

-- Create the database if running against the master DB.
-- sqlcmd runs as SA so we have permission to do this.
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'source_db')
    CREATE DATABASE source_db;
GO

USE source_db;
GO

-- ----------------------------------------
-- customers — master record for each buyer
-- ----------------------------------------
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'customers')
CREATE TABLE customers (
    customer_id   INT            PRIMARY KEY IDENTITY(1,1),
    name          NVARCHAR(100)  NOT NULL,
    email         NVARCHAR(150)  NOT NULL,
    country       NVARCHAR(50)   NOT NULL DEFAULT 'UK',
    created_at    DATETIME2      NOT NULL DEFAULT GETDATE(),
    CONSTRAINT uq_customers_email UNIQUE (email)
);
GO

-- ----------------------------------------
-- products — catalogue of items for sale
-- ----------------------------------------
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'products')
CREATE TABLE products (
    product_id    INT            PRIMARY KEY IDENTITY(1,1),
    name          NVARCHAR(200)  NOT NULL,
    category      NVARCHAR(100)  NOT NULL,
    unit_price    DECIMAL(10,2)  NOT NULL,
    created_at    DATETIME2      NOT NULL DEFAULT GETDATE()
);
GO

-- ----------------------------------------
-- orders — one row per purchase
--
-- WHY store unit_price at order time?
--   The catalogue price can change later. Storing the price
--   at purchase time preserves the historical record.
--   This is the "slowly changing data" problem in miniature.
--
-- WHY CHECK constraint instead of ENUM?
--   SQL Server has no ENUM type. A CHECK constraint enforces
--   the same allowed-values rule at the database level.
-- ----------------------------------------
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'orders')
CREATE TABLE orders (
    order_id      INT            PRIMARY KEY IDENTITY(1,1),
    customer_id   INT            NOT NULL,
    product_id    INT            NOT NULL,
    quantity      INT            NOT NULL DEFAULT 1,
    unit_price    DECIMAL(10,2)  NOT NULL,
    total_amount  AS (quantity * unit_price) PERSISTED,
    status        NVARCHAR(20)   NOT NULL DEFAULT 'pending',
    order_date    DATETIME2      NOT NULL DEFAULT GETDATE(),
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    CONSTRAINT fk_orders_product  FOREIGN KEY (product_id)  REFERENCES products(product_id),
    CONSTRAINT chk_orders_status  CHECK (status IN ('pending','shipped','delivered','cancelled'))
);
GO

-- ============================================================
-- SEED DATA
-- ============================================================

INSERT INTO customers (name, email, country) VALUES
    ('Alice Smith',   'alice@example.com',   'UK'),
    ('Bob Jones',     'bob@example.com',     'US'),
    ('Charlie Brown', 'charlie@example.com', 'DE'),
    ('Diana Prince',  'diana@example.com',   'FR'),
    ('Eric Adams',    'eric@example.com',    'UK');
GO

INSERT INTO products (name, category, unit_price) VALUES
    ('Laptop Pro 15',      'Electronics', 1299.99),
    ('Wireless Headset',   'Electronics',   89.99),
    ('Office Chair',       'Furniture',    349.50),
    ('Python Crash Course','Books',         29.99),
    ('Standing Desk',      'Furniture',    599.00);
GO

INSERT INTO orders (customer_id, product_id, quantity, unit_price, status) VALUES
    (1, 1, 1, 1299.99, 'delivered'),
    (1, 2, 2,   89.99, 'shipped'),
    (2, 3, 1,  349.50, 'delivered'),
    (3, 4, 3,   29.99, 'delivered'),
    (4, 5, 1,  599.00, 'pending'),
    (5, 1, 1, 1299.99, 'cancelled'),
    (2, 4, 2,   29.99, 'delivered'),
    (3, 2, 1,   89.99, 'shipped'),
    (4, 1, 1, 1299.99, 'delivered'),
    (5, 3, 2,  349.50, 'pending');
GO
