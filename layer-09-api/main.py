"""
LAYER 9 — FastAPI: Serve Warehouse Data
========================================

What you will learn:
  - Building a read-only REST API on top of SQL Server warehouse tables
  - FastAPI: automatic OpenAPI docs, type validation, async support
  - The cache-aside pattern with Redis (Layer 6a)
  - Exposing Prometheus metrics from a Python service (Layer 10)
  - Connection pooling via SQLAlchemy (built into utils/db.py)
  - Pydantic response models: typed, self-documenting API contracts

Endpoints:
    GET /health                  → liveness check
    GET /orders                  → paginated orders list
    GET /orders/{order_id}       → single order lookup
    GET /revenue/category        → revenue by category (Redis-cached)
    GET /revenue/customer        → revenue by customer
    GET /metrics                 → Prometheus scrape endpoint (Layer 10)

Start locally (with containers up):
    make l9-up
    # equivalent to: docker compose up -d pipeline-api

Docs UI:
    http://localhost:8000/docs
"""

import os
import sys
import time
from contextlib import asynccontextmanager
from functools import wraps

import redis
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse
from prometheus_client import Counter, Histogram, generate_latest
from pydantic import BaseModel

sys.path.insert(0, "/app")
from utils.db import Database

# ── Connection ────────────────────────────────────────────────
# SQLAlchemy (used by Database) maintains its own connection pool
# internally — no need to manage one manually.  Each call to
# db.query() borrows a connection, runs the query, and returns it.
db = Database(
    name="source_db",
    host=os.getenv("DB_HOST", "mssql"),
    user=os.getenv("DB_USER", "sa"),
    password=os.getenv("SA_PASSWORD", "december1"),
)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

_redis: redis.Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _redis
    try:
        _redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                             decode_responses=True)
        _redis.ping()
    except Exception:
        _redis = None   # Redis is optional — fall back to SQL Server

    yield   # <-- application runs here


app = FastAPI(
    title       = "Pipeline Warehouse API",
    description = "Read-only REST API for the data engineering pipeline warehouse.",
    version     = "1.0.0",
    lifespan    = lifespan,
)


# ── Prometheus metrics ────────────────────────────────────────
REQUEST_COUNT = Counter(
    "api_requests_total", "Total HTTP requests",
    labelnames=["method", "endpoint", "status"],
)
REQUEST_LATENCY = Histogram(
    "api_request_latency_seconds", "HTTP request latency",
    labelnames=["endpoint"],
)
CACHE_HITS   = Counter("api_cache_hits_total",   "Redis cache hits")
CACHE_MISSES = Counter("api_cache_misses_total",  "Redis cache misses")


def track(endpoint: str):
    """Decorator factory for recording request metrics."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                REQUEST_COUNT.labels("GET", endpoint, "200").inc()
                return result
            except HTTPException as exc:
                REQUEST_COUNT.labels("GET", endpoint, str(exc.status_code)).inc()
                raise
            finally:
                REQUEST_LATENCY.labels(endpoint).observe(time.perf_counter() - start)
        return wrapper
    return decorator


# ── Response models ───────────────────────────────────────────
class Order(BaseModel):
    order_id:       int
    customer_name:  str
    country:        str
    product_name:   str
    category:       str
    quantity:       int
    total_amount:   float
    status:         str


class RevenueByCategoryRow(BaseModel):
    category:       str
    order_count:    int
    total_revenue:  float
    avg_order_value:float


class RevenueByCustomerRow(BaseModel):
    customer_name:  str
    country:        str
    order_count:    int
    total_revenue:  float
    avg_order_value:float


# ── DB helper ─────────────────────────────────────────────────
def sql_query(sql: str) -> list[dict]:
    """Run a SELECT and return rows as a list of dicts."""
    df = db.query(sql, cache=False)
    return df.to_dict(orient="records")


# ── Routes ────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    """Prometheus scrape endpoint — consumed by Layer 10."""
    return generate_latest()


@app.get("/orders", response_model=list[Order])
@track("/orders")
async def list_orders(
    limit:  int = Query(20, ge=1, le=200),
    offset: int = Query(0,  ge=0),
):
    """List orders from the warehouse with pagination.
    T-SQL uses OFFSET/FETCH instead of LIMIT/OFFSET.
    """
    rows = sql_query(f"""
        SELECT order_id, customer_name, country, product_name,
               category, quantity, total_amount, status
        FROM   orders_enriched
        ORDER  BY order_id
        OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
    """)
    return [Order(**r) for r in rows]


@app.get("/orders/{order_id}", response_model=Order)
@track("/orders/{order_id}")
async def get_order(order_id: int):
    rows = sql_query(f"""
        SELECT order_id, customer_name, country, product_name,
               category, quantity, total_amount, status
        FROM   orders_enriched
        WHERE  order_id = {order_id}
    """)
    if not rows:
        raise HTTPException(status_code=404, detail="Order not found")
    return Order(**rows[0])


@app.get("/revenue/category", response_model=list[RevenueByCategoryRow])
@track("/revenue/category")
async def revenue_by_category():
    """
    Revenue by product category.
    Served from Redis cache if available, falls back to SQL Server.
    """
    if _redis:
        keys = _redis.keys("revenue:category:*")
        if keys:
            CACHE_HITS.inc()
            rows = []
            for k in keys:
                h = _redis.hgetall(k)
                rows.append(RevenueByCategoryRow(
                    category        = h["category"],
                    order_count     = int(h["order_count"]),
                    total_revenue   = float(h["total_revenue"]),
                    avg_order_value = float(h["avg_order_value"]),
                ))
            return sorted(rows, key=lambda r: r.total_revenue, reverse=True)

    CACHE_MISSES.inc()
    rows = sql_query("""
        SELECT category, order_count, total_revenue, avg_order_value
        FROM   revenue_by_category
        ORDER  BY total_revenue DESC
    """)
    return [RevenueByCategoryRow(**r) for r in rows]


@app.get("/revenue/customer", response_model=list[RevenueByCustomerRow])
@track("/revenue/customer")
async def revenue_by_customer():
    rows = sql_query("""
        SELECT customer_name, country, order_count,
               total_revenue, avg_order_value
        FROM   revenue_by_customer
        ORDER  BY total_revenue DESC
    """)
    return [RevenueByCustomerRow(**r) for r in rows]


# ── Entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
