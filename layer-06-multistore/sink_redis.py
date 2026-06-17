"""
LAYER 6a — Redis Sink: Caching Aggregated Metrics
==================================================

What you will learn:
  - Redis as a key-value cache in a data pipeline
  - When to use Redis: sub-millisecond reads, TTL-based expiry,
    counters, session state, leaderboards
  - The cache-aside pattern: populate the cache from the warehouse
    so the API can serve reads without hitting PostgreSQL
  - Key naming conventions: <namespace>:<entity>:<id>
  - TTL (time-to-live): cache entries expire automatically so
    stale data is never served after the next pipeline run

Pattern in this pipeline:
    PostgreSQL revenue_by_category
        → Redis cache (TTL = 24 h)
            → FastAPI layer reads cache first (Layer 9)
               falls back to Postgres on cache miss

Run:
    make l6-run   # or: python layer-06-multistore/sink_redis.py
"""

import logging
import os

import redis

from utils.db import Database
from utils.logger import setup_log

setup_log(app="layer-06-redis", use_stream=True)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

db = Database(
    name="source_db",
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", "sa"),
    password=os.getenv("SA_PASSWORD", "december1"),
)

CACHE_TTL_SECONDS = 24 * 60 * 60


# ── Redis helpers ─────────────────────────────────────────────
def get_redis() -> redis.Redis:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    return r


# ── Cache population ──────────────────────────────────────────
def cache_revenue_by_category(r: redis.Redis) -> int:
    """
    Read the pre-aggregated category revenue from PostgreSQL
    and store each row as a Redis hash.

    Key pattern:  revenue:category:<category_name>
    Hash fields:  order_count, total_revenue, avg_order_value, as_of_date

    WHY a hash (HSET) instead of a plain string?
      Hashes let you update individual fields without rewriting
      the entire value.  They also pack efficiently in memory.
    """
    df = db.query("""
        SELECT category, order_count, total_revenue,
               avg_order_value, as_of_date
        FROM   revenue_by_category
        ORDER  BY total_revenue DESC
    """)

    count = 0
    for _, row in df.iterrows():
        category, order_count, total_revenue, avg_order_value, as_of_date = (
            row["category"], row["order_count"], row["total_revenue"],
            row["avg_order_value"], row["as_of_date"],
        )
        key = f"revenue:category:{category.lower().replace(' ', '_')}"
        r.hset(key, mapping={
            "category":        category,
            "order_count":     order_count,
            "total_revenue":   str(total_revenue),
            "avg_order_value": str(avg_order_value),
            "as_of_date":      str(as_of_date),
        })
        r.expire(key, CACHE_TTL_SECONDS)
        count += 1
        log.info("Cached %s  (TTL=%ss)", key, CACHE_TTL_SECONDS)

    return count


def cache_revenue_by_customer(r: redis.Redis) -> int:
    """Cache per-customer revenue totals."""
    df = db.query("""
        SELECT customer_name, country, order_count,
               total_revenue, avg_order_value, as_of_date
        FROM   revenue_by_customer
        ORDER  BY total_revenue DESC
    """)

    count = 0
    for _, row in df.iterrows():
        name, country, order_count, total_revenue, avg, as_of_date = (
            row["customer_name"], row["country"], row["order_count"],
            row["total_revenue"], row["avg_order_value"], row["as_of_date"],
        )
        key = f"revenue:customer:{name.lower().replace(' ', '_')}"
        r.hset(key, mapping={
            "customer_name":   name,
            "country":         country,
            "order_count":     order_count,
            "total_revenue":   str(total_revenue),
            "avg_order_value": str(avg),
            "as_of_date":      str(as_of_date),
        })
        r.expire(key, CACHE_TTL_SECONDS)
        count += 1

    log.info("Cached %s customer revenue records", count)
    return count


def verify_cache(r: redis.Redis) -> None:
    """Read back a sample to confirm the round-trip worked."""
    keys = r.keys("revenue:category:*")
    log.info("Redis keys matching 'revenue:category:*': %s", len(keys))
    if keys:
        sample = r.hgetall(keys[0])
        log.info("Sample: %s -> %s", keys[0], sample)


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 60)
    log.info("LAYER 6a — Redis Cache Sink")
    log.info("=" * 60)

    r = get_redis()
    log.info("Connected to Redis at %s:%s", REDIS_HOST, REDIS_PORT)

    log.info("Populating category revenue cache ...")
    n = cache_revenue_by_category(r)
    log.info("-> %s categories cached", n)

    log.info("Populating customer revenue cache ...")
    cache_revenue_by_customer(r)

    verify_cache(r)

    log.info("=" * 60)
    log.info("Redis cache populated.")
    log.info("The FastAPI layer (Layer 9) will read from here first.")
    log.info("=" * 60)
