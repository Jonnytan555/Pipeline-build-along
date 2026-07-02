# Layer 9 — FastAPI REST Endpoint

Serves the warehouse data as a read-only REST API. Uses a cache-aside pattern backed by Redis and exposes a `/metrics` endpoint scraped by Prometheus.

## What it does

- Reads from SQL Server (star schema) and Redis (pre-computed aggregations)
- Returns paginated order data and revenue summaries
- Tracks request counts, latency, and cache hit/miss rates via Prometheus metrics
- Serves interactive API docs at `/docs`

## Files

| File | Purpose |
|---|---|
| `main.py` | FastAPI application — endpoints, models, middleware |

## Run

```bash
make l9-up
# or: docker compose up -d pipeline-api
```

Local dev (no Docker):

```bash
uvicorn layer-09-api.main:app --reload --port 8000
```

API is available at [http://localhost:8000](http://localhost:8000)
Swagger UI: [http://localhost:8000/docs](http://localhost:8000/docs)

## Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness probe (returns `{"status": "ok"}`) |
| `GET` | `/metrics` | Prometheus scrape endpoint |
| `GET` | `/orders` | Paginated orders (`?limit=50&offset=0`) |
| `GET` | `/orders/{order_id}` | Single order lookup |
| `GET` | `/revenue/category` | Revenue by category (Redis-cached) |
| `GET` | `/revenue/customer` | Revenue by customer |

## Caching

Revenue endpoints use cache-aside:
1. Check Redis for cached result
2. On miss: query SQL Server, write result to Redis (TTL 24 h), return
3. On hit: return cached value, increment `api_cache_hits_total`

## Prometheus metrics

| Metric | Type | Labels |
|---|---|---|
| `api_requests_total` | Counter | `endpoint`, `status_code` |
| `api_request_latency_seconds` | Histogram | `endpoint` |
| `api_cache_hits_total` | Counter | — |
| `api_cache_misses_total` | Counter | — |

## Config (from `.env` / Docker environment)

| Variable | Default |
|---|---|
| `DB_HOST` | `mssql` |
| `DB_PORT` | `1433` |
| `REDIS_HOST` | `redis` |
| `REDIS_PORT` | `6379` |

## Pipeline position

```
Layer 8 (star schema in SQL Server)
Layer 6 (Redis cache)
      ↓
[Layer 9: FastAPI]  →  external consumers / dashboards
                    →  Layer 10 (Prometheus scrapes /metrics)
```
