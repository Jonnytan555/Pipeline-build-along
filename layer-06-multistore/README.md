# Layer 6 — Multi-Store Sinks

Routes processed data to three specialised storage systems: Redis for low-latency caching, MongoDB for semi-structured IoT documents, and InfluxDB for time-series analytics. Each sink solves a different query pattern that a general-purpose relational database handles poorly.

## What it does

| Sink | Source | Use case |
|---|---|---|
| Redis | PostgreSQL aggregations | Sub-millisecond cache for the FastAPI layer |
| MongoDB | Kafka `sensor_readings` | Flexible document storage with auto-expiry |
| InfluxDB | Kafka `sensor_readings` | Time-series aggregations (avg temp per hour) |

## Files

| File | Purpose |
|---|---|
| `run_all_sinks.py` | Orchestrator — runs all three sinks in sequence |
| `sink_redis.py` | Cache-aside pattern for warehouse metrics |
| `sink_mongo.py` | Kafka consumer → MongoDB batch insert |
| `sink_influx.py` | Kafka consumer → InfluxDB line protocol writer |

## Run

```bash
make l6-run
# or: python layer-06-multistore/run_all_sinks.py
```

Start the required containers:

```bash
docker compose up -d redis mongo influxdb kafka
```

## Sink details

### Redis (`sink_redis.py`)
- Reads `revenue_by_category` and `revenue_by_customer` from PostgreSQL
- Stores as Redis hashes — key scheme: `revenue:category:{name}`, `revenue:customer:{name}`
- TTL: 24 hours (auto-expires stale cache entries)
- **Config:** `REDIS_HOST=localhost`, `REDIS_PORT=6379`

### MongoDB (`sink_mongo.py`)
- Consumes from Kafka topic `sensor_readings`
- Batch inserts of 50 messages at a time via `insert_many()`
- TTL index auto-deletes documents older than 7 days
- **Database:** `iot_data` / **Collection:** `sensor_readings`
- **Config:** `MONGODB_URI=mongodb://localhost:27017/`

### InfluxDB (`sink_influx.py`)
- Consumes from Kafka topic `sensor_readings`
- Writes 100-point batches using the line protocol
- Tags (indexed): `sensor_id`, `location`
- Fields (measured values): `temperature`, `humidity`
- Example Flux query: average temperature per sensor over last hour
- **Config:** `INFLUXDB_URL=http://localhost:8086`, token in `.env`

## Pipeline position

```
Layer 5 (Kafka)       →  [Layer 6: MongoDB, InfluxDB]
Layer 3/4 (warehouse) →  [Layer 6: Redis]
                                  ↓
                          Layer 9 (FastAPI reads Redis cache)
```
