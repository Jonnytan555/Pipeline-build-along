"""
LAYER 6b — MongoDB Sink: Storing IoT Sensor Readings
=====================================================

What you will learn:
  - MongoDB as a document store in a streaming pipeline
  - When to use MongoDB: semi-structured data, flexible schemas,
    nested documents, rapid prototyping
  - Reading from Kafka and writing every message to MongoDB
  - TTL indexes: MongoDB can auto-expire old documents (like Redis)
  - Why document stores work well for IoT / event data:
    each reading has the same core fields but could gain new
    sensor types without a schema migration

Pattern:
    Kafka topic (sensor_readings)
        → MongoDB collection (iot_data.sensor_readings)
            → Queryable for the last N readings per sensor,
              aggregations, anomaly detection

Run:
    python layer-06-multistore/sink_mongo.py
    # or as part of: make l6-run
"""

import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pymongo import MongoClient, ASCENDING

from utils.logger import setup_log

setup_log(app="layer-06-mongo", use_stream=True)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC",  "sensor_readings")
MONGO_URI    = os.getenv("MONGODB_URI",  "mongodb://localhost:27017/")
MONGO_DB     = os.getenv("MONGODB_DB",   "iot_data")
BATCH_SIZE   = 50


# ── MongoDB setup ─────────────────────────────────────────────
def get_collection():
    """
    Return the sensor_readings collection, creating a TTL index
    on first access.

    TTL index: MongoDB will automatically delete documents where
    'timestamp' is older than 7 days.  This keeps the collection
    from growing unboundedly — only the recent window matters for
    real-time dashboards.
    """
    client = MongoClient(MONGO_URI)
    db     = client[MONGO_DB]
    coll   = db["sensor_readings"]

    coll.create_index(
        [("timestamp", ASCENDING)],
        expireAfterSeconds = 7 * 24 * 60 * 60,
        name               = "ttl_7days",
    )
    coll.create_index([("sensor_id", ASCENDING)], name="idx_sensor_id")

    return coll


# ── Consumer → Mongo ──────────────────────────────────────────
def run_sink(max_messages: int = 500) -> None:
    """
    Consume messages from Kafka and batch-insert into MongoDB.

    insert_many() is significantly faster than individual insert_one()
    calls because it sends one round-trip per batch.
    """
    coll = get_collection()
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers  = KAFKA_BROKER,
        group_id           = "mongo_sink_group",
        auto_offset_reset  = "earliest",
        value_deserializer = lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms= 10_000,
    )

    batch   = []
    total   = 0
    written = 0

    log.info("Consuming from topic '%s' ...", KAFKA_TOPIC)

    for msg in consumer:
        doc = msg.value
        doc["timestamp"] = datetime.fromisoformat(
            doc["timestamp"].replace("Z", "+00:00")
        )
        batch.append(doc)
        total += 1

        if len(batch) >= BATCH_SIZE:
            coll.insert_many(batch)
            written += len(batch)
            batch = []
            log.info("Inserted %s / %s messages ...", written, total)

        if total >= max_messages:
            break

    if batch:
        coll.insert_many(batch)
        written += len(batch)

    consumer.close()
    log.info("Total consumed: %s  |  Inserted into MongoDB: %s", total, written)


def preview_mongo(n: int = 5) -> None:
    """Show the most recent readings and a count per sensor."""
    coll = get_collection()

    log.info("Latest %s readings in MongoDB:", n)
    for doc in coll.find().sort("timestamp", -1).limit(n):
        doc.pop("_id")
        log.info("  %s", doc)

    log.info("Readings per sensor:")
    pipeline = [
        {"$group": {"_id": "$sensor_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    for row in coll.aggregate(pipeline):
        log.info("  %-14s  %s readings", row['_id'], row['count'])


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 60)
    log.info("LAYER 6b — MongoDB Sink")
    log.info("=" * 60)

    run_sink(max_messages=500)
    preview_mongo()

    log.info("=" * 60)
    log.info("MongoDB sink complete.")
    log.info("Query the collection: %s%s.sensor_readings", MONGO_URI, MONGO_DB)
    log.info("=" * 60)
