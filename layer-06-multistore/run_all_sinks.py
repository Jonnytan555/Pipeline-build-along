"""
LAYER 6 — Run All Multi-Store Sinks
====================================

Convenience runner that executes all three sinks in sequence:
  1. Redis  — cache warehouse aggregations (sink_redis.py)
  2. MongoDB — persist Kafka sensor readings as documents (sink_mongo.py)
  3. InfluxDB — write Kafka sensor readings as time-series (sink_influx.py)

WHY three different databases?
  Each serves a distinct query pattern:
    Redis    → millisecond key lookups, cache invalidation
    MongoDB  → flexible document queries, aggregation pipelines
    InfluxDB → time-windowed aggregations, high-frequency writes

In production you might deploy each sink as a separate
microservice or Flink/Spark Streaming job so they scale
independently.  Here we run them sequentially for simplicity.

Run:
    make l6-run
    # equivalent to: python layer-06-multistore/run_all_sinks.py

Prerequisites:
    make l6-up   ← starts Redis, MongoDB, InfluxDB, Kafka
    make l3-run  ← populates PostgreSQL warehouse (for Redis sink)
    make l5-up   ← starts Kafka producer (for MongoDB + InfluxDB sinks)
"""

import logging
import subprocess
import sys
from pathlib import Path

from utils.logger import setup_log

setup_log(app="layer-06-run-all", use_stream=True)
log = logging.getLogger(__name__)

BASE = Path(__file__).parent


def run(script: str) -> None:
    log.info("=" * 60)
    log.info("Running %s ...", script)
    log.info("=" * 60)
    result = subprocess.run(
        [sys.executable, str(BASE / script)],
        check=False,
    )
    if result.returncode != 0:
        log.warning("%s exited with code %s", script, result.returncode)


if __name__ == "__main__":
    run("sink_redis.py")
    run("sink_mongo.py")
    run("sink_influx.py")

    log.info("=" * 60)
    log.info("All Layer 6 sinks complete.")
    log.info("Next -> Layer 7: run data quality checks.")
    log.info("=" * 60)
