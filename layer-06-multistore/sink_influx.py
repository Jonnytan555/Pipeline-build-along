"""
LAYER 6c — InfluxDB Sink: Time-Series Sensor Metrics
=====================================================

What you will learn:
  - InfluxDB as a time-series database (TSDB)
  - When to use a TSDB vs a relational or document database:
    TSDBs are optimised for high-frequency writes keyed on time,
    with built-in downsampling, retention policies, and fast
    range queries like "average temp in the last hour"
  - The InfluxDB data model:
      measurement  → table name (e.g. "sensor_readings")
      tags         → indexed metadata (sensor_id, location)
      fields       → measured values (temperature, humidity)
      time         → timestamp (nanosecond precision)
  - Writing data points with the Python client library
  - Flux query language for aggregation

Pattern:
    Kafka (sensor_readings)
        → InfluxDB measurement (sensor_readings)
            → Grafana dashboard (Layer 10) visualises trends

Run:
    python layer-06-multistore/sink_influx.py
    # or as part of: make l6-run

InfluxDB UI:  http://localhost:8086
  user: admin  |  password: influx_pass
"""

import json
import logging
import os
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from kafka import KafkaConsumer

from utils.logger import setup_log

setup_log(app="layer-06-influx", use_stream=True)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",    "localhost:9092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC",     "sensor_readings")
INFLUX_URL     = os.getenv("INFLUXDB_URL",    "http://localhost:8086")
INFLUX_TOKEN   = os.getenv("INFLUXDB_TOKEN",  "pipeline-token")
INFLUX_ORG     = os.getenv("INFLUXDB_ORG",    "pipeline-org")
INFLUX_BUCKET  = os.getenv("INFLUXDB_BUCKET", "iot_data")
MAX_MESSAGES   = 500


# ── InfluxDB writer ───────────────────────────────────────────
def get_write_api():
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    return client.write_api(write_options=SYNCHRONOUS), client


def reading_to_point(reading: dict) -> Point:
    """
    Convert a sensor reading dict to an InfluxDB Point.

    Tags are indexed (low-cardinality): sensor_id, location.
    Fields are the actual measurements (high-cardinality floats).
    Time is parsed from the ISO-8601 string in the message.

    WHY separate tags and fields?
      InfluxDB indexes tags for fast filtering.  Using a high-
      cardinality value (e.g. a UUID) as a tag explodes the index
      and degrades write performance.  Keep tags few and fixed.
    """
    ts = datetime.fromisoformat(
        reading["timestamp"].replace("Z", "+00:00")
    )
    return (
        Point("sensor_readings")
        .tag("sensor_id", reading["sensor_id"])
        .tag("location",  reading["location"])
        .field("temperature", float(reading["temperature"]))
        .field("humidity",    float(reading["humidity"]))
        .time(ts, WritePrecision.S)
    )


# ── Consumer → InfluxDB ───────────────────────────────────────
def run_sink(max_messages: int = MAX_MESSAGES) -> int:
    write_api, _client = get_write_api()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers  = KAFKA_BROKER,
        group_id           = "influx_sink_group",
        auto_offset_reset  = "earliest",
        value_deserializer = lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms= 10_000,
    )

    points  = []
    total   = 0
    written = 0
    BATCH   = 100

    log.info("Consuming from topic '%s' ...", KAFKA_TOPIC)

    for msg in consumer:
        points.append(reading_to_point(msg.value))
        total += 1

        if len(points) >= BATCH:
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
            written += len(points)
            points = []
            log.info("Written %s / %s points ...", written, total)

        if total >= max_messages:
            break

    if points:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
        written += len(points)

    consumer.close()
    log.info("Total consumed: %s  |  Written to InfluxDB: %s", total, written)
    return written


def query_preview() -> None:
    """
    Run a Flux query for the average temperature per sensor
    over the last hour.

    Flux is InfluxDB's functional query language.  The pipe (|>)
    operator chains transformations — similar to SQL but designed
    for time-series operations like windowed aggregations.
    """
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()

    flux = f"""
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "sensor_readings")
      |> filter(fn: (r) => r._field == "temperature")
      |> group(columns: ["sensor_id"])
      |> mean()
      |> yield(name: "avg_temp_last_hour")
    """

    log.info("Average temperature per sensor (last hour):")
    tables = query_api.query(flux)
    for table in tables:
        for record in table.records:
            log.info("  %-14s  avg=%.2f°C", record.values.get('sensor_id', '?'), record.get_value())


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 60)
    log.info("LAYER 6c — InfluxDB Time-Series Sink")
    log.info("=" * 60)

    run_sink()
    query_preview()

    log.info("=" * 60)
    log.info("InfluxDB sink complete.")
    log.info("Explore in the UI: %s", INFLUX_URL)
    log.info("Next -> Layer 7: validate data quality in the warehouse.")
    log.info("=" * 60)
