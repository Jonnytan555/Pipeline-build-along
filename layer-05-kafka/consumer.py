"""
LAYER 5 — Kafka Consumer: Read Sensor Readings
===============================================

What you will learn:
  - How a consumer subscribes to a topic and poll for messages
  - Consumer groups: multiple consumers sharing partition workload
  - auto_offset_reset: "earliest" vs "latest" (where to start reading)
  - Deserialising JSON bytes back to Python dicts
  - The at-least-once delivery guarantee and why idempotent sinks matter

This consumer logs readings to the terminal.  Layer 6 builds
on this pattern, routing the same messages to Redis, MongoDB, and
InfluxDB as permanent storage.

Run (containers must be up):
    make l5-consume
    # equivalent to: python layer-05-kafka/consumer.py

Ctrl+C to stop.

Group ID:
    consumer_group_01 — changing this resets your read position.
    Two processes with different group IDs both see every message
    (fan-out).  Two processes in the SAME group split the partitions
    (load balancing).
"""

import json
import logging
import os
import signal
import sys

from kafka import KafkaConsumer

from utils.logger import setup_log

setup_log(app="layer-05-consumer", use_stream=True)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC    = os.getenv("KAFKA_TOPIC",  "sensor_readings")
GROUP_ID = "consumer_group_01"


# ── Consumer ──────────────────────────────────────────────────
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers   = BROKER,
    group_id            = GROUP_ID,
    auto_offset_reset   = "latest",
    enable_auto_commit  = True,
    value_deserializer  = lambda b: json.loads(b.decode("utf-8")),
    key_deserializer    = lambda b: b.decode("utf-8") if b else None,
    consumer_timeout_ms = -1,
)


def handle_sigint(sig, frame):
    log.info("Stopping consumer ...")
    consumer.close()
    sys.exit(0)


signal.signal(signal.SIGINT, handle_sigint)


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Listening on topic '%s' (group: %s) ...", TOPIC, GROUP_ID)
    log.info("Press Ctrl+C to stop.")

    count = 0
    for msg in consumer:
        r = msg.value
        count += 1
        log.info(
            "[%5d]  %-12s  %-14s  temp=%5.1f°C  hum=%5.1f%%  %s",
            count, r['sensor_id'], r['location'],
            r['temperature'], r['humidity'], r['timestamp'],
        )
