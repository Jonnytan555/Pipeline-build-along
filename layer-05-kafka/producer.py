"""
LAYER 5 — Kafka Producer: IoT Sensor Readings
==============================================

What you will learn:
  - The producer / broker / consumer model in Kafka
  - Serialising Python dicts to JSON bytes for Kafka messages
  - Partitioning: how message keys route data to the same partition
  - Why a producer should retry on transient broker failures
  - The difference between batch (Layer 2-4) and streaming (Layer 5+)

This producer simulates a fleet of IoT temperature sensors.
Each sensor publishes one reading per second to the Kafka topic
"sensor_readings".  The consumer (consumer.py) and the multi-store
sinks (Layer 6) read from the same topic.

Message format (JSON):
    {
        "sensor_id":   "sensor-03",
        "location":    "Warehouse B",
        "temperature": 21.7,
        "humidity":    58.2,
        "timestamp":   "2024-01-15T10:30:00Z"
    }

Run:
    Automatically starts when the kafka-producer container boots.
    To see output: docker logs -f kafka-producer

Local consumer to verify:
    make l5-consume
    # runs: python layer-05-kafka/consumer.py
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC  = os.getenv("KAFKA_TOPIC",  "sensor_readings")

SENSORS = [
    {"id": "sensor-01", "location": "Warehouse A"},
    {"id": "sensor-02", "location": "Warehouse A"},
    {"id": "sensor-03", "location": "Warehouse B"},
    {"id": "sensor-04", "location": "Office"},
    {"id": "sensor-05", "location": "Server Room"},
]


# ── Producer setup ────────────────────────────────────────────
def connect(retries: int = 10, delay: int = 5) -> KafkaProducer:
    """
    Wait for the Kafka broker to be ready before starting.

    Kafka takes a few seconds to start after the container is
    healthy.  Retrying here is safer than using depends_on with
    a health check — brokers report healthy before they accept
    producer connections.
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers  = BROKER,
                value_serializer   = lambda v: json.dumps(v).encode("utf-8"),
                # key_serializer routes all messages from the same sensor
                # to the same partition, preserving per-sensor ordering.
                key_serializer     = lambda k: k.encode("utf-8"),
                retries            = 3,
                acks               = "all",
            )
            log.info("Connected to Kafka broker at %s", BROKER)
            return producer
        except NoBrokersAvailable:
            log.warning("Broker not ready (attempt %s/%s), retrying in %ss ...", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka broker at {BROKER}")


# ── Message generation ────────────────────────────────────────
def make_reading(sensor: dict) -> dict:
    """Generate a realistic sensor reading with slight noise."""
    return {
        "sensor_id":   sensor["id"],
        "location":    sensor["location"],
        "temperature": round(random.gauss(20.0, 3.0), 2),
        "humidity":    round(random.gauss(55.0, 10.0), 2),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    }


# ── Main loop ─────────────────────────────────────────────────
if __name__ == "__main__":
    producer = connect()

    log.info("Publishing to topic '%s' — one reading per sensor per second ...", TOPIC)
    count = 0

    while True:
        for sensor in SENSORS:
            reading = make_reading(sensor)
            producer.send(TOPIC, key=sensor["id"], value=reading)

        producer.flush()
        count += len(SENSORS)

        if count % 50 == 0:
            log.info("Published %s messages total", count)

        time.sleep(1)
