# Layer 5 — Kafka Streaming

Simulates real-time IoT sensor data using Apache Kafka. A producer publishes JSON messages from five virtual sensors; a consumer reads and logs them. Demonstrates partitioning, consumer groups, and at-least-once delivery.

## What it does

- **Producer:** generates temperature + humidity readings for 5 sensors, one message per sensor per second
- **Consumer:** reads from the topic, deserialises JSON, and logs each reading with a running count

## Files

| File | Purpose |
|---|---|
| `producer.py` | IoT sensor simulator — publishes to `sensor_readings` |
| `consumer.py` | Consumer with JSON deserialisation and logging |
| `Dockerfile` | Shared image for both producer and consumer containers |

## Run

```bash
# Producer runs automatically in its container; tail the logs:
docker logs -f kafka-producer

# Start the consumer:
make l5-consume
# or: python layer-05-kafka/consumer.py
```

Start the full Kafka stack:

```bash
docker compose up -d zookeeper kafka kafka-producer
```

## Message format

```json
{
  "sensor_id": "sensor-03",
  "location": "Server Room",
  "temperature": 22.4,
  "humidity": 48.1,
  "timestamp": "2024-01-15T10:30:00"
}
```

## Topic config

| Setting | Value |
|---|---|
| Topic | `sensor_readings` |
| Broker | `localhost:9092` |
| Producer acks | `all` (strongest guarantee) |
| Consumer group | `consumer_group_01` |
| Auto offset reset | `latest` |

## Sensors

| ID | Location |
|---|---|
| sensor-01 | Warehouse A |
| sensor-02 | Warehouse B |
| sensor-03 | Office |
| sensor-04 | Server Room |
| sensor-05 | Warehouse A |

## Key concepts

- **Partitioning by key (`sensor_id`)** ensures all messages from the same sensor land in the same partition, preserving per-sensor order.
- **At-least-once delivery** — messages may be re-delivered after a failure; consumers must be idempotent.
- **Consumer groups** — multiple consumers in the same group share partitions (load balancing); different group IDs get their own independent stream (fan-out).

## Pipeline position

```
[Layer 5: Kafka]  →  Layer 6 (multi-store sinks: MongoDB, InfluxDB, Redis)
```
