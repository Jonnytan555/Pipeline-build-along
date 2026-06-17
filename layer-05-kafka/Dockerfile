# ============================================================
# LAYER 5 — Kafka producer container
#
# A lightweight Python container that runs producer.py in a
# loop, publishing synthetic sensor readings to the Kafka
# topic every second.
#
# WHY a separate container for the producer?
#   In real systems, producers are deployed close to the data
#   source (edge devices, microservices).  Running them in
#   Docker here simulates that topology: the producer and the
#   broker are independent services connected by the network.
# ============================================================

FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python==2.0.2

COPY producer.py .

CMD ["python", "-u", "producer.py"]
