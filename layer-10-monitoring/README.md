# Layer 10 — Monitoring (Prometheus + Grafana)

Collects metrics from the FastAPI layer and visualises them in Grafana. Provides real-time visibility into API health, request rates, latency distributions, and cache efficiency.

## What it does

- Prometheus scrapes `/metrics` from the FastAPI service every 15 seconds
- Grafana displays a pre-provisioned pipeline dashboard (no manual setup required)
- Prometheus also self-monitors (useful for alerting on collection failures)

## Files

| File | Purpose |
|---|---|
| `prometheus.yml` | Scrape configuration — jobs, intervals, targets |
| `grafana/provisioning/dashboards/pipeline.json` | Pre-built Grafana dashboard |
| `grafana/provisioning/datasources/prometheus.yml` | Auto-configures Prometheus as Grafana data source |

## Run

```bash
docker compose up -d prometheus grafana
```

| Service | URL |
|---|---|
| Prometheus | [http://localhost:9090](http://localhost:9090) |
| Grafana | [http://localhost:3000](http://localhost:3000) — `admin` / `admin` |

The dashboard loads automatically via provisioning — no login to Grafana required to configure it.

## Scrape config

| Job | Target | Interval |
|---|---|---|
| `pipeline_api` | FastAPI `/metrics` | 15 s |
| `prometheus` | Self | 15 s |

## Metrics tracked

| Metric | What it shows |
|---|---|
| `api_requests_total` | Request volume by endpoint and status code |
| `api_request_latency_seconds` | p50 / p95 / p99 latency histograms |
| `api_cache_hits_total` | How often Redis serves the response |
| `api_cache_misses_total` | How often the DB is hit instead |

## Pipeline position

```
Layer 9 (FastAPI exposes /metrics)
      ↓
[Layer 10: Prometheus scrapes → Grafana visualises]
```

In the Kubernetes stage (Stage 3), Prometheus is deployed via the kube-prometheus-stack Helm chart, and the `ServiceMonitor` in `kubernetes/service-monitor.yaml` tells Prometheus which pods to scrape automatically.
