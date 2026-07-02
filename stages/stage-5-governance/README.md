# Stage 5 — Data Governance (Lineage, Catalog, Ownership)

Tracks the full lineage of every dataset from source to warehouse, maintains a metadata catalogue with PII flags and SLA definitions, and demonstrates impact analysis (what breaks if I change this table?).

## What it does

- Defines a typed catalogue of all datasets (MySQL, Kafka, PostgreSQL, S3)
- Records each pipeline transformation as a `LineageEdge` linking source → target
- Performs upstream/downstream impact analysis
- Registers datasets in Apache Atlas (falls back to in-memory if Atlas is unreachable)

## Files

| File | Purpose |
|---|---|
| `lineage_tracker.py` | Data classes, catalogue, lineage graph, impact analysis, Atlas registration |

## Run

```bash
python -m stages.stage-5-governance.lineage_tracker
```

No external dependencies required for the walkthrough (Atlas registration is optional).

## Core data structures

```python
@dataclass
class Column:
    name: str
    data_type: str
    is_pii: bool          # flags columns containing personal data
    pii_type: str         # "email", "name", "phone"

@dataclass
class Dataset:
    name: str
    platform: str         # mysql, postgresql, kafka, s3, snowflake
    owner: str            # team that owns this dataset
    freshness_sla_hours: int
    columns: list[Column]
    tags: list[str]       # e.g. ["source", "pii", "transactional"]

@dataclass
class LineageEdge:
    source: Dataset
    target: Dataset
    process_name: str     # extract_mysql, transform, load_facts …
    records_in: int
    records_out: int
```

## Lineage graph (pipeline)

```
mysql.source_db.orders
    → [extract_mysql]
postgresql.warehouse_db.orders_transformed
    → [load_facts]
postgresql.warehouse_db.fact_orders
    → [refresh_aggregations]
postgresql.warehouse_db.agg_daily_orders
```

## Impact analysis

```python
# Which datasets are upstream of fact_orders?
upstream("fact_orders")
# → [orders_transformed, orders (MySQL)]

# Which datasets break if orders changes schema?
downstream("orders")
# → [orders_transformed, fact_orders, agg_daily_orders, ML model features]
```

## PII catalogue example

```
Dataset: orders (MySQL)
  Columns:
    order_id     INT       PII: No
    customer_id  INT       PII: No
    email        VARCHAR   PII: Yes  (type: email)
    full_name    VARCHAR   PII: Yes  (type: name)
    amount       DECIMAL   PII: No
```

## Why governance matters

| Without it | With it |
|---|---|
| "Where did this revenue number come from?" → 2 days to trace | Lineage graph answers in seconds |
| GDPR audit: "which tables have PII?" → nobody knows | `is_pii=True` flags in catalogue |
| Schema change breaks 3 downstream reports silently | Impact analysis shows all dependents |
| "Who owns this table?" → ask around | `owner` field in every `Dataset` |

## Governance tooling landscape

| Tool | Type | Notes |
|---|---|---|
| Apache Atlas | Open source | Original, Hadoop-era, complex setup |
| OpenMetadata | Open source | Modern, API-first, easy Docker |
| DataHub | Open source | LinkedIn, strong lineage, Kubernetes-native |
| Amundsen | Open source | Lyft, Elasticsearch + Neo4j |
| Alation / Collibra | Commercial | Enterprise, full governance suite |
| dbt docs | dbt-specific | Best option if warehouse is managed by dbt |

## Pipeline position

```
[Stage 5: Governance spans the entire pipeline]
  — Every dataset from Layer 1 (MSSQL) through Layer 11 (MLflow)
    is registered in the catalogue with lineage edges between them
```
