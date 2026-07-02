"""
Stage 5 — Data Governance: Lineage, Catalog, and Ownership
============================================================
CONCEPT: Governance answers the question every data team gets eventually:
  "Where did this number come from?"

Without governance:
  - A business analyst questions a revenue figure → it takes 2 days to trace it
  - GDPR audit: "which tables contain PII?" → nobody knows for sure
  - New team member: "what does this column mean?" → ask someone senior
  - Data breach: "who had access to customer data?" → impossible to reconstruct

With governance:
  - Lineage: orders.csv → MySQL → Kafka → Spark → fact_orders → agg_daily_orders
  - Catalog: every table has an owner, description, PII flag, freshness SLA
  - Access control: who can read/write each dataset; all access logged
  - Impact analysis: "if I change fact_orders, what downstream reports break?"

GOVERNANCE COMPONENTS IN THIS PROJECT
───────────────────────────────────────
  governance/atlas_stub.py  — Apache Atlas REST client (catalog + lineage)

This stage extends that stub into a proper lineage tracker and adds:
  - A dataset catalog (in-memory or PostgreSQL backed)
  - Pipeline run lineage registration (source → transform → target)
  - PII column tagging
  - Impact analysis (what downstream assets depend on this dataset?)
  - OpenMetadata-compatible JSON schema (used by modern data catalogs)

TOOLS IN THIS SPACE
────────────────────
  Apache Atlas     — the original; Hadoop-era, Java, complex to operate
  OpenMetadata     — modern, API-first, easy Docker deployment, great UI
  DataHub          — LinkedIn open-source, strong lineage, K8s-native
  Alation / Collibra — commercial, enterprise focus, $$$
  dbt docs         — if your warehouse is dbt-managed, this is built in
  Amundsen         — Lyft open-source, Elasticsearch + Neo4j

For a new team, OpenMetadata or DataHub are the pragmatic choices.
This script implements the core patterns that map to any of these tools.

Run:
  python stages/stage-5-governance/lineage_tracker.py

  (With Apache Atlas running):
  ATLAS_API_URL=http://localhost:21000/api/atlas/v2 python lineage_tracker.py
"""

import json
import logging
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Literal

import requests

log = logging.getLogger(__name__)

ATLAS_API_URL  = os.getenv("ATLAS_API_URL",  "http://atlas:21000/api/atlas/v2")
ATLAS_USER     = os.getenv("ATLAS_USERNAME", "admin")
ATLAS_PASSWORD = os.getenv("ATLAS_PASSWORD", "admin")


# ── Dataset catalog ───────────────────────────────────────────────────────────

@dataclass
class Column:
    name: str
    data_type: str
    description: str = ""
    is_pii: bool = False                     # Personal Identifiable Information
    pii_type: str = ""                       # e.g. "email", "name", "phone"
    nullable: bool = True


@dataclass
class Dataset:
    """Represents a table, file, Kafka topic, or any data asset."""
    name: str
    platform: Literal["postgresql", "snowflake", "kafka", "s3", "mysql"]
    schema: str = "public"
    database: str = ""
    owner: str = ""
    description: str = ""
    columns: list[Column] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    freshness_sla_hours: int = 24            # alert if not updated within N hours
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def qualified_name(self) -> str:
        """Unique identifier across all platforms."""
        return f"{self.platform}.{self.database}.{self.schema}.{self.name}"

    @property
    def has_pii(self) -> bool:
        return any(c.is_pii for c in self.columns)

    def pii_columns(self) -> list[Column]:
        return [c for c in self.columns if c.is_pii]


@dataclass
class LineageEdge:
    """One step in the data flow: source → process → target."""
    source: Dataset
    target: Dataset
    process_name: str
    process_type: Literal["ingestion", "transform", "aggregation", "export"]
    dag_id: str = ""
    run_id: str = ""
    records_in: int = 0
    records_out: int = 0
    executed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ── Pipeline catalog (this project's datasets) ────────────────────────────────

def build_pipeline_catalog() -> list[Dataset]:
    """
    Defines every dataset in the pipeline with owner, PII flags, and columns.
    In production this lives in a database or YAML files checked into git.
    """
    return [
        Dataset(
            name="orders",
            platform="mysql",
            database="source_db",
            owner="data-engineering",
            description="Raw order transactions from the e-commerce platform",
            freshness_sla_hours=1,
            columns=[
                Column("order_id",   "INT",  "Unique order identifier", is_pii=False),
                Column("customer_id","INT",  "Customer FK",            is_pii=False),
                Column("amount",     "DECIMAL(10,2)", "Order total"),
                Column("email",      "VARCHAR", "Customer email",       is_pii=True, pii_type="email"),
                Column("created_at", "TIMESTAMP"),
            ],
            tags=["source", "transactional"],
        ),
        Dataset(
            name="orders_transformed",
            platform="postgresql",
            database="processed_db",
            owner="data-engineering",
            description="Cleaned and enriched orders; customer_id validated against CRM",
            freshness_sla_hours=2,
            columns=[
                Column("order_id",            "INT",  nullable=False),
                Column("customer_id",         "INT",  nullable=False),
                Column("amount",              "DECIMAL(10,2)"),
                Column("processed_timestamp", "TIMESTAMP"),
            ],
            tags=["staging", "cleaned"],
        ),
        Dataset(
            name="fact_orders",
            platform="postgresql",
            database="processed_db",
            schema="public",
            owner="analytics",
            description="Star schema fact table for order analytics. Join to dim_customers for customer attributes.",
            freshness_sla_hours=4,
            columns=[
                Column("order_key",           "BIGINT", nullable=False),
                Column("order_id",            "INT",    nullable=False),
                Column("customer_key",        "INT",    "FK to dim_customers"),
                Column("date_key",            "INT",    "FK to dim_date"),
                Column("net_amount",          "DECIMAL(10,2)"),
                Column("processed_timestamp", "TIMESTAMP"),
            ],
            tags=["warehouse", "fact", "star-schema"],
        ),
        Dataset(
            name="agg_daily_orders",
            platform="postgresql",
            database="processed_db",
            owner="analytics",
            description="Pre-aggregated daily order summary. Refreshed hourly.",
            freshness_sla_hours=1,
            tags=["warehouse", "aggregate"],
        ),
    ]


# ── Lineage graph ─────────────────────────────────────────────────────────────

def build_pipeline_lineage(catalog: list[Dataset]) -> list[LineageEdge]:
    """
    Defines the full lineage graph for the pipeline.
    Each edge = one data movement or transformation step.

    Full lineage:
      MySQL.orders
         → [batch_ingestion_dag: extract_mysql]
      PostgreSQL.orders_transformed
         → [batch_ingestion_dag: transform]
      PostgreSQL.fact_orders
         → [warehouse_transform_dag: load_facts]
      Snowflake.FACT_ORDERS           (if SNOWFLAKE_ENABLED)
         → [warehouse_transform_dag: refresh_aggregations]
      PostgreSQL.agg_daily_orders
    """
    by_name = {d.name: d for d in catalog}
    return [
        LineageEdge(
            source=by_name["orders"],
            target=by_name["orders_transformed"],
            process_name="extract_mysql",
            process_type="ingestion",
            dag_id="batch_ingestion_dag",
        ),
        LineageEdge(
            source=by_name["orders_transformed"],
            target=by_name["fact_orders"],
            process_name="load_facts",
            process_type="transform",
            dag_id="warehouse_transform_dag",
        ),
        LineageEdge(
            source=by_name["fact_orders"],
            target=by_name["agg_daily_orders"],
            process_name="refresh_aggregations",
            process_type="aggregation",
            dag_id="warehouse_transform_dag",
        ),
    ]


# ── Impact analysis ───────────────────────────────────────────────────────────

def upstream(dataset: Dataset, edges: list[LineageEdge]) -> list[Dataset]:
    """All datasets that feed into this one (direct + transitive)."""
    result, queue = [], [dataset]
    seen = set()
    while queue:
        current = queue.pop()
        for edge in edges:
            if edge.target.name == current.name and edge.source.name not in seen:
                result.append(edge.source)
                queue.append(edge.source)
                seen.add(edge.source.name)
    return result


def downstream(dataset: Dataset, edges: list[LineageEdge]) -> list[Dataset]:
    """All datasets that depend on this one (direct + transitive)."""
    result, queue = [], [dataset]
    seen = set()
    while queue:
        current = queue.pop()
        for edge in edges:
            if edge.source.name == current.name and edge.target.name not in seen:
                result.append(edge.target)
                queue.append(edge.target)
                seen.add(edge.target.name)
    return result


# ── Atlas registration ────────────────────────────────────────────────────────

def register_dataset_in_atlas(dataset: Dataset) -> bool:
    """
    Register a dataset in Apache Atlas as an hive_table entity.
    (Atlas uses hive_table as a generic tabular dataset type.)

    In OpenMetadata / DataHub the API is different but the concept is the same:
    POST the entity JSON to the catalog API.
    """
    payload = {
        "entity": {
            "typeName": "hive_table",
            "attributes": {
                "qualifiedName": dataset.qualified_name,
                "name":          dataset.name,
                "owner":         dataset.owner,
                "description":   dataset.description,
                "comment":       json.dumps({"tags": dataset.tags, "sla_hours": dataset.freshness_sla_hours}),
            },
        }
    }
    try:
        resp = requests.post(
            f"{ATLAS_API_URL}/entity",
            auth=(ATLAS_USER, ATLAS_PASSWORD),
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=10,
        )
        if resp.status_code in (200, 201):
            log.info("Registered '%s' in Atlas.", dataset.qualified_name)
            return True
        log.warning("Atlas registration failed for '%s': %d %s",
                    dataset.name, resp.status_code, resp.text[:200])
        return False
    except requests.RequestException as e:
        log.warning("Atlas not reachable: %s", e)
        return False


# ── Walkthrough ───────────────────────────────────────────────────────────────

def run_walkthrough() -> None:
    print("\n── Stage 5: Data Governance Walkthrough ──")

    catalog = build_pipeline_catalog()
    edges   = build_pipeline_lineage(catalog)
    by_name = {d.name: d for d in catalog}

    print("\n1. DATASET CATALOG")
    print("─" * 50)
    for d in catalog:
        pii_flag = f"  ⚠  PII: {[c.name for c in d.pii_columns()]}" if d.has_pii else ""
        print(f"  {d.qualified_name:<55} owner={d.owner}  SLA={d.freshness_sla_hours}h{pii_flag}")

    print("\n2. LINEAGE GRAPH")
    print("─" * 50)
    for e in edges:
        print(f"  {e.source.name:<30} →[{e.process_name}]→  {e.target.name}")

    print("\n3. IMPACT ANALYSIS — if orders_transformed changes:")
    print("─" * 50)
    target = by_name["orders_transformed"]
    ups   = upstream(target, edges)
    downs = downstream(target, edges)
    print(f"  Upstream (feeds into it):   {[d.name for d in ups] or '(none)'}")
    print(f"  Downstream (depends on it): {[d.name for d in downs]}")
    print("  → Changing orders_transformed will break all downstream assets ↑")

    print("\n4. PII AUDIT")
    print("─" * 50)
    for d in catalog:
        if d.has_pii:
            for col in d.pii_columns():
                print(f"  {d.name}.{col.name}  [{col.pii_type}]  platform={d.platform}")
    print("  → These columns require masking/encryption in Snowflake and access controls in IAM")

    print("\n5. ATLAS REGISTRATION (attempt — needs Atlas running)")
    print("─" * 50)
    for d in catalog:
        ok = register_dataset_in_atlas(d)
        status = "✓" if ok else "✗ (Atlas not reachable — run with ATLAS_API_URL set)"
        print(f"  {status}  {d.qualified_name}")

    print("""
GOVERNANCE IN PRACTICE
───────────────────────
  On-board a new table:
    1. Add a Dataset entry here with columns, owner, and PII flags
    2. Add LineageEdge entries showing where it comes from and goes to
    3. Register in Atlas / OpenMetadata
    4. Set freshness SLA — monitoring checks this daily

  When a GDPR request arrives ("delete everything about customer 42"):
    1. Find all datasets with is_pii=True in catalog → those are your targets
    2. Trace lineage to find derived/aggregated datasets that also contain the data
    3. Delete from all of them — or prove the aggregated data is not re-identifiable

  This is why governance isn't optional for teams handling personal data.
""")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run_walkthrough()
