"""SQLMesh project configuration — connects to PostgreSQL via env vars."""
import os
from sqlmesh.core.config import Config, ModelDefaultsConfig
from sqlmesh.core.config.connection import PostgresConnectionConfig

config = Config(
    connections={
        "dev": PostgresConnectionConfig(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "processed_db"),
            user=os.getenv("POSTGRES_USER", "pipeline"),
            password=os.getenv("POSTGRES_PASSWORD", "pipeline_pass"),
        )
    },
    default_connection="dev",
    model_defaults=ModelDefaultsConfig(dialect="postgres"),
)
