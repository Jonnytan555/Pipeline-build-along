"""
LAYER 11 — MLflow: Train and Track a Revenue Prediction Model
==============================================================

What you will learn:
  - MLflow experiment tracking: log parameters, metrics, artefacts
  - The MLflow model registry: register, version, and stage models
  - Why data engineers care about ML:
      → Features come from the warehouse you just built
      → The model is just another pipeline consumer
  - sklearn Pipeline: combine preprocessing + estimator so the
    model can be deployed without a separate transform step
  - How MLflow stores runs in a backend store (SQLite here,
    PostgreSQL / MSSQL in production)

Model:
    Predict total_amount (order value) from:
      - category (one-hot encoded)
      - country  (one-hot encoded)
      - quantity

This is intentionally simple — the goal is to demonstrate
the MLflow workflow, not to build the best model.

MLflow UI:  http://localhost:5001

Run:
    make l11-run
    # equivalent to: python layer-11-mlflow/train_model.py
"""

import logging
import os

import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from utils.db import Database
from utils.logger import setup_log

setup_log(app="layer-11-mlflow", use_stream=True)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
EXPERIMENT  = "order_value_prediction"
MODEL_NAME  = "order_value_model"

db = Database(
    name="source_db",
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", "sa"),
    password=os.getenv("SA_PASSWORD", "Pipeline_Pass_2024!"),
)


# ── Feature extraction ────────────────────────────────────────
def load_training_data() -> pd.DataFrame:
    """
    Pull feature columns from the SQL Server warehouse.

    The warehouse (orders_enriched) is the feature store for this
    model — the same table that powers the API and BI queries.
    This is deliberate: the model trains on exactly what the
    pipeline produces, so there is no train/serve skew.

    In production you would version this SQL as a dbt model so
    data scientists always know which warehouse columns the model
    was trained on.
    """
    df = db.query("""
        SELECT
            category,
            country,
            quantity,
            CAST(total_amount AS FLOAT) AS total_amount
        FROM orders_enriched
        WHERE total_amount IS NOT NULL
          AND quantity > 0
    """, cache=False)
    log.info("Loaded %s training rows from warehouse", len(df))
    return df


# ── Model pipeline ────────────────────────────────────────────
def build_pipeline() -> Pipeline:
    """
    Combine preprocessing and the estimator into a single sklearn
    Pipeline so the model artefact includes the feature transforms.

    WHY wrap everything in a Pipeline?
      → You can call model.predict(raw_df) without a separate
        preprocessing step — the transform is baked in.
      → MLflow logs the entire pipeline as one artefact, so
        the deployed model is self-contained.
      → No train/serve skew: the same preprocessing runs at
        training time and at prediction time.
    """
    categorical = ["category", "country"]
    numerical   = ["quantity"]

    preprocessor = ColumnTransformer(transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), categorical),
        ("num", StandardScaler(), numerical),
    ])

    return Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("regressor",    Ridge(alpha=1.0)),
    ])


# ── Training run ──────────────────────────────────────────────
def train_and_log(df: pd.DataFrame) -> None:
    """
    Run a hyperparameter sweep, logging each run to MLflow.

    Each iteration of the loop is one MLflow run — its own
    entry in the UI with its own params, metrics, and model.
    The best run can then be registered to the model registry
    and promoted through Staging → Production.
    """
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)

    X = df[["category", "country", "quantity"]]
    y = df["total_amount"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    for alpha in [0.01, 0.1, 1.0, 10.0]:
        with mlflow.start_run(run_name=f"ridge_alpha={alpha}"):

            # ── Log parameters ────────────────────────────────
            # Parameters are inputs to the run: hyperparameters,
            # feature lists, dataset size. They never change after
            # the run — they're the "recipe".
            mlflow.log_param("alpha",      alpha)
            mlflow.log_param("features",   ["category", "country", "quantity"])
            mlflow.log_param("train_rows", len(X_train))

            # ── Train ─────────────────────────────────────────
            pipeline = build_pipeline()
            pipeline.set_params(regressor__alpha=alpha)
            pipeline.fit(X_train, y_train)

            # ── Log metrics ───────────────────────────────────
            # Metrics are outputs: evaluation scores, losses.
            # You compare metrics across runs to pick the winner.
            preds = pipeline.predict(X_test)
            mae   = mean_absolute_error(y_test, preds)
            r2    = r2_score(y_test, preds)

            mlflow.log_metric("mae", round(mae, 4))
            mlflow.log_metric("r2",  round(r2,  4))

            # ── Log model ─────────────────────────────────────
            # Logs the entire sklearn Pipeline (preprocessor +
            # regressor) as a versioned artefact.
            # registered_model_name creates/updates the model in
            # the registry so you can promote it later.
            mlflow.sklearn.log_model(
                sk_model              = pipeline,
                artifact_path         = "model",
                registered_model_name = MODEL_NAME,
            )

            log.info("alpha=%-5s  MAE=£%.2f  R²=%.3f", alpha, mae, r2)


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 60)
    log.info("LAYER 11 — MLflow Model Training")
    log.info("=" * 60)

    log.info("[1] Loading training data from warehouse ...")
    df = load_training_data()
    log.info("  Columns: %s", list(df.columns))
    log.info("  Target range: £%.2f - £%.2f",
             df["total_amount"].min(), df["total_amount"].max())

    log.info("[2] Training models (Ridge regression, 4 alpha values) ...")
    train_and_log(df)

    log.info("=" * 60)
    log.info("Training complete.")
    log.info("View runs in the MLflow UI: %s", MLFLOW_URI)
    log.info("  Experiment: %s", EXPERIMENT)
    log.info("  Registered model: %s", MODEL_NAME)
    log.info("=" * 60)
