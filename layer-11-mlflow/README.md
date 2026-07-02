# Layer 11 — MLflow Model Registry

Trains an order-value prediction model and tracks every experiment run — parameters, metrics, and the trained model artefact — in MLflow. Demonstrates how the warehouse table acts as a feature store.

## What it does

- Reads `orders_enriched` from PostgreSQL as training features
- Runs a hyperparameter sweep over Ridge regression (4 alpha values)
- Logs each run to MLflow: params, metrics (`mae`, `r2`), and the full sklearn pipeline
- Registers the best model in the MLflow Model Registry as `order_value_model`

## Files

| File | Purpose |
|---|---|
| `train_model.py` | Feature engineering, hyperparameter sweep, MLflow logging |

## Run

```bash
make l11-run
# or: python layer-11-mlflow/train_model.py
```

Start MLflow server:

```bash
docker compose up -d mlflow
```

MLflow UI: [http://localhost:5001](http://localhost:5001)

## Model details

| Setting | Value |
|---|---|
| Target | `total_amount` (order value in £) |
| Algorithm | Ridge regression |
| Hyperparameter sweep | `alpha` ∈ {0.01, 0.1, 1.0, 10.0} |
| Experiment name | `order_value_prediction` |
| Registry name | `order_value_model` |

## Features

| Feature | Transformation |
|---|---|
| `category` | One-hot encoding |
| `country` | One-hot encoding |
| `quantity` | Standard scaling |

The sklearn `Pipeline` object (preprocessor + regressor) is logged as a single artefact — this ensures no train/serve skew when the model is deployed.

## MLflow concepts

| Concept | Description |
|---|---|
| **Run** | One training execution — params + metrics + artefacts |
| **Experiment** | Named group of runs |
| **Model Registry** | Versioned store of promoted models (`Staging` / `Production`) |
| **sklearn Pipeline** | Encapsulates preprocessing + model as one serialisable object |

## Pipeline position

```
Layer 8 (orders_enriched in PostgreSQL acts as feature store)
      ↓
[Layer 11: MLflow trains + registers model]
      ↓
Deployed model serves predictions via Layer 9 (FastAPI) or separate inference service
```
