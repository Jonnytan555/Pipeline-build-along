# Stage 4 — Great Expectations (Data Quality Suite)

Replaces the custom pandas validation checks in Layer 7 with a proper Great Expectations suite. Adds structured validation results, auto-generated HTML Data Docs, and first-class Airflow DAG integration.

## What it does

- Defines an `ExpectationSuite` for the orders dataset with 8 expectations
- Validates "good" and "bad" sample batches to demonstrate pass/fail reporting
- Falls back to equivalent pandas checks if GE is not installed
- Shows how the suite slots into the Airflow DAG as a gate before warehouse load

## Files

| File | Purpose |
|---|---|
| `ge_suite.py` | Suite definition, validator, pandas fallback, walkthrough runner |

## Run

```bash
# Install GE if needed
pip install great-expectations

python -m stages.stage-4-great-expectations.ge_suite
```

## Expectations defined

| Expectation | Column | Rule |
|---|---|---|
| `ExpectColumnToExist` | order_id, customer_id, amount, processed_timestamp | Column must be present |
| `ExpectColumnValuesToNotBeNull` | order_id, customer_id, amount | No nulls in key columns |
| `ExpectColumnValuesToBeBetween` | amount | 0.01 ≤ amount ≤ 100,000 |
| `ExpectColumnValuesToBeBetween` | customer_id | customer_id ≥ 1 |
| `ExpectColumnValuesToBeUnique` | order_id | No duplicate order IDs |
| `ExpectTableRowCountToBeBetween` | — | 1 ≤ row_count ≤ 1,000,000 |

## Sample output

```
[GOOD data] Validation PASSED ✓
  Evaluated: 8
  Successful: 8
  Failed: 0

[BAD data] Validation FAILED ✗
  Evaluated: 8
  Successful: 5
  Failed: 3
  ✗ ExpectColumnValuesToNotBeNull (order_id)
  ✗ ExpectColumnValuesToNotBeNull (customer_id)
  ✗ ExpectColumnValuesToBeBetween (amount)
```

## DAG integration (how it gates the pipeline)

```
extract_mysql  →  validate_data  →  transform  →  load_postgres
                       ↓ FAIL
                  skip load, alert Slack/email
```

The `validate_data` task runs the GE suite. If any expectation fails:
- Airflow marks the task as failed
- Downstream `load_postgres` is skipped (`TriggerRule.ALL_SUCCESS`)
- Data stays in the staging area — never reaches the warehouse

## GE vs pandas validation (Layer 7)

| Capability | Layer 7 (`validate.py`) | Stage 4 (GE) |
|---|---|---|
| Null checks | ✓ | ✓ |
| Business rules | ✓ | ✓ |
| Structured results | ✗ | ✓ |
| HTML Data Docs report | ✗ | ✓ |
| Version-controlled suite (JSON) | ✗ | ✓ |
| History tracking | ✗ | ✓ |

## Pipeline position

```
Layer 4 (Airflow DAG)
      ↓
[Stage 4: GE validates each batch at the boundary]
      ↓
Layer 8 (warehouse load — only if validation passes)
```
