# ETL Data Testing Framework

Automation framework for testing ETL pipelines across **Fivetran → Coalesce → Snowflake → Power BI**.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Fivetran   │────▶│  Snowflake   │────▶│  Coalesce    │────▶│  Power BI   │
│  (Ingest)    │     │  (Raw/Stage) │     │  (Transform) │     │  (Report)   │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘     └──────┬──────┘
       │                    │                    │                    │
       ▼                    ▼                    ▼                    ▼
  Sync health          Data quality         Job success          Refresh status
  Freshness            Schema checks        Row counts           Dataset health
  Connector status     Null/unique/dup      Referential integrity
```

## Features

| Category | Checks |
|---|---|
| **Row counts** | Min/max thresholds, source-to-target comparison with tolerance |
| **Schema** | Column names, data types, nullability |
| **Null checks** | Detect NULLs in critical columns |
| **Uniqueness** | Verify primary/natural keys are unique |
| **Duplicates** | Find duplicate rows across column sets |
| **Freshness** | Max data age based on timestamp columns |
| **Accepted values** | Validate column values against an allowed list |
| **Value range** | Min/max numeric bounds |
| **Referential integrity** | Orphan record detection across tables |
| **Custom SQL** | Any query – passes when it returns 0 rows |
| **Fivetran health** | Connector sync state, last sync time |
| **Coalesce jobs** | Trigger & wait for transformation runs |
| **Power BI** | Dataset refresh trigger & freshness check |

## Quick Start

### 1. Install

```bash
# Clone and install in editable mode
git clone <repo-url> && cd etl-data-testing
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# For Power BI support
pip install -e ".[dev,powerbi]"
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your credentials
```

### 3. Verify connectivity

```bash
etl-test ping --all
```

### 4. Run data quality checks

```bash
# From a YAML config
etl-test check tests/configs/sample_tests.yaml

# Filter by tag
etl-test check tests/configs/sample_tests.yaml --tags freshness,row_count
```

### 5. Run the full pipeline

```bash
etl-test pipeline tests/configs/sample_tests.yaml \
  --fivetran-connectors conn_id_1,conn_id_2 \
  --coalesce-env your_env_id \
  --powerbi-dataset your_dataset_id
```

### 6. Run with pytest

```bash
# All tests
pytest tests/ -v

# Only unit tests (no live connections needed)
pytest tests/test_check_runner.py tests/test_models.py -v

# Only Snowflake integration tests
pytest tests/test_data_quality.py -m snowflake -v
```

## Project Structure

```
etl-data-testing/
├── src/etl_testing/
│   ├── __init__.py
│   ├── config.py                  # Settings from .env / environment
│   ├── cli.py                     # Click CLI (etl-test command)
│   ├── pipeline.py                # End-to-end orchestrator
│   ├── reporting.py               # Console, JSON, HTML reports
│   ├── connectors/
│   │   ├── snowflake_connector.py # Snowflake queries & metadata
│   │   ├── fivetran_connector.py  # Fivetran REST API wrapper
│   │   ├── coalesce_connector.py  # Coalesce REST API wrapper
│   │   └── powerbi_connector.py   # Power BI REST API (MSAL auth)
│   └── checks/
│       ├── models.py              # Pydantic models & YAML loader
│       └── runner.py              # Check execution engine
├── tests/
│   ├── conftest.py                # Shared fixtures
│   ├── configs/
│   │   └── sample_tests.yaml      # Example YAML test config
│   ├── test_check_runner.py       # Unit tests (mocked)
│   ├── test_models.py             # Config parsing tests
│   └── test_data_quality.py       # Integration test examples
├── .env.example
├── .gitignore
├── pyproject.toml
└── README.md
```

## Writing Test Configs (YAML)

Create a YAML file with one or more test suites:

```yaml
version: "1"
suites:
  - name: "My Checks"
    tags: [nightly]
    checks:
      - name: "orders_not_empty"
        type: row_count
        severity: error
        target:
          database: ANALYTICS
          schema: DWH
          table: FACT_ORDERS
        min_rows: 1

      - name: "no_future_dates"
        type: custom_sql
        severity: error
        sql: |
          SELECT * FROM "ANALYTICS"."DWH"."FACT_ORDERS"
          WHERE ORDER_DATE > CURRENT_DATE()
```

### Available Check Types

| Type | Required fields |
|---|---|
| `row_count` | `target`, `min_rows` and/or `max_rows` |
| `row_count_compare` | `source`, `target`, `tolerance_pct` |
| `schema` | `target`, `expected_columns` |
| `null_check` | `target`, `columns` |
| `unique_check` | `target`, `columns` |
| `duplicate_check` | `target`, `columns` |
| `freshness` | `target`, `timestamp_column`, `max_age_hours` |
| `accepted_values` | `target`, `column`, `accepted_values` |
| `value_range` | `target`, `column`, `min_value`/`max_value` |
| `referential_integrity` | `target`, `child_key`, `parent`, `parent_key` |
| `custom_sql` | `sql` (pass = 0 rows returned) |

## CLI Reference

```
etl-test --help              # Show all commands
etl-test check --help        # Data quality checks
etl-test pipeline --help     # Full pipeline run
etl-test ping --help         # Connectivity test
```

## Reports

Each run generates:
- **Console output** – Rich-formatted tables with pass/fail status
- **JSON report** – Machine-readable, in `reports/` directory
- **HTML report** – Shareable dashboard-style report

## CI/CD Integration

```yaml
# GitHub Actions example
- name: Run ETL tests
  env:
    SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
    SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
    SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
    # ... other secrets
  run: |
    pip install -e .
    etl-test check tests/configs/production_tests.yaml
```

## License

MIT