"""
Sample pytest-based data quality tests.

These demonstrate how to write tests directly in Python using the framework's
connectors and helpers.  For YAML-driven tests, use `etl-test check <config.yaml>`.

Usage:
    pytest tests/test_data_quality.py -v --tb=short
    pytest tests/test_data_quality.py -m snowflake   # only Snowflake tests
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest


# ═══════════════════════════════════════════════════════════
# Snowflake data quality tests
# ═══════════════════════════════════════════════════════════

@pytest.mark.snowflake
class TestRawLayerQuality:
    """Validate the raw/staging layer populated by Fivetran."""

    # --- Customise these to match your environment ----------
    DATABASE = "ANALYTICS"
    SCHEMA = "RAW_SALESFORCE"
    # --------------------------------------------------------

    def test_customers_not_empty(self, snowflake_conn):
        count = snowflake_conn.get_row_count(self.DATABASE, self.SCHEMA, "CUSTOMERS")
        assert count > 0, "CUSTOMERS table is empty"

    def test_orders_not_empty(self, snowflake_conn):
        count = snowflake_conn.get_row_count(self.DATABASE, self.SCHEMA, "ORDERS")
        assert count > 0, "ORDERS table is empty"

    def test_customers_no_null_ids(self, snowflake_conn):
        nulls = snowflake_conn.get_null_count(self.DATABASE, self.SCHEMA, "CUSTOMERS", "ID")
        assert nulls == 0, f"Found {nulls} NULL IDs in CUSTOMERS"

    def test_orders_freshness(self, snowflake_conn):
        latest = snowflake_conn.get_table_freshness(
            self.DATABASE, self.SCHEMA, "ORDERS", "_FIVETRAN_SYNCED"
        )
        assert latest is not None, "No _FIVETRAN_SYNCED data"
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600
        assert age_hours <= 24, f"Data is {age_hours:.1f}h old (max 24h)"

    def test_customers_schema(self, snowflake_conn):
        cols = snowflake_conn.get_columns(self.DATABASE, self.SCHEMA, "CUSTOMERS")
        col_names = {c["COLUMN_NAME"].upper() for c in cols}
        required = {"ID", "EMAIL", "CREATED_AT"}
        missing = required - col_names
        assert not missing, f"Missing columns: {missing}"


@pytest.mark.snowflake
class TestTransformedLayerQuality:
    """Validate the transformed layer produced by Coalesce."""

    DATABASE = "ANALYTICS"
    RAW_SCHEMA = "RAW_SALESFORCE"
    DWH_SCHEMA = "DWH"

    def test_dim_customer_row_count(self, snowflake_conn):
        cmp = snowflake_conn.compare_row_counts(
            self.DATABASE, self.RAW_SCHEMA, "CUSTOMERS",
            self.DATABASE, self.DWH_SCHEMA, "DIM_CUSTOMER",
        )
        tolerance = 0.05
        if cmp["source"] > 0:
            pct = abs(cmp["difference"]) / cmp["source"]
            assert pct <= tolerance, (
                f"Row count mismatch: source={cmp['source']} target={cmp['target']} "
                f"diff={cmp['difference']} ({pct:.1%})"
            )

    def test_dim_customer_unique_key(self, snowflake_conn):
        total = snowflake_conn.get_row_count(self.DATABASE, self.DWH_SCHEMA, "DIM_CUSTOMER")
        distinct = snowflake_conn.get_distinct_count(self.DATABASE, self.DWH_SCHEMA, "DIM_CUSTOMER", "CUSTOMER_KEY")
        assert total == distinct, f"CUSTOMER_KEY not unique: {total} rows, {distinct} distinct"

    def test_fact_orders_no_orphans(self, snowflake_conn):
        sql = """
            SELECT COUNT(*)
            FROM "{db}"."{dwh}"."FACT_ORDERS" f
            LEFT JOIN "{db}"."{dwh}"."DIM_CUSTOMER" d ON f."CUSTOMER_KEY" = d."CUSTOMER_KEY"
            WHERE d."CUSTOMER_KEY" IS NULL AND f."CUSTOMER_KEY" IS NOT NULL
        """.format(db=self.DATABASE, dwh=self.DWH_SCHEMA)
        orphans = int(snowflake_conn.execute_scalar(sql))
        assert orphans == 0, f"Found {orphans} orphan records in FACT_ORDERS"

    def test_order_amount_positive(self, snowflake_conn):
        sql = f"""
            SELECT COUNT(*)
            FROM "{self.DATABASE}"."{self.DWH_SCHEMA}"."FACT_ORDERS"
            WHERE "ORDER_AMOUNT" < 0
        """
        negatives = int(snowflake_conn.execute_scalar(sql))
        assert negatives == 0, f"Found {negatives} negative ORDER_AMOUNTs"


# ═══════════════════════════════════════════════════════════
# Fivetran connector status tests
# ═══════════════════════════════════════════════════════════

@pytest.mark.fivetran
class TestFivetranConnectors:
    """Validate Fivetran connector health."""

    # Put your connector IDs here
    CONNECTOR_IDS = [
        # "connector_id_1",
        # "connector_id_2",
    ]

    @pytest.mark.parametrize("connector_id", CONNECTOR_IDS)
    def test_connector_healthy(self, fivetran_conn, connector_id):
        assert fivetran_conn.is_sync_healthy(connector_id), (
            f"Connector {connector_id} is not healthy"
        )

    @pytest.mark.parametrize("connector_id", CONNECTOR_IDS)
    def test_connector_synced_recently(self, fivetran_conn, connector_id):
        last_sync = fivetran_conn.get_last_sync_time(connector_id)
        assert last_sync is not None, f"Connector {connector_id} has no successful sync"
        age_hours = (datetime.now(timezone.utc) - last_sync).total_seconds() / 3600
        assert age_hours <= 24, f"Last sync {age_hours:.1f}h ago"


# ═══════════════════════════════════════════════════════════
# Coalesce transformation tests
# ═══════════════════════════════════════════════════════════

@pytest.mark.coalesce
class TestCoalesceTransformations:
    """Validate Coalesce job runs."""

    def test_last_run_succeeded(self, coalesce_conn):
        last = coalesce_conn.get_last_successful_run()
        assert last is not None, "No successful Coalesce run found"
