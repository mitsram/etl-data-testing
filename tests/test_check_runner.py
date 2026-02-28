"""
Unit tests for the check runner (mock Snowflake calls).

Run with:  pytest tests/test_check_runner.py -v
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from etl_testing.checks.models import CheckDefinition, CheckType, Severity, TableRef
from etl_testing.checks.runner import CheckRunner


@pytest.fixture
def mock_sf():
    return MagicMock()


@pytest.fixture
def runner(mock_sf):
    r = CheckRunner()
    r.sf = mock_sf
    return r


def _make_target() -> TableRef:
    return TableRef(database="DB", schema="SCH", table="TBL")


class TestRowCountCheck:
    def test_passes_within_range(self, runner, mock_sf):
        mock_sf.get_row_count.return_value = 100
        chk = CheckDefinition(
            name="test", type=CheckType.ROW_COUNT, target=_make_target(),
            min_rows=50, max_rows=200,
        )
        result = runner.run_checks([chk])[0]
        assert result.passed

    def test_fails_below_min(self, runner, mock_sf):
        mock_sf.get_row_count.return_value = 0
        chk = CheckDefinition(
            name="test", type=CheckType.ROW_COUNT, target=_make_target(), min_rows=1,
        )
        result = runner.run_checks([chk])[0]
        assert not result.passed

    def test_fails_above_max(self, runner, mock_sf):
        mock_sf.get_row_count.return_value = 500
        chk = CheckDefinition(
            name="test", type=CheckType.ROW_COUNT, target=_make_target(), max_rows=100,
        )
        result = runner.run_checks([chk])[0]
        assert not result.passed


class TestNullCheck:
    def test_passes_no_nulls(self, runner, mock_sf):
        mock_sf.get_null_count.return_value = 0
        chk = CheckDefinition(
            name="test", type=CheckType.NULL_CHECK, target=_make_target(), columns=["ID"],
        )
        result = runner.run_checks([chk])[0]
        assert result.passed

    def test_fails_with_nulls(self, runner, mock_sf):
        mock_sf.get_null_count.return_value = 5
        chk = CheckDefinition(
            name="test", type=CheckType.NULL_CHECK, target=_make_target(), columns=["ID"],
        )
        result = runner.run_checks([chk])[0]
        assert not result.passed


class TestFreshnessCheck:
    def test_passes_fresh_data(self, runner, mock_sf):
        mock_sf.get_table_freshness.return_value = datetime.now(timezone.utc) - timedelta(hours=1)
        chk = CheckDefinition(
            name="test", type=CheckType.FRESHNESS, target=_make_target(),
            timestamp_column="UPDATED_AT", max_age_hours=24,
        )
        result = runner.run_checks([chk])[0]
        assert result.passed

    def test_fails_stale_data(self, runner, mock_sf):
        mock_sf.get_table_freshness.return_value = datetime.now(timezone.utc) - timedelta(hours=48)
        chk = CheckDefinition(
            name="test", type=CheckType.FRESHNESS, target=_make_target(),
            timestamp_column="UPDATED_AT", max_age_hours=24,
        )
        result = runner.run_checks([chk])[0]
        assert not result.passed


class TestUniqueCheck:
    def test_passes_all_unique(self, runner, mock_sf):
        mock_sf.get_row_count.return_value = 100
        mock_sf.get_distinct_count.return_value = 100
        chk = CheckDefinition(
            name="test", type=CheckType.UNIQUE_CHECK, target=_make_target(), columns=["ID"],
        )
        result = runner.run_checks([chk])[0]
        assert result.passed

    def test_fails_with_duplicates(self, runner, mock_sf):
        mock_sf.get_row_count.return_value = 100
        mock_sf.get_distinct_count.return_value = 95
        chk = CheckDefinition(
            name="test", type=CheckType.UNIQUE_CHECK, target=_make_target(), columns=["ID"],
        )
        result = runner.run_checks([chk])[0]
        assert not result.passed


class TestCustomSqlCheck:
    def test_passes_zero_rows(self, runner, mock_sf):
        mock_sf.execute.return_value = []
        chk = CheckDefinition(
            name="test", type=CheckType.CUSTOM_SQL, sql="SELECT 1 WHERE 1=0",
        )
        result = runner.run_checks([chk])[0]
        assert result.passed

    def test_fails_with_rows(self, runner, mock_sf):
        mock_sf.execute.return_value = [{"col": 1}]
        chk = CheckDefinition(
            name="test", type=CheckType.CUSTOM_SQL, sql="SELECT 1",
        )
        result = runner.run_checks([chk])[0]
        assert not result.passed


class TestRowCountCompare:
    def test_passes_within_tolerance(self, runner, mock_sf):
        mock_sf.compare_row_counts.return_value = {"source": 100, "target": 103, "difference": 3}
        chk = CheckDefinition(
            name="test", type=CheckType.ROW_COUNT_COMPARE,
            source=_make_target(), target=_make_target(), tolerance_pct=5.0,
        )
        result = runner.run_checks([chk])[0]
        assert result.passed

    def test_fails_outside_tolerance(self, runner, mock_sf):
        mock_sf.compare_row_counts.return_value = {"source": 100, "target": 120, "difference": 20}
        chk = CheckDefinition(
            name="test", type=CheckType.ROW_COUNT_COMPARE,
            source=_make_target(), target=_make_target(), tolerance_pct=5.0,
        )
        result = runner.run_checks([chk])[0]
        assert not result.passed


class TestAcceptedValues:
    def test_passes_all_valid(self, runner, mock_sf):
        mock_sf.execute.return_value = [{"STATUS": "active"}, {"STATUS": "inactive"}]
        chk = CheckDefinition(
            name="test", type=CheckType.ACCEPTED_VALUES, target=_make_target(),
            column="STATUS", accepted_values=["active", "inactive", "pending"],
        )
        result = runner.run_checks([chk])[0]
        assert result.passed

    def test_fails_invalid_values(self, runner, mock_sf):
        mock_sf.execute.return_value = [{"STATUS": "active"}, {"STATUS": "unknown"}]
        chk = CheckDefinition(
            name="test", type=CheckType.ACCEPTED_VALUES, target=_make_target(),
            column="STATUS", accepted_values=["active", "inactive"],
        )
        result = runner.run_checks([chk])[0]
        assert not result.passed


class TestValueRange:
    def test_passes_within_range(self, runner, mock_sf):
        mock_sf.execute.return_value = [{"MIN_VAL": 10, "MAX_VAL": 500}]
        chk = CheckDefinition(
            name="test", type=CheckType.VALUE_RANGE, target=_make_target(),
            column="AMOUNT", min_value=0, max_value=1000,
        )
        result = runner.run_checks([chk])[0]
        assert result.passed

    def test_fails_out_of_range(self, runner, mock_sf):
        mock_sf.execute.return_value = [{"MIN_VAL": -5, "MAX_VAL": 500}]
        chk = CheckDefinition(
            name="test", type=CheckType.VALUE_RANGE, target=_make_target(),
            column="AMOUNT", min_value=0, max_value=1000,
        )
        result = runner.run_checks([chk])[0]
        assert not result.passed
