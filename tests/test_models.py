"""
Unit tests for YAML config loading and model validation.

Run with:  pytest tests/test_models.py -v
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from etl_testing.checks.models import CheckType, Severity, TestConfig, load_test_config


@pytest.fixture
def sample_yaml(tmp_path) -> Path:
    content = textwrap.dedent("""\
        version: "1"
        suites:
          - name: "Test Suite"
            description: "Unit test suite"
            tags: [test]
            checks:
              - name: "row_count_check"
                type: row_count
                severity: error
                target:
                  database: DB
                  schema: SCH
                  table: TBL
                min_rows: 1

              - name: "freshness_check"
                type: freshness
                severity: warning
                tags: [freshness]
                target:
                  database: DB
                  schema: SCH
                  table: TBL
                timestamp_column: UPDATED_AT
                max_age_hours: 12

              - name: "custom_check"
                type: custom_sql
                sql: "SELECT 1 WHERE 1=0"
    """)
    p = tmp_path / "test_config.yaml"
    p.write_text(content)
    return p


def test_load_config(sample_yaml):
    config = load_test_config(sample_yaml)
    assert config.version == "1"
    assert len(config.suites) == 1
    suite = config.suites[0]
    assert suite.name == "Test Suite"
    assert len(suite.checks) == 3


def test_check_types_parsed(sample_yaml):
    config = load_test_config(sample_yaml)
    checks = config.suites[0].checks
    assert checks[0].type == CheckType.ROW_COUNT
    assert checks[1].type == CheckType.FRESHNESS
    assert checks[2].type == CheckType.CUSTOM_SQL


def test_severity_defaults(sample_yaml):
    config = load_test_config(sample_yaml)
    checks = config.suites[0].checks
    assert checks[0].severity == Severity.ERROR
    assert checks[1].severity == Severity.WARNING
    assert checks[2].severity == Severity.ERROR  # default


def test_target_parsed(sample_yaml):
    config = load_test_config(sample_yaml)
    target = config.suites[0].checks[0].target
    assert target is not None
    assert target.database == "DB"
    assert target.schema_ == "SCH"
    assert target.table == "TBL"


def test_tags_parsed(sample_yaml):
    config = load_test_config(sample_yaml)
    suite = config.suites[0]
    assert "test" in suite.tags
    assert "freshness" in suite.checks[1].tags
