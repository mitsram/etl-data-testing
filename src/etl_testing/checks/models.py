"""YAML-based test definition models."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field


# ── Enums ──────────────────────────────────────────────────

class CheckType(str, Enum):
    ROW_COUNT = "row_count"
    ROW_COUNT_COMPARE = "row_count_compare"
    SCHEMA = "schema"
    NULL_CHECK = "null_check"
    UNIQUE_CHECK = "unique_check"
    DUPLICATE_CHECK = "duplicate_check"
    FRESHNESS = "freshness"
    ACCEPTED_VALUES = "accepted_values"
    CUSTOM_SQL = "custom_sql"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    VALUE_RANGE = "value_range"


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


# ── Column expectation models ─────────────────────────────

class ColumnSchema(BaseModel):
    name: str
    data_type: Optional[str] = None
    is_nullable: Optional[bool] = None


# ── Check definitions ─────────────────────────────────────

class TableRef(BaseModel):
    database: str
    schema_: str = Field(..., alias="schema")
    table: str


class CheckDefinition(BaseModel):
    name: str
    type: CheckType
    severity: Severity = Severity.ERROR
    description: Optional[str] = None
    tags: list[str] = Field(default_factory=list)

    # Target table
    target: Optional[TableRef] = None

    # For row_count
    min_rows: Optional[int] = None
    max_rows: Optional[int] = None

    # For row_count_compare
    source: Optional[TableRef] = None
    tolerance_pct: Optional[float] = Field(None, description="Allowed % difference")

    # For schema checks
    expected_columns: Optional[list[ColumnSchema]] = None

    # For null / unique / duplicate checks
    columns: Optional[list[str]] = None

    # For freshness
    timestamp_column: Optional[str] = None
    max_age_hours: Optional[float] = None

    # For accepted_values
    column: Optional[str] = None
    accepted_values: Optional[list[Any]] = None

    # For custom_sql – must return 0 rows to pass
    sql: Optional[str] = None

    # For referential_integrity
    parent: Optional[TableRef] = None
    parent_key: Optional[str] = None
    child_key: Optional[str] = None

    # For value_range
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    model_config = {"populate_by_name": True}


# ── Test suite models ─────────────────────────────────────

class TestSuite(BaseModel):
    name: str
    description: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    checks: list[CheckDefinition]


class TestConfig(BaseModel):
    """Top-level YAML config."""
    version: str = "1"
    suites: list[TestSuite]


# ── Loader ─────────────────────────────────────────────────

def load_test_config(path: str | Path) -> TestConfig:
    """Parse a YAML test configuration file."""
    path = Path(path)
    with path.open() as f:
        raw = yaml.safe_load(f)
    return TestConfig(**raw)
