"""Check runner – executes check definitions against Snowflake."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from etl_testing.checks.models import CheckDefinition, CheckType, Severity
from etl_testing.connectors.snowflake_connector import SnowflakeConnector

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    """Outcome of a single check execution."""

    name: str
    check_type: CheckType
    passed: bool
    severity: Severity
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    executed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    duration_ms: float = 0.0

    @property
    def is_failure(self) -> bool:
        return not self.passed and self.severity == Severity.ERROR

    @property
    def is_warning(self) -> bool:
        return not self.passed and self.severity == Severity.WARNING


class CheckRunner:
    """Execute a list of CheckDefinitions and return results."""

    def __init__(self, snowflake: SnowflakeConnector | None = None) -> None:
        self.sf = snowflake or SnowflakeConnector()

    def run_checks(self, checks: list[CheckDefinition]) -> list[CheckResult]:
        results: list[CheckResult] = []
        for chk in checks:
            logger.info("Running check: %s (%s)", chk.name, chk.type.value)
            start = datetime.now(timezone.utc)
            try:
                result = self._dispatch(chk)
            except Exception as exc:
                result = CheckResult(
                    name=chk.name,
                    check_type=chk.type,
                    passed=False,
                    severity=chk.severity,
                    message=f"Exception: {exc}",
                )
            result.duration_ms = (datetime.now(timezone.utc) - start).total_seconds() * 1000
            results.append(result)
            status = "PASS" if result.passed else "FAIL"
            logger.info("  → %s  %s", status, result.message)
        return results

    # ── Dispatcher ─────────────────────────────────────────

    def _dispatch(self, chk: CheckDefinition) -> CheckResult:
        handler = {
            CheckType.ROW_COUNT: self._check_row_count,
            CheckType.ROW_COUNT_COMPARE: self._check_row_count_compare,
            CheckType.SCHEMA: self._check_schema,
            CheckType.NULL_CHECK: self._check_nulls,
            CheckType.UNIQUE_CHECK: self._check_unique,
            CheckType.DUPLICATE_CHECK: self._check_duplicates,
            CheckType.FRESHNESS: self._check_freshness,
            CheckType.ACCEPTED_VALUES: self._check_accepted_values,
            CheckType.CUSTOM_SQL: self._check_custom_sql,
            CheckType.REFERENTIAL_INTEGRITY: self._check_referential_integrity,
            CheckType.VALUE_RANGE: self._check_value_range,
        }.get(chk.type)
        if handler is None:
            return CheckResult(
                name=chk.name, check_type=chk.type, passed=False,
                severity=chk.severity, message=f"Unknown check type: {chk.type}",
            )
        return handler(chk)

    # ── Individual check implementations ───────────────────

    def _check_row_count(self, chk: CheckDefinition) -> CheckResult:
        t = chk.target
        assert t is not None
        count = self.sf.get_row_count(t.database, t.schema_, t.table)
        passed = True
        if chk.min_rows is not None and count < chk.min_rows:
            passed = False
        if chk.max_rows is not None and count > chk.max_rows:
            passed = False
        msg = f"Row count = {count}"
        if not passed:
            msg += f" (expected {chk.min_rows}–{chk.max_rows})"
        return CheckResult(name=chk.name, check_type=chk.type, passed=passed,
                           severity=chk.severity, message=msg, details={"count": count})

    def _check_row_count_compare(self, chk: CheckDefinition) -> CheckResult:
        s, t = chk.source, chk.target
        assert s is not None and t is not None
        cmp = self.sf.compare_row_counts(
            s.database, s.schema_, s.table,
            t.database, t.schema_, t.table,
        )
        tolerance = chk.tolerance_pct or 0.0
        if cmp["source"] == 0:
            pct_diff = 100.0 if cmp["target"] != 0 else 0.0
        else:
            pct_diff = abs(cmp["difference"]) / cmp["source"] * 100
        passed = pct_diff <= tolerance
        msg = f"Source={cmp['source']} Target={cmp['target']} Diff={cmp['difference']} ({pct_diff:.2f}%)"
        return CheckResult(name=chk.name, check_type=chk.type, passed=passed,
                           severity=chk.severity, message=msg, details=cmp)

    def _check_schema(self, chk: CheckDefinition) -> CheckResult:
        t = chk.target
        assert t is not None and chk.expected_columns is not None
        actual_cols = self.sf.get_columns(t.database, t.schema_, t.table)
        actual_names = {c["COLUMN_NAME"].upper() for c in actual_cols}
        actual_map = {c["COLUMN_NAME"].upper(): c for c in actual_cols}
        issues: list[str] = []

        for exp in chk.expected_columns:
            name_upper = exp.name.upper()
            if name_upper not in actual_names:
                issues.append(f"Missing column: {exp.name}")
                continue
            act = actual_map[name_upper]
            if exp.data_type and act["DATA_TYPE"].upper() != exp.data_type.upper():
                issues.append(f"{exp.name}: expected type {exp.data_type}, got {act['DATA_TYPE']}")
            if exp.is_nullable is not None:
                act_nullable = act["IS_NULLABLE"] == "YES"
                if exp.is_nullable != act_nullable:
                    issues.append(f"{exp.name}: expected nullable={exp.is_nullable}, got {act_nullable}")

        passed = len(issues) == 0
        msg = "Schema OK" if passed else f"Schema issues: {'; '.join(issues)}"
        return CheckResult(name=chk.name, check_type=chk.type, passed=passed,
                           severity=chk.severity, message=msg, details={"issues": issues})

    def _check_nulls(self, chk: CheckDefinition) -> CheckResult:
        t = chk.target
        assert t is not None and chk.columns
        issues: list[str] = []
        for col in chk.columns:
            null_count = self.sf.get_null_count(t.database, t.schema_, t.table, col)
            if null_count > 0:
                issues.append(f"{col}: {null_count} nulls")
        passed = len(issues) == 0
        msg = "No nulls" if passed else f"Null values found: {'; '.join(issues)}"
        return CheckResult(name=chk.name, check_type=chk.type, passed=passed,
                           severity=chk.severity, message=msg, details={"issues": issues})

    def _check_unique(self, chk: CheckDefinition) -> CheckResult:
        t = chk.target
        assert t is not None and chk.columns
        issues: list[str] = []
        for col in chk.columns:
            total = self.sf.get_row_count(t.database, t.schema_, t.table)
            distinct = self.sf.get_distinct_count(t.database, t.schema_, t.table, col)
            if distinct < total:
                issues.append(f"{col}: {total - distinct} duplicates")
        passed = len(issues) == 0
        msg = "All columns unique" if passed else f"Uniqueness violations: {'; '.join(issues)}"
        return CheckResult(name=chk.name, check_type=chk.type, passed=passed,
                           severity=chk.severity, message=msg, details={"issues": issues})

    def _check_duplicates(self, chk: CheckDefinition) -> CheckResult:
        t = chk.target
        assert t is not None and chk.columns
        dup_count = self.sf.get_duplicate_count(t.database, t.schema_, t.table, chk.columns)
        passed = dup_count == 0
        msg = f"Duplicate groups: {dup_count}"
        return CheckResult(name=chk.name, check_type=chk.type, passed=passed,
                           severity=chk.severity, message=msg, details={"duplicate_groups": dup_count})

    def _check_freshness(self, chk: CheckDefinition) -> CheckResult:
        t = chk.target
        assert t is not None and chk.timestamp_column and chk.max_age_hours
        latest = self.sf.get_table_freshness(t.database, t.schema_, t.table, chk.timestamp_column)
        if latest is None:
            return CheckResult(name=chk.name, check_type=chk.type, passed=False,
                               severity=chk.severity, message="No timestamp data found")
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600
        passed = age_hours <= chk.max_age_hours
        msg = f"Data age: {age_hours:.1f}h (max allowed: {chk.max_age_hours}h)"
        return CheckResult(name=chk.name, check_type=chk.type, passed=passed,
                           severity=chk.severity, message=msg, details={"age_hours": age_hours})

    def _check_accepted_values(self, chk: CheckDefinition) -> CheckResult:
        t = chk.target
        assert t is not None and chk.column and chk.accepted_values is not None
        fq = f'"{t.database}"."{t.schema_}"."{t.table}"'
        sql = f'SELECT DISTINCT "{chk.column}" FROM {fq} WHERE "{chk.column}" IS NOT NULL'
        rows = self.sf.execute(sql)
        actual = {list(r.values())[0] for r in rows}
        expected = set(chk.accepted_values)
        invalid = actual - expected
        passed = len(invalid) == 0
        msg = f"All values accepted" if passed else f"Unexpected values: {invalid}"
        return CheckResult(name=chk.name, check_type=chk.type, passed=passed,
                           severity=chk.severity, message=msg, details={"invalid_values": list(invalid)})

    def _check_custom_sql(self, chk: CheckDefinition) -> CheckResult:
        assert chk.sql
        rows = self.sf.execute(chk.sql)
        passed = len(rows) == 0
        msg = "Custom SQL returned 0 rows (pass)" if passed else f"Custom SQL returned {len(rows)} rows (fail)"
        return CheckResult(name=chk.name, check_type=chk.type, passed=passed,
                           severity=chk.severity, message=msg,
                           details={"row_count": len(rows), "sample_rows": rows[:5]})

    def _check_referential_integrity(self, chk: CheckDefinition) -> CheckResult:
        t, p = chk.target, chk.parent
        assert t is not None and p is not None and chk.child_key and chk.parent_key
        child_fq = f'"{t.database}"."{t.schema_}"."{t.table}"'
        parent_fq = f'"{p.database}"."{p.schema_}"."{p.table}"'
        sql = f"""
            SELECT COUNT(*) FROM {child_fq} c
            LEFT JOIN {parent_fq} p ON c."{chk.child_key}" = p."{chk.parent_key}"
            WHERE p."{chk.parent_key}" IS NULL AND c."{chk.child_key}" IS NOT NULL
        """
        orphan_count = int(self.sf.execute_scalar(sql))
        passed = orphan_count == 0
        msg = f"Orphan records: {orphan_count}"
        return CheckResult(name=chk.name, check_type=chk.type, passed=passed,
                           severity=chk.severity, message=msg, details={"orphan_count": orphan_count})

    def _check_value_range(self, chk: CheckDefinition) -> CheckResult:
        t = chk.target
        assert t is not None and chk.column
        fq = f'"{t.database}"."{t.schema_}"."{t.table}"'
        sql = f'SELECT MIN("{chk.column}") AS min_val, MAX("{chk.column}") AS max_val FROM {fq}'
        row = self.sf.execute(sql)[0]
        min_val, max_val = row["MIN_VAL"], row["MAX_VAL"]
        passed = True
        if chk.min_value is not None and min_val is not None and float(min_val) < chk.min_value:
            passed = False
        if chk.max_value is not None and max_val is not None and float(max_val) > chk.max_value:
            passed = False
        msg = f"Range: [{min_val}, {max_val}]"
        if not passed:
            msg += f" (expected [{chk.min_value}, {chk.max_value}])"
        return CheckResult(name=chk.name, check_type=chk.type, passed=passed,
                           severity=chk.severity, message=msg,
                           details={"actual_min": min_val, "actual_max": max_val})
