"""Snowflake connector – execute queries, fetch metadata, validate schemas."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator

import pandas as pd
import snowflake.connector
from snowflake.connector import DictCursor

from etl_testing.config import SnowflakeSettings, get_settings

logger = logging.getLogger(__name__)


class SnowflakeConnector:
    """Thin wrapper around the Snowflake Python connector."""

    def __init__(self, settings: SnowflakeSettings | None = None) -> None:
        self._settings = settings or get_settings().snowflake
        self._conn: snowflake.connector.SnowflakeConnection | None = None

    # ── Connection management ──────────────────────────────

    def connect(self) -> snowflake.connector.SnowflakeConnection:
        if self._conn is None or self._conn.is_closed():
            s = self._settings
            self._conn = snowflake.connector.connect(
                account=s.account,
                user=s.user,
                password=s.password,
                warehouse=s.warehouse,
                database=s.database,
                schema=s.schema_,
                role=s.role,
            )
            logger.info("Connected to Snowflake account=%s db=%s", s.account, s.database)
        return self._conn

    def close(self) -> None:
        if self._conn and not self._conn.is_closed():
            self._conn.close()
            logger.info("Snowflake connection closed")

    @contextmanager
    def cursor(self, dict_cursor: bool = True) -> Generator[Any, None, None]:
        conn = self.connect()
        cur = conn.cursor(DictCursor if dict_cursor else None)
        try:
            yield cur
        finally:
            cur.close()

    # ── Query helpers ──────────────────────────────────────

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
        """Execute a SQL statement and return rows as list of dicts."""
        with self.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def execute_scalar(self, sql: str, params: tuple[Any, ...] | None = None) -> Any:
        """Execute and return the first column of the first row."""
        rows = self.execute(sql, params)
        if rows:
            return list(rows[0].values())[0]
        return None

    def query_to_df(self, sql: str, params: tuple[Any, ...] | None = None) -> pd.DataFrame:
        """Execute a query and return results as a Pandas DataFrame."""
        with self.cursor(dict_cursor=False) as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)

    # ── Metadata helpers ───────────────────────────────────

    def get_row_count(self, database: str, schema: str, table: str) -> int:
        fq = f'"{database}"."{schema}"."{table}"'
        return int(self.execute_scalar(f"SELECT COUNT(*) FROM {fq}"))

    def get_columns(self, database: str, schema: str, table: str) -> list[dict[str, Any]]:
        sql = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH,
                   NUMERIC_PRECISION, NUMERIC_SCALE
            FROM {db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """.format(db=f'"{database}"')
        return self.execute(sql, (schema, table))

    def get_table_freshness(self, database: str, schema: str, table: str, timestamp_column: str) -> datetime | None:
        """Return the max value of a timestamp column (data freshness)."""
        fq = f'"{database}"."{schema}"."{table}"'
        result = self.execute_scalar(f'SELECT MAX("{timestamp_column}") FROM {fq}')
        if result and not isinstance(result, datetime):
            result = datetime.fromisoformat(str(result))
        return result

    def get_null_count(self, database: str, schema: str, table: str, column: str) -> int:
        fq = f'"{database}"."{schema}"."{table}"'
        return int(self.execute_scalar(f'SELECT COUNT(*) FROM {fq} WHERE "{column}" IS NULL'))

    def get_distinct_count(self, database: str, schema: str, table: str, column: str) -> int:
        fq = f'"{database}"."{schema}"."{table}"'
        return int(self.execute_scalar(f'SELECT COUNT(DISTINCT "{column}") FROM {fq}'))

    def get_duplicate_count(self, database: str, schema: str, table: str, columns: list[str]) -> int:
        """Return number of duplicate rows based on a set of columns."""
        fq = f'"{database}"."{schema}"."{table}"'
        cols = ", ".join(f'"{c}"' for c in columns)
        sql = f"SELECT COUNT(*) FROM (SELECT {cols}, COUNT(*) AS cnt FROM {fq} GROUP BY {cols} HAVING cnt > 1)"
        return int(self.execute_scalar(sql))

    def compare_row_counts(
        self,
        src_db: str, src_schema: str, src_table: str,
        tgt_db: str, tgt_schema: str, tgt_table: str,
    ) -> dict[str, int]:
        src = self.get_row_count(src_db, src_schema, src_table)
        tgt = self.get_row_count(tgt_db, tgt_schema, tgt_table)
        return {"source": src, "target": tgt, "difference": tgt - src}
