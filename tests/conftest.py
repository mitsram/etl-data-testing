"""Shared pytest fixtures for ETL testing."""

from __future__ import annotations

import pytest

from etl_testing.connectors.snowflake_connector import SnowflakeConnector
from etl_testing.connectors.fivetran_connector import FivetranConnector
from etl_testing.connectors.coalesce_connector import CoalesceConnector
from etl_testing.config import get_settings


@pytest.fixture(scope="session")
def settings():
    """Return the global settings object."""
    return get_settings()


@pytest.fixture(scope="session")
def snowflake_conn(settings):
    """Create a session-scoped Snowflake connection."""
    conn = SnowflakeConnector(settings.snowflake)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def fivetran_conn(settings):
    """Create a session-scoped Fivetran connector."""
    return FivetranConnector(settings.fivetran)


@pytest.fixture(scope="session")
def coalesce_conn(settings):
    """Create a session-scoped Coalesce connector."""
    return CoalesceConnector(settings.coalesce)
