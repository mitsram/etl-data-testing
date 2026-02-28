"""Centralised configuration loaded from .env / environment variables."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


_ENV_FILE = Path(__file__).resolve().parents[3] / ".env"


class SnowflakeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SNOWFLAKE_", env_file=str(_ENV_FILE), extra="ignore")

    account: str = ""
    user: str = ""
    password: str = ""
    warehouse: str = ""
    database: str = ""
    schema_: str = Field("", alias="SNOWFLAKE_SCHEMA")
    role: str = ""


class FivetranSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FIVETRAN_", env_file=str(_ENV_FILE), extra="ignore")

    api_key: str = ""
    api_secret: str = ""
    base_url: str = "https://api.fivetran.com/v1"


class CoalesceSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="COALESCE_", env_file=str(_ENV_FILE), extra="ignore")

    api_token: str = ""
    base_url: str = "https://app.coalescesoftware.io/api/v1"
    environment_id: str = ""


class PowerBISettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="POWERBI_", env_file=str(_ENV_FILE), extra="ignore")

    tenant_id: str = ""
    client_id: str = ""
    client_secret: str = ""
    workspace_id: str = ""


class Settings(BaseSettings):
    """Aggregate settings – access sub-objects for each platform."""

    model_config = SettingsConfigDict(env_file=str(_ENV_FILE), extra="ignore")

    snowflake: SnowflakeSettings = SnowflakeSettings()
    fivetran: FivetranSettings = FivetranSettings()
    coalesce: CoalesceSettings = CoalesceSettings()
    powerbi: PowerBISettings = PowerBISettings()

    # Framework-level settings
    test_config_path: Optional[str] = Field(None, description="Path to YAML test config")
    report_dir: str = "reports"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached singleton of the application settings."""
    return Settings()
