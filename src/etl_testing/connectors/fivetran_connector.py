"""Fivetran connector – monitor sync status, trigger syncs, inspect connectors."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests
from requests.auth import HTTPBasicAuth

from etl_testing.config import FivetranSettings, get_settings

logger = logging.getLogger(__name__)


class FivetranConnector:
    """Wrapper around the Fivetran REST API v1."""

    def __init__(self, settings: FivetranSettings | None = None) -> None:
        self._settings = settings or get_settings().fivetran
        self._auth = HTTPBasicAuth(self._settings.api_key, self._settings.api_secret)
        self._base = self._settings.base_url.rstrip("/")

    # ── HTTP helpers ───────────────────────────────────────

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self._base}/{path.lstrip('/')}"
        resp = requests.get(url, auth=self._auth, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, json: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self._base}/{path.lstrip('/')}"
        resp = requests.post(url, auth=self._auth, json=json, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _patch(self, path: str, json: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self._base}/{path.lstrip('/')}"
        resp = requests.patch(url, auth=self._auth, json=json, timeout=30)
        resp.raise_for_status()
        return resp.json()

    # ── Connector operations ───────────────────────────────

    def get_connector(self, connector_id: str) -> dict[str, Any]:
        """Get connector details."""
        return self._get(f"connectors/{connector_id}")["data"]

    def get_connector_status(self, connector_id: str) -> dict[str, Any]:
        """Return sync status, last sync time, and setup state."""
        data = self.get_connector(connector_id)
        return {
            "connector_id": connector_id,
            "service": data.get("service"),
            "sync_state": data.get("status", {}).get("sync_state"),
            "setup_state": data.get("status", {}).get("setup_state"),
            "succeeded_at": data.get("succeeded_at"),
            "failed_at": data.get("failed_at"),
            "is_historical_sync": data.get("status", {}).get("is_historical_sync", False),
        }

    def list_connectors(self, group_id: str) -> list[dict[str, Any]]:
        """List all connectors in a group (destination)."""
        return self._get(f"groups/{group_id}/connectors")["data"]["items"]

    def trigger_sync(self, connector_id: str) -> dict[str, Any]:
        """Force a manual sync for a connector."""
        logger.info("Triggering sync for connector %s", connector_id)
        return self._post(f"connectors/{connector_id}/force")

    def wait_for_sync(
        self, connector_id: str, timeout_seconds: int = 1800, poll_interval: int = 30
    ) -> dict[str, Any]:
        """
        Poll until the connector finishes syncing or times out.
        Returns the final status dict.
        """
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            status = self.get_connector_status(connector_id)
            state = status.get("sync_state")
            logger.debug("Connector %s sync_state=%s", connector_id, state)
            if state in ("synced", "rescheduled", "paused"):
                return status
            time.sleep(poll_interval)
        raise TimeoutError(f"Connector {connector_id} did not finish within {timeout_seconds}s")

    def trigger_and_wait(self, connector_id: str, timeout_seconds: int = 1800) -> dict[str, Any]:
        """Trigger a sync and block until it completes."""
        self.trigger_sync(connector_id)
        return self.wait_for_sync(connector_id, timeout_seconds=timeout_seconds)

    # ── Validation helpers ─────────────────────────────────

    def is_sync_healthy(self, connector_id: str) -> bool:
        """Return True if the connector's last sync succeeded."""
        status = self.get_connector_status(connector_id)
        return status["setup_state"] == "connected" and status["sync_state"] in ("synced", "rescheduled")

    def get_last_sync_time(self, connector_id: str) -> datetime | None:
        """Return the datetime of the last successful sync."""
        status = self.get_connector_status(connector_id)
        ts = status.get("succeeded_at")
        if ts:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return None

    def get_sync_frequency(self, connector_id: str) -> int | None:
        """Return the connector sync frequency in minutes."""
        data = self.get_connector(connector_id)
        return data.get("sync_frequency")

    def get_schema_config(self, connector_id: str) -> dict[str, Any]:
        """Return the schema configuration (enabled tables/columns)."""
        return self._get(f"connectors/{connector_id}/schemas")["data"]

    # ── Group / destination operations ─────────────────────

    def get_group(self, group_id: str) -> dict[str, Any]:
        """Get destination group details."""
        return self._get(f"groups/{group_id}")["data"]
