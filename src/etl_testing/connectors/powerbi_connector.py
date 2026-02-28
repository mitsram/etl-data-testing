"""Power BI connector – refresh datasets, validate reports, check data freshness."""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

import requests

from etl_testing.config import PowerBISettings, get_settings

logger = logging.getLogger(__name__)


class PowerBIConnector:
    """Wrapper around the Power BI REST API (using service principal auth via MSAL)."""

    RESOURCE_URL = "https://analysis.windows.net/powerbi/api"
    API_BASE = "https://api.powerbi.com/v1.0/myorg"

    def __init__(self, settings: PowerBISettings | None = None) -> None:
        self._settings = settings or get_settings().powerbi
        self._token: str | None = None
        self._token_expires: float = 0

    # ── Authentication ─────────────────────────────────────

    def _ensure_token(self) -> str:
        if self._token and time.time() < self._token_expires:
            return self._token

        try:
            import msal
        except ImportError:
            raise ImportError(
                "Power BI connector requires the 'msal' package. "
                "Install with:  pip install etl-data-testing[powerbi]"
            )

        s = self._settings
        app = msal.ConfidentialClientApplication(
            s.client_id,
            authority=f"https://login.microsoftonline.com/{s.tenant_id}",
            client_credential=s.client_secret,
        )
        result = app.acquire_token_for_client(scopes=[f"{self.RESOURCE_URL}/.default"])
        if "access_token" not in result:
            raise RuntimeError(f"Power BI auth failed: {result.get('error_description', result)}")

        self._token = result["access_token"]
        self._token_expires = time.time() + result.get("expires_in", 3600) - 60
        logger.info("Acquired Power BI access token")
        return self._token

    @property
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._ensure_token()}"}

    # ── HTTP helpers ───────────────────────────────────────

    def _get(self, path: str) -> dict[str, Any]:
        url = f"{self.API_BASE}/{path.lstrip('/')}"
        resp = requests.get(url, headers=self._headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, json: dict[str, Any] | None = None) -> requests.Response:
        url = f"{self.API_BASE}/{path.lstrip('/')}"
        resp = requests.post(url, headers=self._headers, json=json, timeout=30)
        resp.raise_for_status()
        return resp

    # ── Workspace operations ───────────────────────────────

    def list_datasets(self, workspace_id: str | None = None) -> list[dict[str, Any]]:
        ws = workspace_id or self._settings.workspace_id
        return self._get(f"groups/{ws}/datasets").get("value", [])

    def get_dataset(self, dataset_id: str, workspace_id: str | None = None) -> dict[str, Any]:
        ws = workspace_id or self._settings.workspace_id
        return self._get(f"groups/{ws}/datasets/{dataset_id}")

    def list_reports(self, workspace_id: str | None = None) -> list[dict[str, Any]]:
        ws = workspace_id or self._settings.workspace_id
        return self._get(f"groups/{ws}/reports").get("value", [])

    # ── Dataset refresh operations ─────────────────────────

    def trigger_refresh(self, dataset_id: str, workspace_id: str | None = None) -> None:
        ws = workspace_id or self._settings.workspace_id
        logger.info("Triggering Power BI refresh for dataset %s", dataset_id)
        self._post(f"groups/{ws}/datasets/{dataset_id}/refreshes")

    def get_refresh_history(
        self, dataset_id: str, workspace_id: str | None = None, top: int = 5
    ) -> list[dict[str, Any]]:
        ws = workspace_id or self._settings.workspace_id
        return self._get(f"groups/{ws}/datasets/{dataset_id}/refreshes?$top={top}").get("value", [])

    def wait_for_refresh(
        self, dataset_id: str, workspace_id: str | None = None,
        timeout_seconds: int = 1800, poll_interval: int = 30,
    ) -> dict[str, Any]:
        """Poll until the latest refresh completes."""
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            history = self.get_refresh_history(dataset_id, workspace_id, top=1)
            if history:
                latest = history[0]
                status = latest.get("status", "Unknown")
                if status == "Completed":
                    return latest
                if status == "Failed":
                    raise RuntimeError(f"Power BI refresh failed: {latest}")
            time.sleep(poll_interval)
        raise TimeoutError(f"Power BI refresh for {dataset_id} timed out after {timeout_seconds}s")

    def trigger_and_wait(self, dataset_id: str, workspace_id: str | None = None, timeout_seconds: int = 1800) -> dict[str, Any]:
        self.trigger_refresh(dataset_id, workspace_id)
        return self.wait_for_refresh(dataset_id, workspace_id, timeout_seconds=timeout_seconds)

    # ── Validation helpers ─────────────────────────────────

    def is_dataset_refreshed_recently(
        self, dataset_id: str, max_age_hours: int = 24, workspace_id: str | None = None
    ) -> bool:
        """Check if a dataset was successfully refreshed within max_age_hours."""
        history = self.get_refresh_history(dataset_id, workspace_id, top=1)
        if not history:
            return False
        latest = history[0]
        if latest.get("status") != "Completed":
            return False
        end_time = latest.get("endTime", "")
        if end_time:
            dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
            age_hours = (datetime.now(tz=dt.tzinfo) - dt).total_seconds() / 3600
            return age_hours <= max_age_hours
        return False

    def get_tables(self, dataset_id: str, workspace_id: str | None = None) -> list[dict[str, Any]]:
        """Return tables in a dataset (push datasets only)."""
        ws = workspace_id or self._settings.workspace_id
        return self._get(f"groups/{ws}/datasets/{dataset_id}/tables").get("value", [])

    def get_datasources(self, dataset_id: str, workspace_id: str | None = None) -> list[dict[str, Any]]:
        """Return datasources for a dataset."""
        ws = workspace_id or self._settings.workspace_id
        return self._get(f"groups/{ws}/datasets/{dataset_id}/datasources").get("value", [])
