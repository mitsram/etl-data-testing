"""Coalesce connector – trigger jobs, inspect nodes, validate transformations."""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from etl_testing.config import CoalesceSettings, get_settings

logger = logging.getLogger(__name__)


class CoalesceConnector:
    """Wrapper around the Coalesce REST API."""

    def __init__(self, settings: CoalesceSettings | None = None) -> None:
        self._settings = settings or get_settings().coalesce
        self._base = self._settings.base_url.rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {self._settings.api_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    # ── HTTP helpers ───────────────────────────────────────

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self._base}/{path.lstrip('/')}"
        resp = requests.get(url, headers=self._headers, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, json: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self._base}/{path.lstrip('/')}"
        resp = requests.post(url, headers=self._headers, json=json, timeout=60)
        resp.raise_for_status()
        return resp.json()

    # ── Environment & job operations ───────────────────────

    def get_environments(self) -> list[dict[str, Any]]:
        """List all environments."""
        return self._get("environments")

    def get_environment(self, env_id: str | None = None) -> dict[str, Any]:
        """Get a specific environment."""
        env_id = env_id or self._settings.environment_id
        return self._get(f"environments/{env_id}")

    def trigger_job(self, environment_id: str | None = None, job_type: str = "refresh") -> dict[str, Any]:
        """
        Trigger a Coalesce job (refresh / deploy).
        Returns the run ID for polling.
        """
        env_id = environment_id or self._settings.environment_id
        payload = {
            "environmentID": env_id,
            "jobType": job_type,
        }
        logger.info("Triggering Coalesce %s job in env %s", job_type, env_id)
        return self._post("runs", json=payload)

    def get_run_status(self, run_id: str) -> dict[str, Any]:
        """Get the status of a specific run."""
        return self._get(f"runs/{run_id}")

    def wait_for_run(
        self, run_id: str, timeout_seconds: int = 3600, poll_interval: int = 30
    ) -> dict[str, Any]:
        """Poll until a run completes or times out."""
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            run = self.get_run_status(run_id)
            status = run.get("runStatus", run.get("status", ""))
            logger.debug("Run %s status=%s", run_id, status)
            if status in ("completed", "Completed"):
                return run
            if status in ("failed", "Failed", "canceled", "Canceled"):
                raise RuntimeError(f"Coalesce run {run_id} ended with status: {status}")
            time.sleep(poll_interval)
        raise TimeoutError(f"Coalesce run {run_id} did not finish within {timeout_seconds}s")

    def trigger_and_wait(
        self, environment_id: str | None = None, job_type: str = "refresh", timeout_seconds: int = 3600
    ) -> dict[str, Any]:
        """Trigger a job and block until it completes."""
        result = self.trigger_job(environment_id, job_type)
        run_id = result.get("runCounter") or result.get("id") or result.get("runID")
        if not run_id:
            raise ValueError(f"Could not extract run ID from trigger response: {result}")
        return self.wait_for_run(str(run_id), timeout_seconds=timeout_seconds)

    # ── Node / object introspection ────────────────────────

    def get_nodes(self, environment_id: str | None = None) -> list[dict[str, Any]]:
        """List all nodes (tables / views) in an environment."""
        env_id = environment_id or self._settings.environment_id
        return self._get(f"environments/{env_id}/nodes")

    def get_node(self, node_id: str, environment_id: str | None = None) -> dict[str, Any]:
        """Get details for a specific node."""
        env_id = environment_id or self._settings.environment_id
        return self._get(f"environments/{env_id}/nodes/{node_id}")

    def get_node_sql(self, node_id: str, environment_id: str | None = None) -> str | None:
        """Return the generated SQL for a node, if available."""
        node = self.get_node(node_id, environment_id)
        return node.get("generatedSQL") or node.get("sql")

    # ── Run history ────────────────────────────────────────

    def get_runs(self, environment_id: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
        """Return recent runs for an environment."""
        env_id = environment_id or self._settings.environment_id
        return self._get("runs", params={"environmentID": env_id, "limit": limit}).get("data", [])

    def get_last_successful_run(self, environment_id: str | None = None) -> dict[str, Any] | None:
        """Return the most recent successful run."""
        runs = self.get_runs(environment_id, limit=20)
        for run in runs:
            status = run.get("runStatus", run.get("status", ""))
            if status in ("completed", "Completed"):
                return run
        return None
