"""Pipeline orchestrator – coordinate end-to-end ETL testing across all platforms."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from etl_testing.checks.models import TestConfig, load_test_config
from etl_testing.checks.runner import CheckResult, CheckRunner
from etl_testing.connectors.coalesce_connector import CoalesceConnector
from etl_testing.connectors.fivetran_connector import FivetranConnector
from etl_testing.connectors.snowflake_connector import SnowflakeConnector

logger = logging.getLogger(__name__)


class PipelineStage(str, Enum):
    FIVETRAN_SYNC = "fivetran_sync"
    COALESCE_TRANSFORM = "coalesce_transform"
    DATA_QUALITY = "data_quality"
    POWERBI_REFRESH = "powerbi_refresh"


@dataclass
class StageResult:
    stage: PipelineStage
    success: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None
    duration_seconds: float = 0.0


@dataclass
class PipelineResult:
    stages: list[StageResult] = field(default_factory=list)
    check_results: list[CheckResult] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        stages_ok = all(s.success for s in self.stages)
        checks_ok = all(not r.is_failure for r in self.check_results)
        return stages_ok and checks_ok

    @property
    def summary(self) -> dict[str, Any]:
        total_checks = len(self.check_results)
        passed = sum(1 for r in self.check_results if r.passed)
        failed = sum(1 for r in self.check_results if r.is_failure)
        warnings = sum(1 for r in self.check_results if r.is_warning)
        return {
            "stages_executed": len(self.stages),
            "stages_passed": sum(1 for s in self.stages if s.success),
            "total_checks": total_checks,
            "checks_passed": passed,
            "checks_failed": failed,
            "checks_warnings": warnings,
            "overall_success": self.all_passed,
        }


class PipelineOrchestrator:
    """
    Orchestrate an end-to-end ETL test run.

    Typical flow:
      1. Trigger Fivetran sync(s) and wait
      2. Trigger Coalesce transformation job and wait
      3. Run data quality checks against Snowflake
      4. (Optional) Trigger Power BI refresh and validate
    """

    def __init__(
        self,
        snowflake: SnowflakeConnector | None = None,
        fivetran: FivetranConnector | None = None,
        coalesce: CoalesceConnector | None = None,
    ) -> None:
        self.snowflake = snowflake or SnowflakeConnector()
        self.fivetran = fivetran or FivetranConnector()
        self.coalesce = coalesce or CoalesceConnector()

    # ── Stage runners ──────────────────────────────────────

    def run_fivetran_sync(self, connector_ids: list[str], timeout_seconds: int = 1800) -> StageResult:
        """Trigger and wait for Fivetran syncs."""
        start = datetime.now(timezone.utc)
        result = StageResult(stage=PipelineStage.FIVETRAN_SYNC, success=True, message="", started_at=start)
        try:
            statuses = {}
            for cid in connector_ids:
                self.fivetran.trigger_sync(cid)
            for cid in connector_ids:
                statuses[cid] = self.fivetran.wait_for_sync(cid, timeout_seconds=timeout_seconds)
            result.message = f"All {len(connector_ids)} connectors synced successfully"
            result.details = {"connector_statuses": statuses}
        except Exception as exc:
            result.success = False
            result.message = f"Fivetran sync failed: {exc}"
        result.finished_at = datetime.now(timezone.utc)
        result.duration_seconds = (result.finished_at - start).total_seconds()
        return result

    def run_coalesce_transform(
        self, environment_id: str | None = None, job_type: str = "refresh", timeout_seconds: int = 3600
    ) -> StageResult:
        """Trigger and wait for a Coalesce transformation."""
        start = datetime.now(timezone.utc)
        result = StageResult(stage=PipelineStage.COALESCE_TRANSFORM, success=True, message="", started_at=start)
        try:
            run = self.coalesce.trigger_and_wait(environment_id, job_type, timeout_seconds)
            result.message = "Coalesce job completed successfully"
            result.details = {"run": run}
        except Exception as exc:
            result.success = False
            result.message = f"Coalesce job failed: {exc}"
        result.finished_at = datetime.now(timezone.utc)
        result.duration_seconds = (result.finished_at - start).total_seconds()
        return result

    def run_data_quality_checks(self, config: TestConfig) -> tuple[StageResult, list[CheckResult]]:
        """Run all data quality checks from a TestConfig."""
        start = datetime.now(timezone.utc)
        runner = CheckRunner(snowflake=self.snowflake)
        all_results: list[CheckResult] = []
        for suite in config.suites:
            results = runner.run_checks(suite.checks)
            all_results.extend(results)
        failures = sum(1 for r in all_results if r.is_failure)
        stage = StageResult(
            stage=PipelineStage.DATA_QUALITY,
            success=failures == 0,
            message=f"{len(all_results)} checks executed, {failures} failures",
            started_at=start,
        )
        stage.finished_at = datetime.now(timezone.utc)
        stage.duration_seconds = (stage.finished_at - start).total_seconds()
        return stage, all_results

    # ── Full pipeline run ──────────────────────────────────

    def run_full_pipeline(
        self,
        test_config_path: str,
        fivetran_connector_ids: list[str] | None = None,
        coalesce_environment_id: str | None = None,
        run_fivetran: bool = True,
        run_coalesce: bool = True,
        run_powerbi: bool = False,
        powerbi_dataset_id: str | None = None,
    ) -> PipelineResult:
        """Execute the complete ETL test pipeline."""
        pipeline = PipelineResult()
        config = load_test_config(test_config_path)

        # 1. Fivetran
        if run_fivetran and fivetran_connector_ids:
            logger.info("═══ Stage 1: Fivetran Sync ═══")
            stage = self.run_fivetran_sync(fivetran_connector_ids)
            pipeline.stages.append(stage)
            if not stage.success:
                logger.error("Fivetran sync failed – aborting pipeline")
                return pipeline

        # 2. Coalesce
        if run_coalesce:
            logger.info("═══ Stage 2: Coalesce Transformation ═══")
            stage = self.run_coalesce_transform(coalesce_environment_id)
            pipeline.stages.append(stage)
            if not stage.success:
                logger.error("Coalesce transformation failed – aborting pipeline")
                return pipeline

        # 3. Data quality
        logger.info("═══ Stage 3: Data Quality Checks ═══")
        dq_stage, check_results = self.run_data_quality_checks(config)
        pipeline.stages.append(dq_stage)
        pipeline.check_results = check_results

        # 4. Power BI (optional)
        if run_powerbi and powerbi_dataset_id:
            logger.info("═══ Stage 4: Power BI Refresh ═══")
            start = datetime.now(timezone.utc)
            try:
                from etl_testing.connectors.powerbi_connector import PowerBIConnector
                pbi = PowerBIConnector()
                pbi.trigger_and_wait(powerbi_dataset_id)
                stage = StageResult(
                    stage=PipelineStage.POWERBI_REFRESH, success=True,
                    message="Power BI dataset refreshed", started_at=start,
                )
            except Exception as exc:
                stage = StageResult(
                    stage=PipelineStage.POWERBI_REFRESH, success=False,
                    message=f"Power BI refresh failed: {exc}", started_at=start,
                )
            stage.finished_at = datetime.now(timezone.utc)
            stage.duration_seconds = (stage.finished_at - start).total_seconds()
            pipeline.stages.append(stage)

        return pipeline
