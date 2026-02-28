"""CLI entry point – `etl-test` command."""

from __future__ import annotations

import logging
import sys

import click
from rich.console import Console
from rich.logging import RichHandler

from etl_testing.checks.models import load_test_config
from etl_testing.checks.runner import CheckRunner
from etl_testing.connectors.snowflake_connector import SnowflakeConnector
from etl_testing.pipeline import PipelineOrchestrator
from etl_testing.reporting import (
    print_check_results,
    print_pipeline_report,
    print_summary,
    save_html_report,
    save_json_report,
)

console = Console()


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, markup=True)],
    )


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging")
def main(verbose: bool) -> None:
    """ETL Data Testing Framework – validate your Fivetran → Coalesce → Snowflake → Power BI pipeline."""
    _setup_logging(verbose)


# ── Run data quality checks only ───────────────────────────

@main.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--report-dir", default="reports", help="Output directory for reports")
@click.option("--html/--no-html", default=True, help="Generate HTML report")
@click.option("--json-report/--no-json-report", "json_flag", default=True, help="Generate JSON report")
@click.option("--tags", default=None, help="Comma-separated tags to filter checks")
def check(config_path: str, report_dir: str, html: bool, json_flag: bool, tags: str | None) -> None:
    """Run data quality checks from a YAML config file."""
    config = load_test_config(config_path)
    tag_set = {t.strip() for t in tags.split(",")} if tags else None

    sf = SnowflakeConnector()
    runner = CheckRunner(snowflake=sf)

    from etl_testing.pipeline import PipelineResult
    pipeline_result = PipelineResult()

    for suite in config.suites:
        checks = suite.checks
        if tag_set:
            checks = [c for c in checks if tag_set & set(c.tags)]
        if checks:
            console.print(f"\n[bold]Suite: {suite.name}[/bold]")
            results = runner.run_checks(checks)
            pipeline_result.check_results.extend(results)

    print_check_results(pipeline_result.check_results)
    print_summary(pipeline_result)

    if json_flag:
        path = save_json_report(pipeline_result, report_dir)
        console.print(f"JSON report: {path}")
    if html:
        path = save_html_report(pipeline_result, report_dir)
        console.print(f"HTML report: {path}")

    sf.close()
    failures = sum(1 for r in pipeline_result.check_results if r.is_failure)
    sys.exit(1 if failures > 0 else 0)


# ── Run full pipeline ──────────────────────────────────────

@main.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--fivetran-connectors", default=None, help="Comma-separated Fivetran connector IDs")
@click.option("--coalesce-env", default=None, help="Coalesce environment ID override")
@click.option("--skip-fivetran", is_flag=True, help="Skip Fivetran sync stage")
@click.option("--skip-coalesce", is_flag=True, help="Skip Coalesce transformation stage")
@click.option("--powerbi-dataset", default=None, help="Power BI dataset ID to refresh")
@click.option("--report-dir", default="reports", help="Output directory for reports")
def pipeline(
    config_path: str,
    fivetran_connectors: str | None,
    coalesce_env: str | None,
    skip_fivetran: bool,
    skip_coalesce: bool,
    powerbi_dataset: str | None,
    report_dir: str,
) -> None:
    """Run the full ETL test pipeline (sync → transform → validate → refresh)."""
    connector_ids = [c.strip() for c in fivetran_connectors.split(",")] if fivetran_connectors else []

    orchestrator = PipelineOrchestrator()
    result = orchestrator.run_full_pipeline(
        test_config_path=config_path,
        fivetran_connector_ids=connector_ids,
        coalesce_environment_id=coalesce_env,
        run_fivetran=not skip_fivetran and len(connector_ids) > 0,
        run_coalesce=not skip_coalesce,
        run_powerbi=powerbi_dataset is not None,
        powerbi_dataset_id=powerbi_dataset,
    )

    print_pipeline_report(result)
    save_json_report(result, report_dir)
    save_html_report(result, report_dir)

    orchestrator.snowflake.close()
    sys.exit(0 if result.all_passed else 1)


# ── Validate connectivity ─────────────────────────────────

@main.command()
@click.option("--snowflake", "test_sf", is_flag=True, help="Test Snowflake connection")
@click.option("--fivetran", "test_ft", is_flag=True, help="Test Fivetran API")
@click.option("--coalesce", "test_cl", is_flag=True, help="Test Coalesce API")
@click.option("--all", "test_all", is_flag=True, help="Test all connections")
def ping(test_sf: bool, test_ft: bool, test_cl: bool, test_all: bool) -> None:
    """Test connectivity to configured platforms."""
    if test_all:
        test_sf = test_ft = test_cl = True
    if not any([test_sf, test_ft, test_cl]):
        test_sf = test_ft = test_cl = True

    if test_sf:
        try:
            sf = SnowflakeConnector()
            version = sf.execute_scalar("SELECT CURRENT_VERSION()")
            console.print(f"[green]✓[/green] Snowflake connected (v{version})")
            sf.close()
        except Exception as e:
            console.print(f"[red]✗[/red] Snowflake: {e}")

    if test_ft:
        try:
            from etl_testing.connectors.fivetran_connector import FivetranConnector
            ft = FivetranConnector()
            # A lightweight call to verify auth
            ft._get("users")
            console.print("[green]✓[/green] Fivetran API authenticated")
        except Exception as e:
            console.print(f"[red]✗[/red] Fivetran: {e}")

    if test_cl:
        try:
            from etl_testing.connectors.coalesce_connector import CoalesceConnector
            cl = CoalesceConnector()
            cl.get_environments()
            console.print("[green]✓[/green] Coalesce API authenticated")
        except Exception as e:
            console.print(f"[red]✗[/red] Coalesce: {e}")


if __name__ == "__main__":
    main()
