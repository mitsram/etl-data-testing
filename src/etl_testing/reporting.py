"""HTML + console report generation for test results."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.table import Table

from etl_testing.checks.runner import CheckResult
from etl_testing.pipeline import PipelineResult, StageResult

logger = logging.getLogger(__name__)
console = Console()


# ── Console report ─────────────────────────────────────────

def print_stage_results(stages: list[StageResult]) -> None:
    """Print pipeline stage results to console."""
    table = Table(title="Pipeline Stages", show_lines=True)
    table.add_column("Stage", style="bold")
    table.add_column("Status")
    table.add_column("Duration")
    table.add_column("Message")

    for s in stages:
        status = "[green]✓ PASS[/green]" if s.success else "[red]✗ FAIL[/red]"
        dur = f"{s.duration_seconds:.1f}s"
        table.add_row(s.stage.value, status, dur, s.message)

    console.print(table)


def print_check_results(results: list[CheckResult]) -> None:
    """Print data quality check results to console."""
    table = Table(title="Data Quality Checks", show_lines=True)
    table.add_column("#", style="dim")
    table.add_column("Check", style="bold")
    table.add_column("Type")
    table.add_column("Status")
    table.add_column("Message")
    table.add_column("Duration")

    for i, r in enumerate(results, 1):
        if r.passed:
            status = "[green]✓ PASS[/green]"
        elif r.is_warning:
            status = "[yellow]⚠ WARN[/yellow]"
        else:
            status = "[red]✗ FAIL[/red]"
        dur = f"{r.duration_ms:.0f}ms"
        table.add_row(str(i), r.name, r.check_type.value, status, r.message, dur)

    console.print(table)


def print_summary(result: PipelineResult) -> None:
    """Print overall pipeline summary."""
    s = result.summary
    console.print()
    console.rule("[bold]Pipeline Summary[/bold]")
    console.print(f"  Stages: {s['stages_passed']}/{s['stages_executed']} passed")
    console.print(f"  Checks: {s['checks_passed']}/{s['total_checks']} passed, "
                  f"{s['checks_failed']} failed, {s['checks_warnings']} warnings")
    if s["overall_success"]:
        console.print("  [bold green]OVERALL: PASS ✓[/bold green]")
    else:
        console.print("  [bold red]OVERALL: FAIL ✗[/bold red]")
    console.print()


def print_pipeline_report(result: PipelineResult) -> None:
    """Full console report."""
    if result.stages:
        print_stage_results(result.stages)
    if result.check_results:
        print_check_results(result.check_results)
    print_summary(result)


# ── JSON report ────────────────────────────────────────────

def _serialize_result(r: CheckResult) -> dict[str, Any]:
    return {
        "name": r.name,
        "check_type": r.check_type.value,
        "passed": r.passed,
        "severity": r.severity.value,
        "message": r.message,
        "details": r.details,
        "executed_at": r.executed_at.isoformat(),
        "duration_ms": r.duration_ms,
    }


def _serialize_stage(s: StageResult) -> dict[str, Any]:
    return {
        "stage": s.stage.value,
        "success": s.success,
        "message": s.message,
        "started_at": s.started_at.isoformat(),
        "finished_at": s.finished_at.isoformat() if s.finished_at else None,
        "duration_seconds": s.duration_seconds,
    }


def save_json_report(result: PipelineResult, output_dir: str = "reports") -> Path:
    """Save the full pipeline result as a JSON file."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = out / f"etl_test_report_{ts}.json"
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": result.summary,
        "stages": [_serialize_stage(s) for s in result.stages],
        "check_results": [_serialize_result(r) for r in result.check_results],
    }
    path.write_text(json.dumps(report, indent=2, default=str))
    logger.info("JSON report saved to %s", path)
    return path


# ── HTML report ────────────────────────────────────────────

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>ETL Test Report – {{ generated_at }}</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         margin: 2rem; background: #f8f9fa; color: #212529; }
  h1 { color: #343a40; } h2 { margin-top: 2rem; }
  .summary { display: flex; gap: 1rem; flex-wrap: wrap; margin: 1rem 0; }
  .card { background: #fff; border-radius: 8px; padding: 1rem 1.5rem;
          box-shadow: 0 1px 3px rgba(0,0,0,0.1); min-width: 140px; }
  .card .label { font-size: 0.85rem; color: #6c757d; }
  .card .value { font-size: 1.6rem; font-weight: 700; }
  .pass { color: #28a745; } .fail { color: #dc3545; } .warn { color: #ffc107; }
  table { width: 100%; border-collapse: collapse; margin-top: 1rem; background: #fff;
          border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
  th { background: #343a40; color: #fff; text-align: left; padding: 0.75rem; }
  td { padding: 0.75rem; border-bottom: 1px solid #dee2e6; }
  tr:hover { background: #f1f3f5; }
  .badge { padding: 0.25rem 0.6rem; border-radius: 4px; font-size: 0.8rem; font-weight: 600; }
  .badge-pass { background: #d4edda; color: #155724; }
  .badge-fail { background: #f8d7da; color: #721c24; }
  .badge-warn { background: #fff3cd; color: #856404; }
</style>
</head>
<body>
<h1>ETL Test Report</h1>
<p>Generated: {{ generated_at }}</p>

<div class="summary">
  <div class="card"><div class="label">Stages</div>
    <div class="value">{{ stages_passed }}/{{ stages_total }}</div></div>
  <div class="card"><div class="label">Checks Passed</div>
    <div class="value pass">{{ checks_passed }}</div></div>
  <div class="card"><div class="label">Checks Failed</div>
    <div class="value fail">{{ checks_failed }}</div></div>
  <div class="card"><div class="label">Warnings</div>
    <div class="value warn">{{ checks_warnings }}</div></div>
  <div class="card"><div class="label">Overall</div>
    <div class="value {{ 'pass' if overall else 'fail' }}">{{ 'PASS' if overall else 'FAIL' }}</div></div>
</div>

{% if stages %}
<h2>Pipeline Stages</h2>
<table>
<tr><th>Stage</th><th>Status</th><th>Duration</th><th>Message</th></tr>
{% for s in stages %}
<tr>
  <td>{{ s.stage }}</td>
  <td><span class="badge badge-{{ 'pass' if s.success else 'fail' }}">{{ '✓ PASS' if s.success else '✗ FAIL' }}</span></td>
  <td>{{ "%.1f"|format(s.duration_seconds) }}s</td>
  <td>{{ s.message }}</td>
</tr>
{% endfor %}
</table>
{% endif %}

{% if checks %}
<h2>Data Quality Checks</h2>
<table>
<tr><th>#</th><th>Check</th><th>Type</th><th>Status</th><th>Message</th><th>Duration</th></tr>
{% for c in checks %}
<tr>
  <td>{{ loop.index }}</td>
  <td>{{ c.name }}</td>
  <td>{{ c.check_type }}</td>
  <td><span class="badge badge-{{ c.badge }}">{{ c.status_text }}</span></td>
  <td>{{ c.message }}</td>
  <td>{{ "%.0f"|format(c.duration_ms) }}ms</td>
</tr>
{% endfor %}
</table>
{% endif %}

</body></html>
"""


def save_html_report(result: PipelineResult, output_dir: str = "reports") -> Path:
    """Render and save an HTML report."""
    from jinja2 import Template

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = out / f"etl_test_report_{ts}.html"

    s = result.summary
    checks_data = []
    for r in result.check_results:
        if r.passed:
            badge, status_text = "pass", "✓ PASS"
        elif r.is_warning:
            badge, status_text = "warn", "⚠ WARN"
        else:
            badge, status_text = "fail", "✗ FAIL"
        checks_data.append({
            "name": r.name, "check_type": r.check_type.value,
            "badge": badge, "status_text": status_text,
            "message": r.message, "duration_ms": r.duration_ms,
        })

    template = Template(_HTML_TEMPLATE)
    html = template.render(
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        stages_passed=s["stages_passed"], stages_total=s["stages_executed"],
        checks_passed=s["checks_passed"], checks_failed=s["checks_failed"],
        checks_warnings=s["checks_warnings"], overall=s["overall_success"],
        stages=[{"stage": st.stage.value, "success": st.success,
                 "duration_seconds": st.duration_seconds, "message": st.message}
                for st in result.stages],
        checks=checks_data,
    )
    path.write_text(html)
    logger.info("HTML report saved to %s", path)
    return path
