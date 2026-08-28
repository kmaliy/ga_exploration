"""Command-line interface for the pipeline (Typer).

The CLI is a thin shell over the ``ga_pipeline.pipeline`` library. Airflow
and the tests call the library directly and never go through here.

Examples:
--------
Load one week of both endpoints into BigQuery::

    python data_pipeline.py run --start-date 2016-08-01 --end-date 2016-08-07

Dry-run (no BigQuery; writes JSONL to artifacts/samples/)::

    python data_pipeline.py run --start-date 2016-08-01 --end-date 2016-08-01 --dry-run

Step 5 LLM summary of what's loaded::

    python data_pipeline.py summarize --start-date 2016-08-01 --end-date 2016-08-31

Step 5 NL->SQL question over the loaded tables (guardrailed)::

    python data_pipeline.py ask "Which device had the highest bounce rate in August 2016?"
"""

import json
import logging
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import Annotated

import typer

from ga_pipeline.exceptions import PipelineError

logger = logging.getLogger(__name__)

app = typer.Typer(
    name="data_pipeline",
    help="ETL: assessment GA API -> BigQuery (idempotent, quality-checked).",
    add_completion=False,
    pretty_exceptions_show_locals=False,
)


class Endpoint(StrEnum):
    """Which endpoint(s) a run processes."""

    ALL = "all"
    DAILY_VISITS = "daily-visits"
    GA_SESSIONS = "ga-sessions"


_DATE = {"formats": ["%Y-%m-%d"], "metavar": "YYYY-MM-DD"}

StartDate = Annotated[datetime, typer.Option(..., **_DATE, help="First date to process.")]
EndDate = Annotated[datetime, typer.Option(..., **_DATE, help="Last date to process (inclusive).")]


@app.callback()
def _configure(
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Debug logging.")] = False,
) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@app.command()
def run(
    start_date: StartDate,
    end_date: EndDate,
    endpoint: Annotated[Endpoint, typer.Option(help="Which endpoint(s) to process.")] = Endpoint.ALL,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Skip BigQuery; write transformed JSONL locally.")
    ] = False,
    output_dir: Annotated[
        Path | None,
        typer.Option(help="Target directory for --dry-run output (default: artifacts/samples/)."),
    ] = None,
) -> None:
    """Extract, transform, quality-check, and load a date range."""
    from ga_pipeline.pipeline import run_range

    if end_date < start_date:
        raise typer.BadParameter("--end-date must not be before --start-date")
    with _PipelineErrorsAsExitCode():
        result = run_range(
            start_date=start_date.date(),
            end_date=end_date.date(),
            endpoint=endpoint.value,
            dry_run=dry_run,
            output_dir=output_dir,
        )
    typer.echo(result.as_dict())


@app.command()
def summarize(
    start_date: StartDate,
    end_date: EndDate,
) -> None:
    """Step 5: LLM narrative over loaded aggregates (rule-based without an API key)."""
    from ga_pipeline.llm.summary import summarize_range

    with _PipelineErrorsAsExitCode():
        text = summarize_range(start_date.date(), end_date.date())
    typer.echo(text)


@app.command()
def ask(
    question: Annotated[str, typer.Argument(help="Natural-language question about the loaded tables.")],
    max_scanned_mb: Annotated[
        int, typer.Option(help="Refuse generated SQL estimated to scan more than this many MB.")
    ] = 100,
) -> None:
    """Step 5: NL->SQL with guardrails (read-only, partition-filtered, cost-capped)."""
    from ga_pipeline.llm.nl_sql import ask as ask_question

    with _PipelineErrorsAsExitCode():
        result = ask_question(question, max_scanned_bytes=max_scanned_mb * 1024 * 1024)
    typer.echo(f"-- estimated scan: {result.estimated_bytes / 1e6:.1f} MB")
    typer.echo(result.sql)
    for row in result.rows:
        typer.echo(json.dumps(row, default=str))


class _PipelineErrorsAsExitCode:
    """Translate PipelineError into a clean non-zero exit (no traceback wall)."""

    def __enter__(self) -> None:
        return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        if exc_type is not None and issubclass(exc_type, PipelineError):
            logger.error("Pipeline failed: %s", exc)
            raise typer.Exit(code=1) from exc
        return False


def main() -> None:
    """Entry point for ``data_pipeline.py`` and the ``ga-pipeline`` script."""
    app()


if __name__ == "__main__":
    main()
