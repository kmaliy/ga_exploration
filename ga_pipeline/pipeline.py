"""End-to-end ETL orchestration.

Each public function is one rerunnable unit of work:

* ``run_ga_sessions_for_date``: extract, drift check, flatten, dedupe,
  pre-load checks, partition replacement, post-load checks, for one date.
* ``run_daily_visits``: extract, transform, pre-load checks, MERGE, for a
  date range.
* ``run_range``: the full pipeline over a range; this is what the CLI and
  the Airflow DAG call.

Dry-run mode stops after the pre-load quality checks and writes the
transformed rows to local files instead of BigQuery. Useful for testing and
for producing sample outputs without cloud access.
"""

import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from ga_pipeline import quality, transform
from ga_pipeline.config import (
    DAILY_VISITS_RANGE,
    GA_SESSIONS_RANGE,
    ApiSettings,
    BigQuerySettings,
)
from ga_pipeline.extract.api_client import AssessmentApiClient
from ga_pipeline.load.bq_loader import BigQueryLoader

logger = logging.getLogger(__name__)


@dataclass
class RunResult:
    """Outcome summary for one pipeline invocation (surfaced in logs/Airflow)."""

    sessions_loaded: dict[str, int] = field(default_factory=dict)
    daily_visits_loaded: int = 0
    duplicates_dropped: int = 0
    drift_warnings: list[str] = field(default_factory=list)
    dry_run: bool = False

    def as_dict(self) -> dict[str, Any]:
        """Serialize the summary for logs / Airflow XCom (metadata only)."""
        return {
            "sessions_loaded": self.sessions_loaded,
            "daily_visits_loaded": self.daily_visits_loaded,
            "duplicates_dropped": self.duplicates_dropped,
            "drift_warnings": self.drift_warnings,
            "dry_run": self.dry_run,
        }


def run_ga_sessions_for_date(
    day: date,
    client: AssessmentApiClient,
    loader: BigQueryLoader | None,
    output_dir: Path | None = None,
) -> RunResult:
    """ETL one day of GA sessions. Idempotent: reruns replace the partition."""
    result = RunResult(dry_run=loader is None)
    loaded_at = transform.utcnow()

    raw_records = list(client.iter_ga_sessions(day.strftime("%Y%m%d")))
    logger.info("Extracted %d raw session record(s) for %s", len(raw_records), day)

    drift = transform.detect_schema_drift(raw_records)
    result.drift_warnings = drift["unexpected_keys"]

    rows = [transform.flatten_session(record, loaded_at) for record in raw_records]
    rows, dropped = transform.dedupe_sessions(rows)
    result.duplicates_dropped = dropped

    quality.check_sessions_pre_load(rows, day, dropped).raise_if_failed()

    if loader is None:
        _write_local(rows, output_dir, f"ga_sessions_flat_{day.isoformat()}")
        result.sessions_loaded[day.isoformat()] = len(rows)
        return result

    loader.replace_sessions_partition(rows, day)
    quality.check_sessions_post_load(loader, day, expected_count=len(rows)).raise_if_failed()
    result.sessions_loaded[day.isoformat()] = len(rows)
    return result


def run_daily_visits(
    start_date: date,
    end_date: date,
    client: AssessmentApiClient,
    loader: BigQueryLoader | None,
    output_dir: Path | None = None,
) -> RunResult:
    """ETL daily visits for a range. Idempotent: reruns MERGE on visit_date."""
    result = RunResult(dry_run=loader is None)
    loaded_at = transform.utcnow()

    raw_records = list(client.iter_daily_visits(start_date.isoformat(), end_date.isoformat()))
    logger.info(
        "Extracted %d raw daily-visit record(s) for %s..%s",
        len(raw_records),
        start_date,
        end_date,
    )

    rows = [transform.transform_daily_visit(record, loaded_at) for record in raw_records]
    quality.check_daily_visits_pre_load(rows, start_date, end_date).raise_if_failed()

    if loader is None:
        _write_local(rows, output_dir, f"daily_visits_{start_date}_{end_date}")
        result.daily_visits_loaded = len(rows)
        return result

    result.daily_visits_loaded = loader.merge_daily_visits(rows)
    return result


def run_reconciliation(start_date: date, end_date: date, loader: BigQueryLoader | None = None) -> None:
    """Cross-endpoint reconciliation for each date in the range (post-load)."""
    loader = loader or BigQueryLoader(BigQuerySettings.from_env())
    for day in _date_range(start_date, end_date):
        quality.check_reconciliation(loader, day).raise_if_failed()


def run_range(
    start_date: date,
    end_date: date,
    *,
    endpoint: str = "all",
    dry_run: bool = False,
    output_dir: Path | None = None,
    client: AssessmentApiClient | None = None,
    loader: BigQueryLoader | None = None,
    reconcile: bool = True,
) -> RunResult:
    """Run the full pipeline for [start_date, end_date] inclusive."""
    if end_date < start_date:
        raise ValueError(f"end_date {end_date} is before start_date {start_date}")

    client = client or AssessmentApiClient(ApiSettings.from_env())
    if not dry_run and loader is None:
        loader = BigQueryLoader(BigQuerySettings.from_env())
        loader.ensure_dataset()
        loader.ensure_tables()
    if dry_run:
        loader = None

    combined = RunResult(dry_run=dry_run)

    if endpoint in ("all", "daily-visits"):
        window = _clamp_window(start_date, end_date, DAILY_VISITS_RANGE, "daily-visits")
        if window:
            partial = run_daily_visits(*window, client, loader, output_dir)
            combined.daily_visits_loaded = partial.daily_visits_loaded

    if endpoint in ("all", "ga-sessions"):
        window = _clamp_window(start_date, end_date, GA_SESSIONS_RANGE, "ga-sessions")
        for day in _date_range(*window) if window else ():
            partial = run_ga_sessions_for_date(day, client, loader, output_dir)
            combined.sessions_loaded.update(partial.sessions_loaded)
            combined.duplicates_dropped += partial.duplicates_dropped
            combined.drift_warnings = sorted(set(combined.drift_warnings) | set(partial.drift_warnings))
            if loader is not None and reconcile:
                quality.check_reconciliation(loader, day).raise_if_failed()

    logger.info("Pipeline run complete: %s", combined.as_dict())
    return combined


def _clamp_window(
    start: date, end: date, valid_range: tuple[date, date], what: str
) -> tuple[date, date] | None:
    """Clamp a requested window to the endpoint's available date range.

    The API returns HTTP 500 for out-of-range dates, so such requests would
    only burn retries. Returns None (with a warning) if the window does not
    overlap the available range at all.
    """
    lo, hi = valid_range
    clamped_start, clamped_end = max(start, lo), min(end, hi)
    if clamped_start > clamped_end:
        logger.warning(
            "%s: window %s..%s is outside the available range %s..%s, skipping",
            what,
            start,
            end,
            lo,
            hi,
        )
        return None
    if (clamped_start, clamped_end) != (start, end):
        logger.warning(
            "%s: window %s..%s clamped to available range -> %s..%s",
            what,
            start,
            end,
            clamped_start,
            clamped_end,
        )
    return clamped_start, clamped_end


def _date_range(start: date, end: date) -> Iterator[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _write_local(rows: list[dict[str, Any]], output_dir: Path | None, stem: str) -> None:
    target_dir = output_dir or Path("artifacts/samples")
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{stem}.jsonl"
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    logger.info("Dry-run: wrote %d row(s) to %s", len(rows), path)
