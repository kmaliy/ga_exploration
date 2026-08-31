"""Concrete data-quality checks.

Two layers, both run before the table is trusted:

1. Pre-load checks run in memory on the transformed rows and catch bad
   batches before they reach BigQuery.
2. Post-load checks run as SQL against the destination and verify what
   actually landed, including reconciliation between the two endpoints.

Violations are collected into a single ``QualityReport`` so one failure shows
the full picture instead of one violation per rerun.
"""

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from google.cloud import bigquery

from ga_pipeline.exceptions import DataQualityError
from ga_pipeline.load.bq_loader import BigQueryLoader
from ga_pipeline.load.schemas import DAILY_VISITS_TABLE, GA_SESSIONS_TABLE
from ga_pipeline.transform import session_key

logger = logging.getLogger(__name__)

# Two endpoints are expected to return same results.
# Raise this only to reflect a known, explained discrepancy.
RECONCILIATION_TOLERANCE_PCT = 0.0

REQUIRED_SESSION_FIELDS = ("session_date", "full_visitor_id", "visit_id")
NON_NEGATIVE_SESSION_FIELDS = (
    "totals_visits",
    "totals_hits",
    "totals_pageviews",
    "totals_time_on_site_seconds",
    "totals_transactions",
)


@dataclass
class QualityReport:
    """Aggregated outcome of a batch of checks."""

    context: str
    violations: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    checks_run: int = 0

    def fail(self, message: str) -> None:
        """Record a violation (the batch must not be trusted)."""
        self.violations.append(message)

    def warn(self, message: str) -> None:
        """Record a non-blocking warning and log it immediately."""
        self.warnings.append(message)
        logger.warning("[DQ:%s] %s", self.context, message)

    def raise_if_failed(self) -> None:
        """Raise DataQualityError with all violations, or log success."""
        if self.violations:
            details = "; ".join(self.violations)
            raise DataQualityError(
                f"{len(self.violations)} data-quality violation(s) [{self.context}]: {details}"
            )
        logger.info("[DQ:%s] %d check(s) passed", self.context, self.checks_run)


# Preload checks (in memory)


def check_sessions_pre_load(
    rows: list[dict[str, Any]], expected_date: date, dropped_duplicates: int
) -> QualityReport:
    """Validate a transformed sessions batch before it may be loaded."""
    report = QualityReport(context=f"sessions/{expected_date}/pre-load")

    _check_not_empty(report, rows, f"ga_sessions for {expected_date}")
    _check_required_fields(report, rows, REQUIRED_SESSION_FIELDS)
    _check_single_date(report, rows, "session_date", expected_date)
    _check_unique_grain(report, rows)
    _check_non_negative(report, rows, NON_NEGATIVE_SESSION_FIELDS)
    if dropped_duplicates:
        report.warn(f"{dropped_duplicates} duplicate row(s) dropped during dedupe")
    return report


def check_daily_visits_pre_load(
    rows: list[dict[str, Any]], start_date: date, end_date: date
) -> QualityReport:
    """Validate a transformed daily-visits batch before it may be loaded."""
    report = QualityReport(context=f"daily_visits/{start_date}..{end_date}/pre-load")

    _check_not_empty(report, rows, "daily_visits batch")
    _check_required_fields(report, rows, ("visit_date", "visits"))
    report.checks_run += 1
    seen_dates: set[str] = set()
    for row in rows:
        if row["visit_date"] in seen_dates:
            report.fail(f"duplicate visit_date in batch: {row['visit_date']}")
        seen_dates.add(row["visit_date"])
        if not (start_date.isoformat() <= row["visit_date"] <= end_date.isoformat()):
            report.fail(f"visit_date {row['visit_date']} outside requested window")
        if row["visits"] < 0:
            report.fail(f"negative visits for {row['visit_date']}: {row['visits']}")
    return report


# Post-load checks (SQL against BigQuery)


def check_sessions_post_load(
    loader: BigQueryLoader, partition_date: date, expected_count: int
) -> QualityReport:
    """Prove what actually landed in the destination partition."""
    report = QualityReport(context=f"sessions/{partition_date}/post-load")
    table = loader.table_id(GA_SESSIONS_TABLE)
    params = [bigquery.ScalarQueryParameter("d", "DATE", partition_date)]

    # 1. Loaded row count matches the deduped batch exactly.
    rows = loader.query_rows(
        f"SELECT COUNT(*) AS n FROM `{table}` WHERE session_date = @d",  # noqa: S608 - table id from config
        params,
    )
    report.checks_run += 1
    actual = rows[0]["n"] if rows else 0
    if actual != expected_count:
        report.fail(f"loaded count {actual} != expected {expected_count}")

    # 2. Grain uniqueness holds in the destination.
    rows = loader.query_rows(
        f"""
        SELECT COUNT(*) AS dupes FROM (
          SELECT full_visitor_id, visit_id, visit_start_time
          FROM `{table}`
          WHERE session_date = @d
          GROUP BY 1, 2, 3
          HAVING COUNT(*) > 1
        )
        """,  # noqa: S608 - table ids from config; values parameterized
        params,
    )
    report.checks_run += 1
    if rows and rows[0]["dupes"]:
        report.fail(f"{rows[0]['dupes']} duplicate session grain(s) in destination")

    # 3. No NULLs in required analytic fields.
    rows = loader.query_rows(
        f"""
        SELECT
          COUNTIF(full_visitor_id IS NULL) AS null_visitor,
          COUNTIF(visit_id IS NULL) AS null_visit
        FROM `{table}` WHERE session_date = @d
        """,  # noqa: S608 - table ids from config; values parameterized
        params,
    )
    report.checks_run += 1
    if rows and (rows[0]["null_visitor"] or rows[0]["null_visit"]):
        report.fail(f"NULL keys in destination: {rows[0]}")

    return report


def check_reconciliation(
    loader: BigQueryLoader,
    partition_date: date,
    tolerance_pct: float = RECONCILIATION_TOLERANCE_PCT,
) -> QualityReport:
    """Cross-endpoint sanity: daily_visits.visits vs sessions on the same calendar day.

    The two endpoints date a session differently. ``/ga-sessions-data`` shards on
    GA's ``date``, which is the property's local day (UTC-7 for this property:
    every session_date spans 07:00Z to 07:00Z the next day). ``/daily-visits``
    counts by UTC date. So a session at 20:00 local on the 1st is 03:00 UTC on
    the 2nd, and the two sources file it under different dates.

    Comparing sessions by ``session_date`` against ``daily_visits`` therefore
    compares two different days, and the gap is the day-over-day change in
    evening traffic — routinely 5-30%, which made the old check warn on almost
    every run. Instead we count sessions by the UTC date of ``visit_start_time``,
    which is the same day ``/daily-visits`` is counting. On verified data the two
    then agree exactly, so drift is a real signal: a missed page, a partial load,
    a double load.

    Sessions belonging to UTC day D live in the ``D-1`` and ``D`` partitions, so
    both are read; the ``session_date`` predicate keeps this partition-pruned.

    Fails only when one side is entirely missing (a broken or partial load).
    Drift is reported as a warning, so the run continues with the data flagged
    rather than blocked.
    """
    report = QualityReport(context=f"reconciliation/{partition_date}")
    daily = loader.table_id(DAILY_VISITS_TABLE)
    sessions = loader.table_id(GA_SESSIONS_TABLE)
    params = [bigquery.ScalarQueryParameter("d", "DATE", partition_date)]

    rows = loader.query_rows(
        f"""
        SELECT
          (SELECT visits FROM `{daily}` WHERE visit_date = @d) AS daily_visits,
          (SELECT COALESCE(SUM(totals_visits), COUNT(*))
             FROM `{sessions}`
            WHERE session_date BETWEEN DATE_SUB(@d, INTERVAL 1 DAY) AND @d
              AND DATE(visit_start_time) = @d)             AS session_visits,
          (SELECT COUNT(*) FROM `{sessions}`
            WHERE session_date = DATE_SUB(@d, INTERVAL 1 DAY)) AS prior_partition_rows,
          (SELECT COUNT(*) FROM `{sessions}`
            WHERE session_date BETWEEN DATE_SUB(@d, INTERVAL 1 DAY) AND @d
              AND visit_start_time IS NULL)                AS undated_rows
        """,  # noqa: S608 - table ids from config; values parameterized
        params,
    )
    report.checks_run += 1
    if not rows or rows[0]["daily_visits"] is None:
        report.warn(f"no daily_visits row for {partition_date}; reconciliation skipped")
        return report

    daily_count = rows[0]["daily_visits"]
    session_count = rows[0]["session_visits"] or 0
    prior_rows = rows[0].get("prior_partition_rows") or 0
    undated = rows[0].get("undated_rows") or 0

    # Hard failure only when one side is missing entirely, which means a
    # broken or partial load rather than a counting difference.
    if daily_count == 0 and session_count != 0:
        report.fail(f"daily_visits=0 but {session_count} session visit(s) loaded")
        return report
    if daily_count > 0 and session_count == 0:
        report.fail(f"daily_visits={daily_count} but zero sessions loaded")
        return report
    if daily_count == 0:
        return report

    drift_pct = abs(daily_count - session_count) / daily_count * 100
    if drift_pct <= tolerance_pct:
        return report

    detail = f"daily_visits={daily_count}, sessions={session_count}, drift {drift_pct:.1f}%"
    if prior_rows == 0:
        report.warn(
            f"{detail}; the {partition_date - timedelta(days=1)} partition is empty, so "
            "sessions that belong to this UTC day but are dated to the previous local day "
            "are not loaded yet — reconcile again once that partition is backfilled"
        )
    elif undated:
        report.warn(
            f"{detail}; {undated} row(s) have a NULL visit_start_time and cannot be "
            "assigned to a UTC day, which accounts for at least part of the gap"
        )
    else:
        report.warn(
            f"{detail}; both partitions are loaded and every row is timestamped, so this "
            "is a genuine discrepancy — check for a missed page or a partial load"
        )
    return report


def _check_not_empty(report: QualityReport, rows: list[dict[str, Any]], what: str) -> None:
    report.checks_run += 1
    if not rows:
        report.fail(f"empty batch: {what}")


def _check_required_fields(
    report: QualityReport, rows: list[dict[str, Any]], fields: tuple[str, ...]
) -> None:
    report.checks_run += 1
    for index, row in enumerate(rows):
        for field_name in fields:
            if row.get(field_name) in (None, ""):
                report.fail(f"row {index}: required field '{field_name}' is null/empty")
                break


def _check_single_date(
    report: QualityReport, rows: list[dict[str, Any]], field_name: str, expected: date
) -> None:
    report.checks_run += 1
    bad = {row[field_name] for row in rows if row.get(field_name) != expected.isoformat()}
    if bad:
        report.fail(f"rows outside partition date {expected}: {sorted(bad)[:5]}")


def _check_unique_grain(report: QualityReport, rows: list[dict[str, Any]]) -> None:
    report.checks_run += 1
    seen: set[tuple] = set()
    dupes = 0
    for row in rows:
        key = session_key(row)
        if key in seen:
            dupes += 1
        seen.add(key)
    if dupes:
        report.fail(f"{dupes} duplicate session grain(s) after dedupe: dedupe is broken")


def _check_non_negative(report: QualityReport, rows: list[dict[str, Any]], fields: tuple[str, ...]) -> None:
    report.checks_run += 1
    for index, row in enumerate(rows):
        for field_name in fields:
            value = row.get(field_name)
            if value is not None and value < 0:
                report.fail(f"row {index}: {field_name} is negative ({value})")
