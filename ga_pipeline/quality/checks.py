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
from datetime import date
from typing import Any

from google.cloud import bigquery

from ga_pipeline.exceptions import DataQualityError
from ga_pipeline.load.bq_loader import BigQueryLoader
from ga_pipeline.load.schemas import DAILY_VISITS_TABLE, GA_SESSIONS_TABLE
from ga_pipeline.transform import session_key

logger = logging.getLogger(__name__)

# Drift between daily_visits.visits and the per-date session visit sum above
# which a warning is logged. The endpoints count visits differently, so this
# is a reporting threshold, not a failure threshold.
RECONCILIATION_TOLERANCE_PCT = 5.0

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
        for warning in self.warnings:
            logger.warning("[DQ:%s] %s", self.context, warning)
        if self.violations:
            details = "; ".join(self.violations)
            raise DataQualityError(
                f"{len(self.violations)} data-quality violation(s) [{self.context}]: {details}"
            )
        logger.info("[DQ:%s] %d check(s) passed", self.context, self.checks_run)


# --------------------------------------------------------------------------- #
# Pre-load checks (in memory)
# --------------------------------------------------------------------------- #


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


# --------------------------------------------------------------------------- #
# Post-load checks (SQL against BigQuery)
# --------------------------------------------------------------------------- #


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
    """Cross-endpoint sanity: daily_visits.visits vs session visits per date.

    Fails only when one side is entirely missing (broken/partial load);
    metric drift between the endpoints is recorded as a warning; see
    docs/01-api-exploration.md for details.
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
             FROM `{sessions}` WHERE session_date = @d) AS session_visits
        """,  # noqa: S608 - table ids from config; values parameterized
        params,
    )
    report.checks_run += 1
    if not rows or rows[0]["daily_visits"] is None:
        report.warn(f"no daily_visits row for {partition_date}; reconciliation skipped")
        return report

    daily_count = rows[0]["daily_visits"]
    session_count = rows[0]["session_visits"] or 0

    # Hard failure only when one side is missing entirely, which means a
    # broken or partial load rather than a metric-definition mismatch.
    if daily_count == 0 and session_count != 0:
        report.fail(f"daily_visits=0 but {session_count} session visit(s) loaded")
        return report
    if daily_count > 0 and session_count == 0:
        report.fail(f"daily_visits={daily_count} but zero sessions loaded")
        return report
    if daily_count == 0:
        return report

    # The endpoints date a session differently: /ga-sessions-data shards on
    # GA's `date` (the property's local timezone, Pacific) while /daily-visits
    # counts by UTC date, so evening sessions land in the next UTC day. On a
    # normal day the spill in and out cancels out to ~1%; on 2016-08-01, the
    # first day of the dataset, nothing spills in and the gap is 32%
    # (1711 sessions, 415 of them starting after midnight UTC, vs 1296 daily
    # visits: 1711 - 415 == 1296 exactly). Drift is therefore a warning.
    drift_pct = abs(daily_count - session_count) / daily_count * 100
    if drift_pct > tolerance_pct:
        report.warn(
            f"visits drift {drift_pct:.1f}% exceeds {tolerance_pct}% "
            f"(daily_visits={daily_count}, sessions={session_count}); expected, "
            "the endpoints count visits differently, see docs/01-api-exploration.md"
        )
    elif drift_pct > 0:
        report.warn(
            f"visits drift {drift_pct:.1f}% within tolerance "
            f"(daily_visits={daily_count}, sessions={session_count})"
        )
    return report


# --------------------------------------------------------------------------- #
# Shared primitives
# --------------------------------------------------------------------------- #


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
