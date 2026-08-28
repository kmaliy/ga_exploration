"""Normalization of raw /daily-visits records.

Pure functions (dict in, dict out), unit-testable without network access.
"""

from datetime import datetime
from typing import Any

from ga_pipeline.exceptions import FatalApiError
from ga_pipeline.transform.coercion import parse_ga_date, require_field, to_int


def transform_daily_visit(record: dict[str, Any], loaded_at: datetime) -> dict[str, Any]:
    """Normalize one raw daily-visits record to the ``daily_visits`` row shape.

    The API sends ``visit_date`` and ``total_visits``; the field names from
    the task description (``date``, ``visits``) are accepted as fallbacks.
    """
    raw_date = require_field(record, "visit_date", "date")
    visits = to_int(require_field(record, "total_visits", "visits", "visit_count"))
    if visits is None:
        raise FatalApiError(f"daily-visits record for {raw_date} has non-numeric visits")
    return {
        "visit_date": parse_ga_date(raw_date).isoformat(),
        "visits": visits,
        "loaded_at": loaded_at.isoformat(),
    }
