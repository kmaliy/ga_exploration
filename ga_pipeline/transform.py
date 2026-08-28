"""Flattening and normalization of raw API records.

All functions are pure (dict in, dict out), so they can be unit-tested
without network or BigQuery access.

Field lookup accepts both camelCase and snake_case names, since the API
follows the GA export format but its exact casing is not guaranteed. Numeric
values may arrive as strings and are coerced. ``detect_schema_drift`` checks
raw records against the expected key set before transformation, so a contract
change raises an error instead of loading NULL columns.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any

from ga_pipeline.exceptions import FatalApiError, SchemaDriftError

logger = logging.getLogger(__name__)

# Keys we require on every raw GA session record (either casing).
REQUIRED_SESSION_KEYS = ("date", "fullVisitorId", "visitId")
# Full expected top-level contract; anything outside it is reported as drift.
EXPECTED_SESSION_KEYS = frozenset(
    {
        "date",
        "fullVisitorId",
        "full_visitor_id",
        "visitId",
        "visit_id",
        "visitNumber",
        "visit_number",
        "visitStartTime",
        "visit_start_time",
        "visitorId",
        "visitor_id",
        "totals",
        "trafficSource",
        "traffic_source",
        "device",
        "geoNetwork",
        "geo_network",
        "channelGrouping",
        "channel_grouping",
        "socialEngagementType",
        "social_engagement_type",
        "hits",
        "hits_sample",
        "customDimensions",
        "custom_dimensions",
        "userId",
        "user_id",
        "clientId",
        "client_id",
    }
)


# --------------------------------------------------------------------------- #
# GA sessions
# --------------------------------------------------------------------------- #


def flatten_session(record: dict[str, Any], loaded_at: datetime) -> dict[str, Any]:
    """Flatten one nested GA session record to the ``ga_sessions_flat`` row shape.

    No hit count is derived from ``hits_sample``, because the API truncates
    that array; ``totals.hits`` has the real number. ``device_category`` is
    derived from ``isMobile`` when the API does not send it.
    """
    totals = _get(record, "totals") or {}
    traffic = _get(record, "trafficSource", "traffic_source") or {}
    device = _get(record, "device") or {}
    geo = _get(record, "geoNetwork", "geo_network") or {}

    session_date = parse_ga_date(_require(record, "date"))
    visit_id = _to_int(_require(record, "visitId", "visit_id"))
    if visit_id is None:
        raise FatalApiError(f"Record for {session_date} has non-numeric visitId")

    return {
        "session_date": session_date.isoformat(),
        "full_visitor_id": str(_require(record, "fullVisitorId", "full_visitor_id")),
        "visit_id": visit_id,
        "visit_number": _to_int(_get(record, "visitNumber", "visit_number")),
        "visit_start_time": _epoch_to_iso(_get(record, "visitStartTime", "visit_start_time")),
        "channel_grouping": _to_str(_get(record, "channelGrouping", "channel_grouping")),
        "totals_visits": _to_int(_get(totals, "visits")),
        "totals_hits": _to_int(_get(totals, "hits")),
        "totals_pageviews": _to_int(_get(totals, "pageviews")),
        "totals_bounces": _to_int(_get(totals, "bounces")),
        "totals_new_visits": _to_int(_get(totals, "newVisits", "new_visits")),
        "totals_time_on_site_seconds": _to_int(_get(totals, "timeOnSite", "time_on_site")),
        "totals_transactions": _to_int(_get(totals, "transactions")),
        "totals_transaction_revenue_micros": _to_int(
            _get(totals, "transactionRevenue", "transaction_revenue")
        ),
        "traffic_source": _to_str(_get(traffic, "source")),
        "traffic_medium": _to_str(_get(traffic, "medium")),
        "traffic_campaign": _to_str(_get(traffic, "campaign")),
        "traffic_keyword": _to_str(_get(traffic, "keyword")),
        "traffic_referral_path": _to_str(_get(traffic, "referralPath", "referral_path")),
        "traffic_is_true_direct": _to_bool(_get(traffic, "isTrueDirect", "is_true_direct")),
        "device_category": _device_category(device),
        "device_browser": _to_str(_get(device, "browser")),
        "device_operating_system": _to_str(_get(device, "operatingSystem", "operating_system")),
        "device_is_mobile": _to_bool(_get(device, "isMobile", "is_mobile")),
        "geo_continent": _to_str(_get(geo, "continent")),
        "geo_sub_continent": _to_str(_get(geo, "subContinent", "sub_continent")),
        "geo_country": _to_str(_get(geo, "country")),
        "geo_region": _to_str(_get(geo, "region")),
        "geo_metro": _to_str(_get(geo, "metro")),
        "geo_city": _to_str(_get(geo, "city")),
        "geo_network_domain": _to_str(_get(geo, "networkDomain", "network_domain")),
        "loaded_at": loaded_at.isoformat(),
    }


def session_key(row: dict[str, Any]) -> tuple:
    """Natural key defining the duplicate grain for a flattened session row."""
    return (row["full_visitor_id"], row["visit_id"], row.get("visit_start_time"))


def dedupe_sessions(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Drop exact-grain duplicates, keeping the first occurrence.

    Returns ``(unique_rows, dropped_count)``; the caller logs and asserts on
    the dropped count as part of data quality.
    """
    seen: set[tuple] = set()
    unique: list[dict[str, Any]] = []
    for row in rows:
        key = session_key(row)
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    dropped = len(rows) - len(unique)
    if dropped:
        logger.warning("Deduplication dropped %d duplicate session row(s)", dropped)
    return unique, dropped


def detect_schema_drift(raw_records: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Validate raw records against the expected top-level contract.

    Raises ``SchemaDriftError`` when required keys are missing; returns a
    report ``{"unexpected_keys": [...]}`` for new-but-nonbreaking keys, which
    the caller logs as a warning (forward-compatible by design).
    """
    unexpected: set[str] = set()
    for record in raw_records:
        keys = set(record)
        for required in REQUIRED_SESSION_KEYS:
            if _get(record, required, _snake(required)) is None:
                raise SchemaDriftError(
                    f"Required key '{required}' missing from API record; got keys: {sorted(keys)}"
                )
        unexpected |= keys - EXPECTED_SESSION_KEYS
    if unexpected:
        logger.warning("Schema drift: unexpected top-level keys %s", sorted(unexpected))
    return {"unexpected_keys": sorted(unexpected)}


# --------------------------------------------------------------------------- #
# Daily visits
# --------------------------------------------------------------------------- #


def transform_daily_visit(record: dict[str, Any], loaded_at: datetime) -> dict[str, Any]:
    """Normalize one raw daily-visits record to the ``daily_visits`` row shape.

    The API sends ``visit_date`` and ``total_visits``; the field names from
    the task description (``date``, ``visits``) are accepted as fallbacks.
    """
    raw_date = _require(record, "visit_date", "date")
    visits = _to_int(_require(record, "total_visits", "visits", "visit_count"))
    if visits is None:
        raise FatalApiError(f"daily-visits record for {raw_date} has non-numeric visits")
    return {
        "visit_date": parse_ga_date(raw_date).isoformat(),
        "visits": visits,
        "loaded_at": loaded_at.isoformat(),
    }


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def parse_ga_date(value: Any) -> date:
    """Parse either YYYYMMDD (GA export style) or YYYY-MM-DD."""
    text = str(value).strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise FatalApiError(f"Unparseable date value: {value!r}")


def utcnow() -> datetime:
    """Timezone-aware current UTC time (single source for loaded_at)."""
    return datetime.now(tz=UTC)


def _get(container: Any, *names: str) -> Any:
    if not isinstance(container, dict):
        return None
    for name in names:
        if name in container:
            return container[name]
    return None


def _require(record: dict[str, Any], *names: str) -> Any:
    value = _get(record, *names)
    if value is None:
        raise FatalApiError(f"Record missing required field {names[0]!r}: keys={sorted(record)}")
    return value


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


# The demo dataset uses this placeholder string for fields it does not
# provide; it is treated as NULL on ingest.
_DEMO_SENTINEL = "not available in demo dataset"


def _to_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    if text == _DEMO_SENTINEL:
        return None
    return text


def _device_category(device: dict[str, Any]) -> str | None:
    """Use ``deviceCategory`` if present, otherwise derive it from ``isMobile``.

    The API omits ``deviceCategory`` even though it supports it as a filter.
    The fallback maps isMobile=True to "mobile" and False to "desktop", which
    cannot distinguish tablets from phones.
    """
    category = _to_str(_get(device, "deviceCategory", "device_category"))
    if category is not None:
        return category
    is_mobile = _to_bool(_get(device, "isMobile", "is_mobile"))
    if is_mobile is None:
        return None
    return "mobile" if is_mobile else "desktop"


def _epoch_to_iso(value: Any) -> str | None:
    seconds = _to_int(value)
    if seconds is None:
        return None
    return datetime.fromtimestamp(seconds, tz=UTC).isoformat()


def _snake(name: str) -> str:
    out = []
    for ch in name:
        if ch.isupper():
            out.append("_")
            out.append(ch.lower())
        else:
            out.append(ch)
    return "".join(out)
