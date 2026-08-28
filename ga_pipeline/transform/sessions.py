"""Flattening, deduplication and drift detection for GA session records.

All functions are pure (dict in, dict out), so they can be unit-tested without
network or BigQuery access. ``detect_schema_drift`` checks raw records against
the expected key set before transformation, so a contract change raises an
error instead of quietly loading NULL columns.
"""

import logging
from datetime import datetime
from typing import Any

from ga_pipeline.exceptions import FatalApiError, SchemaDriftError
from ga_pipeline.transform.coercion import (
    epoch_to_iso,
    get_field,
    parse_ga_date,
    require_field,
    snake_case,
    to_bool,
    to_int,
    to_str,
)

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


def flatten_session(record: dict[str, Any], loaded_at: datetime) -> dict[str, Any]:
    """Flatten one nested GA session record to the ``ga_sessions_flat`` row shape.

    No hit count is derived from ``hits_sample``, because the API truncates
    that array; ``totals.hits`` has the real number. ``device_category`` is
    derived from ``isMobile`` when the API does not send it.
    """
    totals = get_field(record, "totals") or {}
    traffic = get_field(record, "trafficSource", "traffic_source") or {}
    device = get_field(record, "device") or {}
    geo = get_field(record, "geoNetwork", "geo_network") or {}

    session_date = parse_ga_date(require_field(record, "date"))
    visit_id = to_int(require_field(record, "visitId", "visit_id"))
    if visit_id is None:
        raise FatalApiError(f"Record for {session_date} has non-numeric visitId")

    return {
        "session_date": session_date.isoformat(),
        "full_visitor_id": str(require_field(record, "fullVisitorId", "full_visitor_id")),
        "visit_id": visit_id,
        "visit_number": to_int(get_field(record, "visitNumber", "visit_number")),
        "visit_start_time": epoch_to_iso(get_field(record, "visitStartTime", "visit_start_time")),
        "channel_grouping": to_str(get_field(record, "channelGrouping", "channel_grouping")),
        "totals_visits": to_int(get_field(totals, "visits")),
        "totals_hits": to_int(get_field(totals, "hits")),
        "totals_pageviews": to_int(get_field(totals, "pageviews")),
        "totals_bounces": to_int(get_field(totals, "bounces")),
        "totals_new_visits": to_int(get_field(totals, "newVisits", "new_visits")),
        "totals_time_on_site_seconds": to_int(get_field(totals, "timeOnSite", "time_on_site")),
        "totals_transactions": to_int(get_field(totals, "transactions")),
        "totals_transaction_revenue_micros": to_int(
            get_field(totals, "transactionRevenue", "transaction_revenue")
        ),
        "traffic_source": to_str(get_field(traffic, "source")),
        "traffic_medium": to_str(get_field(traffic, "medium")),
        "traffic_campaign": to_str(get_field(traffic, "campaign")),
        "traffic_keyword": to_str(get_field(traffic, "keyword")),
        "traffic_referral_path": to_str(get_field(traffic, "referralPath", "referral_path")),
        "traffic_is_true_direct": to_bool(get_field(traffic, "isTrueDirect", "is_true_direct")),
        "device_category": _device_category(device),
        "device_browser": to_str(get_field(device, "browser")),
        "device_operating_system": to_str(get_field(device, "operatingSystem", "operating_system")),
        "device_is_mobile": to_bool(get_field(device, "isMobile", "is_mobile")),
        "geo_continent": to_str(get_field(geo, "continent")),
        "geo_sub_continent": to_str(get_field(geo, "subContinent", "sub_continent")),
        "geo_country": to_str(get_field(geo, "country")),
        "geo_region": to_str(get_field(geo, "region")),
        "geo_metro": to_str(get_field(geo, "metro")),
        "geo_city": to_str(get_field(geo, "city")),
        "geo_network_domain": to_str(get_field(geo, "networkDomain", "network_domain")),
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
            if get_field(record, required, snake_case(required)) is None:
                raise SchemaDriftError(
                    f"Required key '{required}' missing from API record; got keys: {sorted(keys)}"
                )
        unexpected |= keys - EXPECTED_SESSION_KEYS
    if unexpected:
        logger.warning("Schema drift: unexpected top-level keys %s", sorted(unexpected))
    return {"unexpected_keys": sorted(unexpected)}


def _device_category(device: dict[str, Any]) -> str | None:
    """Use ``deviceCategory`` if present, otherwise derive it from ``isMobile``.

    The API omits ``deviceCategory`` even though it supports it as a filter.
    The fallback maps isMobile=True to "mobile" and False to "desktop", which
    cannot distinguish tablets from phones.
    """
    category = to_str(get_field(device, "deviceCategory", "device_category"))
    if category is not None:
        return category
    is_mobile = to_bool(get_field(device, "isMobile", "is_mobile"))
    if is_mobile is None:
        return None
    return "mobile" if is_mobile else "desktop"
