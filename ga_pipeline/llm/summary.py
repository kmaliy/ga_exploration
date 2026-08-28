"""LLM-assisted traffic summary.

Sends only aggregates to the LLM, never row-level data or identifiers.
Without ``ANTHROPIC_API_KEY`` a plain rule-based summary is returned instead,
so the pipeline works without the LLM. Anomaly detection itself is plain
Python (day-over-day change against a threshold); the LLM only writes the
narrative around it.
"""

import itertools
import json
import logging
from datetime import date
from typing import Any

from google.cloud import bigquery

from ga_pipeline.config import BigQuerySettings
from ga_pipeline.llm import client as llm_client
from ga_pipeline.load.bq_loader import BigQueryLoader
from ga_pipeline.load.schemas import DAILY_VISITS_TABLE, GA_SESSIONS_TABLE

logger = logging.getLogger(__name__)

ANOMALY_THRESHOLD_PCT = 30.0  # day-over-day swing considered noteworthy


def summarize_range(start_date: date, end_date: date, loader: BigQueryLoader | None = None) -> str:
    """Return a plain-language summary of the loaded data for a date range."""
    loader = loader or BigQueryLoader(BigQuerySettings.from_env())
    aggregates = _collect_aggregates(loader, start_date, end_date)
    anomalies = detect_anomalies(aggregates["daily"])

    narrative = llm_client.try_complete(_prompt(start_date, end_date, aggregates, anomalies))
    return narrative or _rule_based_summary(start_date, end_date, aggregates, anomalies)


# --------------------------------------------------------------------------- #
# Aggregation (partition-pruned SQL)
# --------------------------------------------------------------------------- #


def _collect_aggregates(loader: BigQueryLoader, start_date: date, end_date: date) -> dict[str, Any]:
    daily_table = loader.table_id(DAILY_VISITS_TABLE)
    sessions_table = loader.table_id(GA_SESSIONS_TABLE)
    params = [
        bigquery.ScalarQueryParameter("start", "DATE", start_date),
        bigquery.ScalarQueryParameter("end", "DATE", end_date),
    ]

    daily = loader.query_rows(
        f"""
        SELECT visit_date, visits
        FROM `{daily_table}`
        WHERE visit_date BETWEEN @start AND @end
        ORDER BY visit_date
        """,  # noqa: S608 - table ids from config; values parameterized
        params,
    )
    by_device = loader.query_rows(
        f"""
        SELECT device_category, COUNT(*) AS sessions,
               ROUND(SAFE_DIVIDE(SUM(totals_bounces), COUNT(*)) * 100, 1) AS bounce_rate_pct
        FROM `{sessions_table}`
        WHERE session_date BETWEEN @start AND @end
        GROUP BY device_category
        ORDER BY sessions DESC
        """,  # noqa: S608 - table ids from config; values parameterized
        params,
    )
    top_countries = loader.query_rows(
        f"""
        SELECT geo_country, COUNT(*) AS sessions
        FROM `{sessions_table}`
        WHERE session_date BETWEEN @start AND @end
        GROUP BY geo_country
        ORDER BY sessions DESC
        LIMIT 5
        """,  # noqa: S608 - table ids from config; values parameterized
        params,
    )
    return {
        "daily": [{"date": str(row["visit_date"]), "visits": row["visits"]} for row in daily],
        "by_device": by_device,
        "top_countries": top_countries,
    }


def detect_anomalies(daily: list[dict[str, Any]]) -> list[str]:
    """Flag day-over-day swings beyond the threshold (deterministic, no LLM)."""
    anomalies = []
    for previous, current in itertools.pairwise(daily):
        if not previous["visits"]:
            continue
        change_pct = (current["visits"] - previous["visits"]) / previous["visits"] * 100
        if abs(change_pct) >= ANOMALY_THRESHOLD_PCT:
            direction = "up" if change_pct > 0 else "down"
            anomalies.append(
                f"{current['date']}: visits {direction} {abs(change_pct):.0f}% "
                f"day-over-day ({previous['visits']} -> {current['visits']})"
            )
    return anomalies


# --------------------------------------------------------------------------- #
# Narratives
# --------------------------------------------------------------------------- #


def _rule_based_summary(
    start_date: date, end_date: date, aggregates: dict[str, Any], anomalies: list[str]
) -> str:
    daily = aggregates["daily"]
    total = sum(row["visits"] for row in daily)
    header = (
        f"Traffic summary {start_date}..{end_date} (rule-based; set ANTHROPIC_API_KEY for narrative mode):"
    )
    lines = [
        header,
        f"- {total} total visits across {len(daily)} day(s)",
    ]
    if aggregates["by_device"]:
        top_device = aggregates["by_device"][0]
        lines.append(f"- Top device: {top_device['device_category']} ({top_device['sessions']} sessions)")
    if aggregates["top_countries"]:
        top_country = aggregates["top_countries"][0]
        lines.append(f"- Top country: {top_country['geo_country']} ({top_country['sessions']} sessions)")
    lines.append(
        f"- Anomalies (>{ANOMALY_THRESHOLD_PCT:.0f}% day-over-day): "
        + ("; ".join(anomalies) if anomalies else "none")
    )
    return "\n".join(lines)


def _prompt(
    start_date: date,
    end_date: date,
    aggregates: dict[str, Any],
    anomalies: list[str],
) -> str:
    payload = json.dumps({"aggregates": aggregates, "detected_anomalies": anomalies}, default=str)
    return (
        "You are a web-analytics analyst. Using ONLY the aggregate JSON below, "
        f"write a concise (<=150 words) plain-language summary of site traffic for "
        f"{start_date}..{end_date} for a non-technical stakeholder: overall level and trend, "
        "device and country mix, and the detected anomalies with plausible framing "
        "(clearly labeled as hypotheses). Do not invent numbers.\n\n" + payload
    )
