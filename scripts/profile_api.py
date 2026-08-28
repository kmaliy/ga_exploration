"""Extended API exploration: profile field contents and probe API behaviour.

Talks to the API with plain requests instead of the pipeline client, because
the pipeline client retries, validates dates and hides the response envelope,
and here we want to see raw behaviour, including errors.

Sections (pick with --sections, comma-separated, default all):

    profile     field presence, null rates and value ranges across sample
                days; also looks for records with transactions
    sweep       record count for every day in the range, compared against
                /daily-visits by GA shard date (slow, ~366 requests). Shard
                date is the property's local day and /daily-visits counts by
                UTC date, so the drift this reports is the timezone offset
                between the endpoints, not a load-correctness measure
    limits      what happens at limit=5000 and limit=0
    stability   same query twice (is ordering stable?) and page 1 vs page 2
    ratelimit   burst of requests: any 429s, Retry-After header, latency

Usage:
    export GA_API_KEY=...
    uv run python scripts/profile_api.py
    uv run python scripts/profile_api.py --sections profile,limits

Output: artifacts/reports/api_profile_report.md and api_profile_raw.json.
"""

import json
import os
import statistics
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Annotated, Any

import requests
import typer

DEFAULT_BASE_URL = "https://dish-second-course-gateway-2tximoqc.nw.gateway.dev"
SESSIONS_RANGE = (date(2016, 8, 1), date(2017, 8, 1))
DAILY_RANGE = (date(2016, 8, 1), date(2017, 8, 2))

# Sample days spread across the year, including Black Friday, Cyber Monday
# and Christmas (best chance of seeing transaction data).
PROFILE_DATES = ("20160801", "20161123", "20161128", "20161225", "20170315", "20170716")
PROFILE_LIMIT = 100
DISTINCT_CAP = 15
BURST_SIZE = 20

app = typer.Typer(add_completion=False, pretty_exceptions_show_locals=False)


class Api:
    """Small probing client. No retries and no input validation on purpose."""

    def __init__(self) -> None:
        key = os.environ.get("GA_API_KEY")
        if not key:
            typer.echo("ERROR: set GA_API_KEY first (see .env.example)", err=True)
            raise typer.Exit(code=2)
        self.base = os.environ.get("GA_API_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
        self.session = requests.Session()
        self.session.headers["X-API-Key"] = key

    def get(self, path: str, **params: Any) -> tuple[int, Any, float, dict[str, str]]:
        """Return (status, parsed_body_or_text, seconds, headers)."""
        started = time.perf_counter()
        response = self.session.get(f"{self.base}{path}", params=params, timeout=60)
        elapsed = time.perf_counter() - started
        try:
            body = response.json()
        except ValueError:
            body = response.text[:300]
        return response.status_code, body, elapsed, dict(response.headers)


# ---- profile ----


def run_profile(api: Api, pause: float) -> dict[str, Any]:
    stats: dict[str, dict[str, Any]] = {}
    transaction_examples: list[dict[str, Any]] = []
    records_seen = 0

    for ga_date in PROFILE_DATES:
        status, body, elapsed, _ = api.get("/ga-sessions-data", date=ga_date, limit=PROFILE_LIMIT, page=1)
        typer.echo(f"  profile {ga_date}: HTTP {status} in {elapsed:.1f}s")
        if status != 200 or not isinstance(body, dict):
            continue
        for record in body.get("records", []):
            records_seen += 1
            _collect_paths(record, "", stats)
            totals = record.get("totals") or {}
            if totals.get("transactions") and len(transaction_examples) < 3:
                transaction_examples.append(
                    {"date": ga_date, "totals": totals, "visitId": record.get("visitId")}
                )
        time.sleep(pause)

    fields = {
        path: {
            "present_pct": round(100 * s["present"] / records_seen, 1),
            "null_pct_when_present": round(100 * s["nulls"] / max(s["present"], 1), 1),
            "types": sorted(s["types"]),
            "distinct_sample": sorted(map(str, s["values"]))[:DISTINCT_CAP],
            "numeric_range": [s["min"], s["max"]] if s["min"] is not None else None,
        }
        for path, s in sorted(stats.items())
    }
    return {
        "records_profiled": records_seen,
        "dates": list(PROFILE_DATES),
        "fields": fields,
        "transaction_examples": transaction_examples,
    }


def _collect_paths(record: dict[str, Any], prefix: str, stats: dict[str, dict[str, Any]]) -> None:
    for key, value in record.items():
        path = f"{prefix}{key}"
        entry = stats.setdefault(
            path,
            {"present": 0, "nulls": 0, "types": set(), "values": set(), "min": None, "max": None},
        )
        entry["present"] += 1
        if value is None:
            entry["nulls"] += 1
            continue
        entry["types"].add(type(value).__name__)
        if isinstance(value, dict) and prefix == "":  # one nesting level is enough
            _collect_paths(value, f"{path}.", stats)
        elif isinstance(value, list):
            entry["values"].add(f"<list len={len(value)}>")
        elif isinstance(value, bool):
            entry["values"].add(value)
        elif isinstance(value, int | float):
            entry["min"] = value if entry["min"] is None else min(entry["min"], value)
            entry["max"] = value if entry["max"] is None else max(entry["max"], value)
            if len(entry["values"]) < DISTINCT_CAP:
                entry["values"].add(value)
        elif len(entry["values"]) < DISTINCT_CAP:
            entry["values"].add(str(value)[:60])


# ---- sweep ----


def run_sweep(api: Api, start: date, end: date, pause: float) -> dict[str, Any]:
    daily_visits = _fetch_daily_visits(api)
    per_day: list[dict[str, Any]] = []
    failures: list[str] = []

    day = start
    while day <= end:
        ga_date = day.strftime("%Y%m%d")
        status, body, _, _ = api.get("/ga-sessions-data", date=ga_date, limit=1, page=1)
        if status == 200 and isinstance(body, dict):
            sessions = body.get("pagination", {}).get("total_records")
            visits = daily_visits.get(day.isoformat())
            drift = (
                round(abs(visits - sessions) / visits * 100, 2) if visits and sessions is not None else None
            )
            per_day.append(
                {
                    "date": day.isoformat(),
                    "sessions": sessions,
                    "daily_visits": visits,
                    "drift_pct": drift,
                }
            )
        else:
            failures.append(f"{day.isoformat()}: HTTP {status}")
        if len(per_day) % 30 == 0 and per_day:
            typer.echo(f"  sweep progress: {day.isoformat()} ({len(per_day)} days done)")
        time.sleep(pause)
        day += timedelta(days=1)

    drifts = [d["drift_pct"] for d in per_day if d["drift_pct"] is not None]
    return {
        "days_swept": len(per_day),
        "failures": failures,
        "empty_days": [d["date"] for d in per_day if d["sessions"] == 0],
        "drift_stats": {
            "min_pct": min(drifts) if drifts else None,
            "max_pct": max(drifts) if drifts else None,
            "mean_pct": round(statistics.fmean(drifts), 2) if drifts else None,
            # Shard-date drift, not the pipeline's reconciliation drift: this
            # compares two different calendar days. 5% is a reporting cut-off
            # for this report only.
            "days_over_5pct_shard_drift": [d for d in per_day if (d["drift_pct"] or 0) > 5],
        },
        "volume": {
            "min_sessions_day": min(per_day, key=lambda d: d["sessions"] or 0, default=None),
            "max_sessions_day": max(per_day, key=lambda d: d["sessions"] or 0, default=None),
        },
        "per_day": per_day,
    }


def _fetch_daily_visits(api: Api) -> dict[str, int]:
    visits: dict[str, int] = {}
    page = 1
    while True:
        status, body, _, _ = api.get(
            "/daily-visits",
            start_date=DAILY_RANGE[0].isoformat(),
            end_date=DAILY_RANGE[1].isoformat(),
            page=page,
            limit=200,
        )
        if status != 200 or not isinstance(body, dict):
            typer.echo(f"  daily-visits fetch failed: HTTP {status}", err=True)
            return visits
        for row in body.get("records", []):
            visits[row["visit_date"]] = row["total_visits"]
        if not body.get("pagination", {}).get("has_next"):
            return visits
        page += 1


# ---- limits / stability / ratelimit ----


def run_limits(api: Api) -> dict[str, Any]:
    results = {}
    for limit in (5000, 0):
        status, body, elapsed, _ = api.get("/ga-sessions-data", date="20170801", limit=limit, page=1)
        summary: dict[str, Any] = {"status": status, "seconds": round(elapsed, 2)}
        if isinstance(body, dict):
            summary["records_returned"] = body.get("metadata", {}).get("records_returned")
            summary["pagination"] = body.get("pagination")
        else:
            summary["body"] = body
        results[f"limit={limit}"] = summary
        typer.echo(f"  limit={limit}: HTTP {status}")
    return results


def run_stability(api: Api) -> dict[str, Any]:
    def visit_ids(body: Any) -> list[Any]:
        if isinstance(body, dict):
            return [r.get("visitId") for r in body.get("records", [])]
        return []

    _, first, _, _ = api.get("/ga-sessions-data", date="20170801", limit=20, page=1)
    _, second, _, _ = api.get("/ga-sessions-data", date="20170801", limit=20, page=1)
    _, next_page, _, _ = api.get("/ga-sessions-data", date="20170801", limit=20, page=2)

    ids_a, ids_b, ids_p2 = visit_ids(first), visit_ids(second), visit_ids(next_page)
    return {
        "same_query_identical_order": ids_a == ids_b,
        "page1_page2_overlap": sorted(set(ids_a) & set(ids_p2)),
        "page1_ids": ids_a[:5],
    }


def run_ratelimit(api: Api) -> dict[str, Any]:
    statuses: list[int] = []
    latencies: list[float] = []
    retry_after_seen = False
    for _ in range(BURST_SIZE):
        status, _, elapsed, headers = api.get("/daily-visits", limit=1, page=1)
        statuses.append(status)
        latencies.append(round(elapsed, 2))
        if "Retry-After" in headers:
            retry_after_seen = True
    return {
        "burst_size": BURST_SIZE,
        "status_counts": {code: statuses.count(code) for code in sorted(set(statuses))},
        "saw_429": 429 in statuses,
        "retry_after_header_seen": retry_after_seen,
        "latency_seconds": {
            "min": min(latencies),
            "median": round(statistics.median(latencies), 2),
            "max": max(latencies),
        },
    }


# ---- report ----


def write_report(results: dict[str, Any], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_path = out_dir / "api_profile_raw.json"
    raw_path.write_text(json.dumps(results, indent=2, default=str))

    lines = ["# API profiling report", ""]
    profile = results.get("profile")
    if profile:
        lines += [
            "## Field profile",
            f"Profiled {profile['records_profiled']} records across {profile['dates']}.",
            "",
            "| field | present % | null % (when present) | types | range / sample values |",
            "|---|---|---|---|---|",
        ]
        for path, f in profile["fields"].items():
            domain = f["numeric_range"] or ", ".join(f["distinct_sample"][:6])
            lines.append(
                f"| `{path}` | {f['present_pct']} | {f['null_pct_when_present']} "
                f"| {', '.join(f['types'])} | {domain} |"
            )
        found = len(profile["transaction_examples"])
        verdict = "revenue fields exist in live data" if found else "revenue fields never observed"
        lines += ["", f"Transaction examples found: {found} ({verdict})", ""]
    sweep = results.get("sweep")
    if sweep:
        ds = sweep["drift_stats"]
        lines += [
            "## Daily sweep: sessions by shard date vs /daily-visits",
            "Sessions are counted by GA's shard `date` (the property's local day, UTC-7) "
            "and compared against `/daily-visits`, which counts by UTC date. These are "
            "different calendar days, so the drift below measures the timezone offset "
            "between the endpoints — it is the signal that revealed the offset, not a "
            "measure of whether a load was complete. The pipeline reconciles on "
            "`DATE(visit_start_time)` instead, where the expected drift is zero.",
            "",
            f"Swept {sweep['days_swept']} days; failures: {sweep['failures'] or 'none'}; "
            f"empty days: {sweep['empty_days'] or 'none'}.",
            f"Shard-date drift vs daily-visits: min {ds['min_pct']}%, mean {ds['mean_pct']}%, "
            f"max {ds['max_pct']}%.",
            f"Days where shard-date drift exceeds 5%: {ds['days_over_5pct_shard_drift'] or 'none'}.",
            f"Volume: min day {sweep['volume']['min_sessions_day']}, "
            f"max day {sweep['volume']['max_sessions_day']}.",
            "",
        ]
    for key, title in (
        ("limits", "Limit ceiling"),
        ("stability", "Ordering stability"),
        ("ratelimit", "Rate-limit burst"),
    ):
        if key in results:
            lines += [f"## {title}", "```json", json.dumps(results[key], indent=2), "```", ""]

    report_path = out_dir / "api_profile_report.md"
    report_path.write_text("\n".join(lines))
    return report_path


# ---- entry point ----


@app.command()
def main(
    sections: Annotated[
        str, typer.Option(help="Comma list: profile,sweep,limits,stability,ratelimit or 'all'.")
    ] = "all",
    sweep_start: Annotated[str, typer.Option(help="Sweep start (YYYY-MM-DD).")] = "2016-08-01",
    sweep_end: Annotated[str, typer.Option(help="Sweep end (YYYY-MM-DD).")] = "2017-08-01",
    pause: Annotated[float, typer.Option(help="Pause between calls in seconds.")] = 0.2,
    out_dir: Annotated[Path, typer.Option(help="Report directory.")] = Path("artifacts/reports"),
) -> None:
    """Run the selected probes and write a markdown + JSON report."""
    chosen = (
        {"profile", "sweep", "limits", "stability", "ratelimit"}
        if sections == "all"
        else {s.strip() for s in sections.split(",")}
    )
    api = Api()
    results: dict[str, Any] = {}

    if "profile" in chosen:
        typer.echo("== profile ==")
        results["profile"] = run_profile(api, pause)
    if "limits" in chosen:
        typer.echo("== limits ==")
        results["limits"] = run_limits(api)
    if "stability" in chosen:
        typer.echo("== stability ==")
        results["stability"] = run_stability(api)
    if "ratelimit" in chosen:
        typer.echo("== ratelimit ==")
        results["ratelimit"] = run_ratelimit(api)
    if "sweep" in chosen:
        typer.echo("== sweep (slow, ~366 requests) ==")
        results["sweep"] = run_sweep(
            api, date.fromisoformat(sweep_start), date.fromisoformat(sweep_end), pause
        )

    report = write_report(results, out_dir)
    typer.echo(f"\nReport: {report}\nRaw:    {out_dir / 'api_profile_raw.json'}")


if __name__ == "__main__":
    sys.exit(app())
