"""Type coercion and field lookup shared by the transform modules.

The API follows the GA export format but its exact casing is not guaranteed,
so every lookup accepts both camelCase and snake_case names. Numeric values may
arrive as strings and are coerced rather than trusted.

Nothing here knows about GA sessions or daily visits — it is the generic layer
that ``sessions.py`` and ``daily_visits.py`` are written on top of.
"""

from datetime import UTC, date, datetime
from typing import Any

from ga_pipeline.exceptions import FatalApiError

# The demo dataset uses this placeholder string for fields it does not
# provide; it is treated as NULL on ingest.
DEMO_SENTINEL = "not available in demo dataset"


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


def get_field(container: Any, *names: str) -> Any:
    """Return the first present key from ``names``, or None."""
    if not isinstance(container, dict):
        return None
    for name in names:
        if name in container:
            return container[name]
    return None


def require_field(record: dict[str, Any], *names: str) -> Any:
    """Like :func:`get_field`, but raise ``FatalApiError`` when absent."""
    value = get_field(record, *names)
    if value is None:
        raise FatalApiError(f"Record missing required field {names[0]!r}: keys={sorted(record)}")
    return value


_TRUE_STRINGS = frozenset({"true", "1", "yes"})
_FALSE_STRINGS = frozenset({"false", "0", "no"})


def to_int(value: Any) -> int | None:
    """Coerce to int, returning None for empty or non-numeric values.

    Integral float strings ("1.0") count as ints; fractional values do not —
    silently truncating "2.5" would hide a contract change, so it is NULL.
    """
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        pass
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(number) if number.is_integer() else None


def to_bool(value: Any) -> bool | None:
    """Coerce to bool, accepting the string forms the API sends.

    Only recognized forms map to a value; anything else — including the
    demo-dataset placeholder — is None. Defaulting unknown strings to False
    would silently record a wrong value instead of a NULL (and let
    ``device_category`` be derived from garbage).
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in _TRUE_STRINGS:
        return True
    if text in _FALSE_STRINGS:
        return False
    return None


def to_str(value: Any) -> str | None:
    """Coerce to str, mapping the demo-dataset placeholder to None."""
    if value is None:
        return None
    text = str(value)
    if text == DEMO_SENTINEL:
        return None
    return text


def epoch_to_iso(value: Any) -> str | None:
    """Convert a Unix epoch (seconds) to an ISO-8601 UTC timestamp string."""
    seconds = to_int(value)
    if seconds is None:
        return None
    return datetime.fromtimestamp(seconds, tz=UTC).isoformat()


def snake_case(name: str) -> str:
    """Convert a camelCase API field name to snake_case."""
    out = []
    for ch in name:
        if ch.isupper():
            out.append("_")
            out.append(ch.lower())
        else:
            out.append(ch)
    return "".join(out)
