"""Transform layer: raw API records -> destination row shapes.

Split by destination table, over a shared coercion layer:

* :mod:`ga_pipeline.transform.sessions` — ``ga_sessions_flat``
* :mod:`ga_pipeline.transform.daily_visits` — ``daily_visits``
* :mod:`ga_pipeline.transform.coercion` — generic field lookup and type coercion

Every name the flat ``transform`` module used to expose is re-exported here, so
``from ga_pipeline.transform import flatten_session`` keeps working.
"""

from ga_pipeline.transform.coercion import parse_ga_date, utcnow
from ga_pipeline.transform.daily_visits import transform_daily_visit
from ga_pipeline.transform.sessions import (
    EXPECTED_SESSION_KEYS,
    REQUIRED_SESSION_KEYS,
    dedupe_sessions,
    detect_schema_drift,
    flatten_session,
    session_key,
)

__all__ = [
    "EXPECTED_SESSION_KEYS",
    "REQUIRED_SESSION_KEYS",
    "dedupe_sessions",
    "detect_schema_drift",
    "flatten_session",
    "parse_ga_date",
    "session_key",
    "transform_daily_visit",
    "utcnow",
]
