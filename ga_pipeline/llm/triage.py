"""LLM-assisted failure triage, wired into the DAG's failure callback.

Called from the Airflow ``on_failure_callback`` after retries are exhausted.
The trust-sensitive work stays deterministic: secrets and PII-shaped strings
are stripped by regex *before* anything leaves the process, and the
pipeline's exception taxonomy maps to a concrete first response via a fixed
playbook. The LLM only drafts a short advisory narrative on top; without
``ANTHROPIC_API_KEY`` (or on any LLM failure) the deterministic message is
the alert. ``triage_failure`` never raises — a broken alert enricher must
not eat the alert itself.
"""

import json
import logging
import re
from collections.abc import Mapping
from typing import Any

from ga_pipeline.llm import client as llm_client

logger = logging.getLogger(__name__)

_MAX_ERROR_CHARS = 2000

# Deterministic redaction, applied before logging or any LLM call.
# Order matters: specific token shapes first, generic key=value catch-all last.
_REDACTIONS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"), "[email]"),
    (re.compile(r"\bAIza[0-9A-Za-z_-]{4,}"), "[api-key]"),
    (re.compile(r"\bsk-ant-[0-9A-Za-z_-]+"), "[api-key]"),
    (re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._-]+"), r"\1[token]"),
    (
        re.compile(
            r"(?i)\b(api[_-]?key|token|secret|password|passwd|credential|authorization)\b(\s*[=:]\s*)\S+"
        ),
        r"\1\2[redacted]",
    ),
]

# First response per exception class (see ga_pipeline.exceptions and docs/airflow.md).
_PLAYBOOK: dict[str, str] = {
    "ConfigError": (
        "Missing or invalid environment configuration; retries will not help. "
        "Check the deployment's environment variables against .env.example."
    ),
    "SchemaDriftError": (
        "API payload changed shape; do NOT rerun until schemas/transform are updated "
        "(the drift report is in the task log)."
    ),
    "DataQualityError": ("Load is not trusted; inspect the failing checks in the task log before any rerun."),
    "TransientApiError": (
        "API still failing after client-side and Airflow retries; check API status/quota "
        "before clearing the task."
    ),
    "FatalApiError": (
        "Non-retryable API failure (auth or contract); verify GA_API_KEY and the endpoint before rerunning."
    ),
    "TransientLoadError": (
        "BigQuery transient errors exhausted retries; check BigQuery status/quota, then "
        "clear the task (loads are idempotent)."
    ),
    "LoadError": (
        "Non-retryable BigQuery failure; check dataset/table permissions and schema before rerunning."
    ),
    "AirflowTaskTimeout": (
        "Task exceeded its 3-minute budget; check window size and API latency rather than re-clearing."
    ),
}
_DEFAULT_HINT = "No playbook match; read the task log."


def redact(text: str) -> str:
    """Strip secrets and PII-shaped strings from free text (deterministic)."""
    for pattern, replacement in _REDACTIONS:
        text = pattern.sub(replacement, text)
    return text


def triage_failure(metadata: Mapping[str, Any], error: str, log_tail: str = "") -> str:
    """Return a short incident summary for the alert message. Never raises.

    ``metadata`` is task identity only (dag/task/run/try — no payload rows);
    ``error`` should be ``f"{type(exc).__name__}: {exc}"`` so the playbook can
    match the exception class; ``log_tail`` is optional extra evidence. Both
    free-text inputs are redacted before they are logged or sent anywhere.
    """
    try:
        error_red = redact(str(error))[:_MAX_ERROR_CHARS]
        tail_red = redact(str(log_tail))[-_MAX_ERROR_CHARS:] if log_tail else ""
        base = _deterministic_triage(metadata, error_red)
    except Exception as exc:  # alerting must survive its own bugs
        logger.warning("Failure triage itself failed: %s", exc)
        return f"ETL failure (triage unavailable): {dict(metadata)}"
    try:
        narrative = llm_client.try_complete(_prompt(metadata, error_red, tail_red), max_tokens=300)
    except Exception as exc:  # try_complete should not raise, but never trust that here
        logger.warning("LLM triage failed: %s", exc)
        narrative = None
    if narrative:
        return f"{base}\n\nLLM triage (advisory, verify against the task log):\n{narrative}"
    return base


def _deterministic_triage(metadata: Mapping[str, Any], redacted_error: str) -> str:
    ids = " ".join(f"{key}={value}" for key, value in metadata.items())
    first_line = redacted_error.splitlines()[0] if redacted_error else "(no exception captured)"
    hint = next((hint for name, hint in _PLAYBOOK.items() if name in redacted_error), _DEFAULT_HINT)
    return f"ETL failure: {ids}\nError: {first_line}\nNext step: {hint}"


def _prompt(metadata: Mapping[str, Any], redacted_error: str, redacted_tail: str) -> str:
    payload = json.dumps(
        {"task": dict(metadata), "error": redacted_error, "log_tail": redacted_tail},
        default=str,
    )
    return (
        "You are the on-call data engineer for a Google Analytics -> BigQuery ETL. "
        "Using ONLY the redacted JSON below, write an incident note of at most 80 words: "
        "what failed, the most likely cause, and the single best first debugging step. "
        "Plain text, no markdown, no invented details.\n\n" + payload
    )
