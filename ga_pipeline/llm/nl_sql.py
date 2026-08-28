"""Natural-language questions over the loaded tables (NL->SQL).

The LLM writes BigQuery SQL; it is never trusted. Every generated statement
must pass deterministic guardrails before it touches data:

1. Read-only: one single SELECT/WITH statement, no DML/DDL keywords.
2. Table allowlist: only the two destination tables — no other datasets,
   no ``INFORMATION_SCHEMA``.
3. Partition discipline: the statement must reference the partition column
   (``session_date`` / ``visit_date``) — the same "never full-scan" rule the
   rest of the project documents for dashboards.
4. Cost cap: a dry run estimates bytes scanned and the query is refused over
   the cap *before* execution; the real run additionally sets
   ``maximum_bytes_billed`` so BigQuery enforces the same limit server-side.
5. Bounded output: a ``LIMIT`` is appended when missing.

The keyword screen is deliberately coarse (a literal like ``'create account'``
in a filter is refused too); for an internal analyst tool, refusing a rare
awkward query beats ever running a destructive one. Unlike the summary and
triage features there is no fallback here: without a working LLM this command
refuses instead of guessing.
"""

import logging
import re
from dataclasses import dataclass
from typing import Any

from ga_pipeline.config import BigQuerySettings
from ga_pipeline.exceptions import ConfigError, SqlGuardrailError, TransientApiError
from ga_pipeline.llm import client as llm_client
from ga_pipeline.load.bq_loader import BigQueryLoader
from ga_pipeline.load.schemas import ALL_SPECS

logger = logging.getLogger(__name__)

DEFAULT_MAX_SCANNED_BYTES = 100 * 1024 * 1024  # refuse queries estimated over 100 MB
MAX_RESULT_ROWS = 200

_FORBIDDEN_KEYWORDS = re.compile(
    r"(?i)\b(insert|update|delete|merge|drop|create|alter|truncate|grant|revoke|call|begin|commit|execute|export)\b"
)
_BACKTICKED = re.compile(r"`([^`]+)`")
_HAS_LIMIT = re.compile(r"(?i)\blimit\s+\d+\b")


@dataclass(frozen=True)
class AskResult:
    """Outcome of one NL->SQL question: the vetted SQL, its cost, and rows."""

    question: str
    sql: str
    estimated_bytes: int
    rows: list[dict[str, Any]]


def ask(
    question: str,
    loader: BigQueryLoader | None = None,
    max_scanned_bytes: int = DEFAULT_MAX_SCANNED_BYTES,
) -> AskResult:
    """Answer a natural-language question with guardrailed, LLM-generated SQL.

    Raises:
        ConfigError: no ``ANTHROPIC_API_KEY`` (there is no NL->SQL fallback).
        TransientApiError: the LLM returned nothing usable; retry later.
        SqlGuardrailError: the generated SQL violated a guardrail; nothing ran.
    """
    if not llm_client.is_configured():
        raise ConfigError("The ask command needs ANTHROPIC_API_KEY: NL->SQL has no deterministic fallback.")
    loader = loader or BigQueryLoader(BigQuerySettings.from_env())
    allowed_tables = {loader.table_id(spec.name) for spec in ALL_SPECS}
    partition_fields = {spec.partition_field for spec in ALL_SPECS}

    raw = llm_client.try_complete(_prompt(question, loader), max_tokens=600)
    if raw is None:
        raise TransientApiError("LLM returned no SQL; retry, or check ANTHROPIC_API_KEY / network.")
    sql = validate_sql(_extract_sql(raw), allowed_tables, partition_fields)

    estimated = loader.dry_run_bytes(sql)
    if estimated > max_scanned_bytes:
        raise SqlGuardrailError(
            f"Generated query would scan ~{estimated / 1e6:.0f} MB "
            f"(cap {max_scanned_bytes / 1e6:.0f} MB); not executed. "
            "Ask a narrower question (tighter date range) or raise --max-scanned-mb."
        )
    logger.info("NL->SQL estimated scan %.1f MB for question %r", estimated / 1e6, question)
    rows = loader.query_rows(sql, maximum_bytes_billed=max_scanned_bytes)
    return AskResult(question=question, sql=sql, estimated_bytes=estimated, rows=rows[:MAX_RESULT_ROWS])


def validate_sql(sql: str, allowed_tables: set[str], partition_fields: set[str]) -> str:
    """Enforce the read-only/allowlist/partition guardrails; return final SQL.

    Raises ``SqlGuardrailError`` on any violation. Appends a ``LIMIT`` when
    missing. Purely deterministic — no LLM involvement.
    """
    sql = sql.strip().rstrip(";").strip()
    if ";" in sql:
        raise SqlGuardrailError("Multiple SQL statements are not allowed.")
    first_word = sql.split(None, 1)[0].upper() if sql else ""
    if first_word not in {"SELECT", "WITH"}:
        raise SqlGuardrailError("Only SELECT statements are allowed.")
    if match := _FORBIDDEN_KEYWORDS.search(sql):
        raise SqlGuardrailError(f"Forbidden keyword {match.group(0)!r}; only read-only SELECTs run.")
    if "information_schema" in sql.lower():
        raise SqlGuardrailError("INFORMATION_SCHEMA access is not allowed.")
    referenced = set(_BACKTICKED.findall(sql))
    if not referenced:
        raise SqlGuardrailError("Tables must be referenced by their full backticked ids.")
    if unknown := referenced - allowed_tables:
        raise SqlGuardrailError(f"Table(s) not on the allowlist: {sorted(unknown)}.")
    if not any(re.search(rf"\b{re.escape(field)}\b", sql) for field in partition_fields):
        raise SqlGuardrailError(
            f"Query must filter a partition column ({', '.join(sorted(partition_fields))}); "
            "full scans are refused."
        )
    if not _HAS_LIMIT.search(sql):
        sql = f"{sql}\nLIMIT {MAX_RESULT_ROWS}"
    return sql


def _extract_sql(raw: str) -> str:
    """Strip optional markdown fences from the LLM response."""
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def _prompt(question: str, loader: BigQueryLoader) -> str:
    return (
        "You translate analytics questions into BigQuery Standard SQL.\n"
        f"{_schema_block(loader)}\n"
        "Data covers 2016-08-01 through 2017-08-01.\n"
        "Rules:\n"
        "- Reply with exactly one SELECT statement and nothing else (no markdown, no prose).\n"
        "- Reference tables by the full backticked ids above; use no other tables.\n"
        "- ALWAYS filter the partition column (session_date / visit_date) to the narrowest\n"
        "  date range that answers the question; never scan the full table.\n"
        "- Project only the needed columns (no SELECT *).\n"
        f"- Include LIMIT {MAX_RESULT_ROWS} unless the result is already a small aggregate.\n\n"
        f"Question: {question}"
    )


def _schema_block(loader: BigQueryLoader) -> str:
    lines = []
    for spec in ALL_SPECS:
        clustering = f", clustered by {', '.join(spec.clustering_fields)}" if spec.clustering_fields else ""
        columns = ", ".join(f"{field.name} {field.field_type}" for field in spec.schema)
        lines.append(
            f"Table `{loader.table_id(spec.name)}` "
            f"(partitioned by {spec.partition_field}{clustering}): {columns}"
        )
    return "\n".join(lines)
