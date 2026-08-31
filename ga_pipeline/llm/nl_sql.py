"""Natural-language questions over the loaded tables (NL->SQL).

The LLM writes BigQuery SQL; it is never trusted. Every generated statement
must pass deterministic guardrails before it touches data:

1. Read-only: one single SELECT/WITH statement, no DML/DDL keywords.
2. Table allowlist: only the two destination tables — no other datasets,
   no ``INFORMATION_SCHEMA``. Every FROM/JOIN target (including comma
   cross-joins) must be a backticked allowlisted id, a subquery, ``UNNEST``
   or a CTE name; a qualified table referenced without backticks is refused
   rather than let past the allowlist. SQL comments are refused outright so
   they cannot hide a reference from the checks.
3. Partition discipline: the statement must *filter* on a partition column
   (``session_date`` / ``visit_date``) — the same "never full-scan" rule the
   rest of the project documents for dashboards. Mentioning the column in a
   SELECT or GROUP BY is not enough; it has to appear in a comparison.
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
_COMPARISON = r"(?:=|!=|<>|<=|>=|<|>)"

# Relation-target scan (see _check_relation_targets). Backticked ids are
# masked with this placeholder after they pass the allowlist, so any dotted
# name still visible to the scanner is an attempt to reach a table without
# going through the allowlist.
_ALLOWED_TABLE_PLACEHOLDER = "__allowed_table__"
_STRING_LITERAL = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")
_COMMENT_MARKERS = ("--", "#", "/*")
_SQL_TOKEN = re.compile(r"[A-Za-z_][A-Za-z0-9_.]*|[(),]")
# Keywords that end a FROM clause at its own nesting depth.
_FROM_CLAUSE_ENDERS = frozenset(
    {"where", "group", "order", "limit", "having", "qualify", "window", "union", "intersect", "except"}
)


def _filters_on(sql: str, field: str) -> bool:
    """True when ``field`` appears in a comparison, not merely mentioned.

    ``SELECT session_date, COUNT(*) ... GROUP BY 1`` names the partition column
    but still scans every partition, so naming it is not sufficient.
    """
    name = re.escape(field)
    predicates = (
        rf"\b{name}\b\s*{_COMPARISON}",  # session_date >= '2016-08-01'
        rf"{_COMPARISON}\s*\b{name}\b",  # '2016-08-01' <= session_date
        rf"\b{name}\b\s+(?:not\s+)?between\b",  # session_date BETWEEN a AND b
        rf"\b{name}\b\s+(?:not\s+)?in\s*\(",  # session_date IN (...)
    )
    return any(re.search(pattern, sql, re.IGNORECASE) for pattern in predicates)


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

    with llm_client.traced("answer-question", tags=["ask"], inputs={"question": question}) as trace:
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
        # row values may be identifying; the trace records the query and its shape only
        trace.output({"sql": sql, "estimated_bytes": estimated, "rows_returned": len(rows)})
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
    _check_relation_targets(sql)
    if not any(_filters_on(sql, field) for field in partition_fields):
        raise SqlGuardrailError(
            f"Query must filter a partition column ({', '.join(sorted(partition_fields))}) "
            "with a comparison, e.g. BETWEEN or >=; naming it in SELECT or GROUP BY still "
            "scans every partition, so full scans are refused."
        )
    if not _HAS_LIMIT.search(sql):
        sql = f"{sql}\nLIMIT {MAX_RESULT_ROWS}"
    return sql


def _check_relation_targets(sql: str) -> None:
    """Refuse FROM/JOIN targets that dodge the backticked-table allowlist.

    The allowlist check above only sees *backticked* ids, so on its own it
    would accept ``FROM `allowed` JOIN other.dataset.tbl`` — one allowlisted
    table smuggling in an unchecked one. This scan walks every relation
    target: after ``FROM``, after every ``JOIN``, and after commas at the FROM
    clause's own nesting depth (comma cross-joins). A target must be a
    backticked id (already validated, masked to a placeholder), a subquery,
    or a dot-free name — ``UNNEST`` or a CTE, neither of which can address a
    table: the loader never sets a default dataset, so BigQuery cannot
    resolve a single-part name to one. Any dotted target is refused.

    Comments are refused outright so they cannot hide a reference from this
    scan, and ``EXTRACT(part FROM expr)`` is recognized so its ``FROM`` is
    not mistaken for a clause. Coarse by design, like the keyword screen:
    a rare awkward-but-legitimate query is refused rather than trusted.
    """
    masked = _BACKTICKED.sub(f" {_ALLOWED_TABLE_PLACEHOLDER} ", sql)
    masked = _STRING_LITERAL.sub(" __literal__ ", masked)
    for marker in _COMMENT_MARKERS:
        if marker in masked:
            raise SqlGuardrailError("SQL comments are not allowed.")

    paren_is_extract: list[bool] = []  # one entry per open paren
    from_depths: list[int] = []  # nesting depths with an open FROM clause
    expect_target = False
    previous = ""
    for tok in _SQL_TOKEN.findall(masked):
        if tok == "(":
            paren_is_extract.append(previous == "extract")
            expect_target = False  # subquery target, or a function argument list
            previous = ""
            continue
        if tok == ")":
            if paren_is_extract:
                paren_is_extract.pop()
            while from_depths and from_depths[-1] > len(paren_is_extract):
                from_depths.pop()
            previous = ""
            continue
        if tok == ",":
            if from_depths and from_depths[-1] == len(paren_is_extract):
                expect_target = True  # comma cross-join
            previous = ""
            continue
        word = tok.lower()
        if expect_target:
            if word != _ALLOWED_TABLE_PLACEHOLDER and "." in tok:
                raise SqlGuardrailError(f"Relation {tok!r} must be a full backticked id from the allowlist.")
            expect_target = False
        elif word == "from" and not (paren_is_extract and paren_is_extract[-1]):
            from_depths.append(len(paren_is_extract))
            expect_target = True
        elif word == "join":
            expect_target = True
        elif word in _FROM_CLAUSE_ENDERS and from_depths and from_depths[-1] == len(paren_is_extract):
            from_depths.pop()
        previous = word


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
        "- Reply with exactly one SELECT statement and nothing else (no markdown, no prose, no SQL comments).\n"
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
