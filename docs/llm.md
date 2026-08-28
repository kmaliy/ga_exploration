# LLM features

Three features, all optional, all behind the `llm` extra (`uv sync --extra
llm`). They share one rule: the LLM is never load-bearing and never trusted.
Calls go through [`ga_pipeline/llm/client.py`](../ga_pipeline/llm/client.py),
which reads key and model from the environment and applies a 30s timeout and
two retries. Every deterministic guarantee holds with the LLM absent, down or
wrong.

Nothing in the core ETL path imports `ga_pipeline/llm/` at module scope — the
CLI and the DAG import it lazily — so deleting the directory leaves a working
pipeline.

## Traffic summary

```bash
uv run data_pipeline.py summarize --start-date 2016-08-01 --end-date 2016-08-31
```

`summary.py` sends aggregates only, never rows, and asks for a narrative a
stakeholder can read. Anomaly detection is plain Python (±30% day over day).
The LLM narrates, it does not compute. Without `ANTHROPIC_API_KEY` the command
still works and returns a rule-based summary.

## Failure triage

The DAG's `on_failure_callback` calls `triage.py`. Redaction strips secrets and
PII-shaped strings before anything is logged or sent. A fixed playbook maps each
exception type to a first response, e.g. `DataQualityError` to "inspect the
failing checks before any rerun". The LLM then adds a short narrative labelled
advisory. `triage_failure` never raises and falls back to the deterministic
message, so a broken LLM cannot swallow an alert. More in
[airflow.md](airflow.md).

## NL→SQL

```bash
uv run data_pipeline.py ask "Which device had the highest bounce rate in August 2016?"
```

The LLM writes the SQL; `nl_sql.py` decides whether it runs.

| Guardrail | Rule |
|---|---|
| Read-only | one `SELECT`; DML and DDL keywords refused |
| Table allowlist | the two destination tables, no `INFORMATION_SCHEMA` |
| Partition filter | must constrain `session_date` or `visit_date` |
| Cost cap | a dry run estimates bytes scanned; over the cap (100 MB default, `--max-scanned-mb`) is refused before execution, and `maximum_bytes_billed` enforces it server-side on the real run |
| Bounded output | a `LIMIT` is appended if missing |

Unlike the other two there is no fallback. Without a working LLM the command
refuses rather than guessing.
