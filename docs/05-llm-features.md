# LLM integration (Step 5, bonus)

Three integrations, sharing one principle: **the LLM is never load-bearing and
never trusted.** All calls go through
[`ga_pipeline/llm/client.py`](../ga_pipeline/llm/client.py) (key and model from
environment variables, 30 s timeout, 2 retries), and every deterministic
guarantee holds with the LLM absent, down, or wrong.

Everything in this step lives in `ga_pipeline/llm/` and depends on the optional
`llm` extra (`uv sync --extra llm`). Nothing in the core ETL path imports it at
module scope — the CLI and the DAG import from it lazily — so the pipeline runs
with that directory removed.

## 5a. Traffic summary

```bash
uv run data_pipeline.py summarize --start-date 2016-08-01 --end-date 2016-08-31
```

`ga_pipeline/llm/summary.py` sends **aggregates only** — never row-level data —
to an LLM for a stakeholder-friendly narrative. Anomaly detection itself is
deterministic Python (±30% day-over-day); the LLM narrates, it does not compute.
Without `ANTHROPIC_API_KEY` the command still works and returns a rule-based
summary.

## 5b. Failure triage in alerting (wired into Step 3)

The DAG's `on_failure_callback` calls `ga_pipeline/llm/triage.py`:

1. Regex redaction strips secrets and PII-shaped strings (emails, API keys,
   tokens) *before* anything is logged or sent anywhere.
2. A fixed playbook maps the pipeline's exception taxonomy to a concrete first
   response — e.g. `DataQualityError` → "inspect the failing checks before any
   rerun".
3. The LLM adds a short narrative, labeled *advisory*.

`triage_failure` never raises and falls back to the deterministic message alone,
so a broken or absent LLM can never eat an alert. Details in
[03-airflow.md](03-airflow.md).

## 5c. Guardrailed NL→SQL

```bash
uv run data_pipeline.py ask "Which device had the highest bounce rate in August 2016?"
```

The LLM writes BigQuery SQL over the two destination tables; deterministic
guardrails in `ga_pipeline/llm/nl_sql.py` decide whether it runs:

| Guardrail | Rule |
|---|---|
| Read-only | one `SELECT` statement; DML/DDL keywords refused |
| Table allowlist | only the two destination tables, no `INFORMATION_SCHEMA` |
| Partition discipline | must filter `session_date` / `visit_date` — the "never full-scan" rule from [02-pipeline-design.md](02-pipeline-design.md), enforced in code |
| Cost cap | a dry run estimates bytes scanned; over the cap (default 100 MB, `--max-scanned-mb`) is refused *before* execution, and `maximum_bytes_billed` is set on the real run so BigQuery enforces the same limit server-side |
| Bounded output | a `LIMIT` is appended when missing |

Unlike 5a and 5b there is no fallback: without a working LLM the command refuses
instead of guessing.
