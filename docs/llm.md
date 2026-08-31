# LLM features

Three features, all optional, all behind the `llm` extra (`uv sync --extra
llm`). They share one rule: the LLM is never load-bearing and never trusted.
Calls go through [`ga_pipeline/llm/client.py`](../ga_pipeline/llm/client.py),
which reads key and model from the environment and applies a 30s timeout and
two retries. A personal or service-account key that is not scoped to a
workspace must also say which workspace it acts in; set
`ANTHROPIC_WORKSPACE_ID` and the client sends the header. Workspace-scoped keys
need nothing. Every deterministic guarantee holds with the LLM absent, down or
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

## Tracing (optional)

Every LLM call goes through one function, so a single instrumentation hook
covers all three features. With Langfuse configured you get the prompt, the
response, latency and token counts for each call — useful mainly for seeing
what SQL `ask` actually generated and why a guardrail refused it.

Run Langfuse locally:

```bash
git clone --depth=1 https://github.com/langfuse/langfuse.git
cd langfuse && docker compose up -d
```

Open http://localhost:3000, create an account and a project, then copy the two
project keys into `.env`:

```
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_SECRET_KEY=sk-lf-...
LANGFUSE_BASE_URL=http://localhost:3000
```

Install the extra and run anything that calls the LLM:

```bash
uv sync --extra llm --extra tracing
set -a; source .env; set +a
uv run data_pipeline.py summarize --start-date 2016-08-01 --end-date 2016-08-07
```

Traces appear in the Langfuse UI. Stop it with `docker compose down` in the
Langfuse directory.

### What a trace looks like

Each feature is one named trace, with the Anthropic call nested inside it:

| Trace | Tag | Input | Output |
|---|---|---|---|
| `summarize-traffic` | `summarize` | the date range | the summary, and whether the LLM or the fallback produced it |
| `answer-question` | `ask` | the question | the generated SQL, estimated bytes, row count |
| `triage-failure` | `triage` | task ids and the **redacted** error | the alert text, and whether the LLM was used |

Names are stable and verb-first so dashboards can filter on them, and inputs
are set explicitly rather than inferred from the call — otherwise the loader,
settings and API keys would land in the trace as function arguments. Outputs
carry shapes, not payloads: `answer-question` records the row *count*, never
the rows, since row values can identify visitors.

Model name, token counts and latency come from the Anthropic instrumentor
automatically, so cost is calculated for you.

It is off by default and cannot break anything. `enable_tracing()` returns
immediately unless `LANGFUSE_PUBLIC_KEY` is set, and a missing extra or an
unreachable collector is logged and swallowed rather than raised — the same
posture the LLM calls themselves take. Spans export in a background batch, so
a dead collector costs no latency on the call. Each trace flushes on exit,
which matters here: the CLI is a short-lived process, and without a flush the
batch exporter would be killed before it ever shipped anything.
