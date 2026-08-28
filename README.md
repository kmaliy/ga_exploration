# GA Exploration: API to BigQuery ETL

ETL pipeline for the assessment API: extracts `/daily-visits` (flat) and
`/ga-sessions-data` (nested), flattens sessions, and loads both into
partitioned, clustered BigQuery tables: idempotently, with retries, explicit
data-quality gates, and secrets kept strictly in environment variables.

```
             ┌────────────── extract ──────────────┐   ┌── transform ──┐   ┌───────── load ─────────┐
 /daily-visits ──► paginated GET, retry/backoff ──► normalize ──► pre-load DQ ──► MERGE on visit_date ──► daily_visits
 /ga-sessions ───► paginated GET, retry/backoff ──► drift check ► flatten ► dedupe ► pre-load DQ
                                                       └──► partition replace (table$YYYYMMDD) ──► ga_sessions_flat
                                                                                   └──► post-load DQ + reconciliation
```

## Repository layout

| Path | Purpose |
|---|---|
| `data_pipeline.py` | CLI entry point (Step 2 deliverable) |
| `pyproject.toml` / `uv.lock` | uv-managed dependencies; the committed lock pins exact versions |
| `ga_pipeline/` | Implementation package: `api_client`, `transform`, `bq_loader`, `quality`, `pipeline`, `llm_client`, `llm_summary`, `llm_triage`, `nl_sql`, `config`, `schemas`, `exceptions` |
| `dags/etl_google_analytics_dag.py` | Airflow DAG (Step 3); docs in `docs/DAG.md` |
| `Dockerfile` | Container image (Step 4), example `docker run` inside |
| `ga_pipeline/sql/ddl.sql` | Destination DDL documenting partitioning/clustering intent |
| `scripts/explore_api.sh`, `scripts/profile_api.py` | Step 1 exploration probes + deep profiler |
| `docs/API_EXPLORATION.md` | Step 1 findings (evidence → pipeline decisions) |
| `artifacts/` | Step 1 evidence (reports/) and dry-run output (samples/) |
| `tests/` | Unit tests (fixtures, no network/cloud needed) + DAG contract tests |

## Setup

The project is managed with [uv](https://docs.astral.sh/uv/):
`pyproject.toml` declares dependencies, `uv.lock` pins the exact resolved
versions (committed for reproducibility). There is deliberately no
`requirements.txt`: the lockfile supersedes it as the pinned-dependency
manifest. Plain-pip consumers can generate one on demand with
`uv export --format requirements-txt --no-dev --extra llm -o requirements.txt`.

```bash
./scripts/setup.sh              # one-shot bootstrap (idempotent, safe to re-run)
./scripts/setup.sh --airflow    # also installs the Airflow group (DAG tests, `airflow dags test`)
./scripts/setup.sh --no-verify  # skip the lint + test verification
```

`setup.sh` executes, in order:

1. checks `uv` is installed (prints the install command and exits if not);
2. installs the pinned Python from `.python-version` if missing;
3. creates `.env` from `.env.example` if absent (an existing `.env` is never touched);
4. `uv sync` from `uv.lock`: exact pinned deps, dev group + `llm` extra;
5. verifies the environment: `ruff check`, `ruff format --check`, `pytest`;
6. checks for Google Cloud ADC (warning only; dry-runs need no GCP);
7. prints next steps.

It never reads, prints, or transmits secrets. Afterwards, fill in `.env` and
load it into your shell yourself: `set -a; source .env; set +a`
(a child process cannot export variables into your shell, so this step is manual).
Manual equivalent of the whole script: `uv sync --extra llm && cp .env.example .env`.

Secrets policy: everything sensitive (`GA_API_KEY`, service-account path,
`ANTHROPIC_API_KEY`) comes from environment variables. Nothing secret is in
git or in the image; `.env` and `*.json` credentials are gitignored, and the
key that appeared in the assessment PDF should be treated as compromised and
rotated.

## Step 1: API exploration

Running API exploration step:

```bash
set -a; source .env; set +a
mkdir -p artifacts/reports
chmod +x scripts/explore_api.sh scripts/setup.sh
```

```bash
./scripts/explore_api.sh | tee artifacts/reports/api_exploration.log
uv run python scripts/profile_api.py --sections profile,limits,stability,ratelimit
```

Full findings are in [docs/API_EXPLORATION.md](docs/API_EXPLORATION.md).

Most importand findings as follows:
1. pagination must stop on `pagination.has_next`
because a page past the end returns HTTP 500; 
2. `daily-visits` uses
`visit_date`/`total_visits` instead of the field names in the spec; 
3. session records have no `deviceCategory` and only a truncated `hits_sample`; some geo
fields contain a literal placeholder string instead of null; 
4. most invalid input returns 500 rather than 4xx, so date windows are validated before calling; 
5. `limit` is capped at 500; and no revenue or transaction fields
appeared in 600 profiled records. Each finding led to a concrete change in
the pipeline, listed in the doc.

## Step 2: Running the pipeline

```bash
# Load one week of both endpoints into BigQuery
uv run data_pipeline.py run --start-date 2016-08-01 --end-date 2016-08-07

# One endpoint only
uv run data_pipeline.py run --start-date 2016-08-01 --end-date 2016-08-07 --endpoint ga-sessions

# Dry-run: extract + transform + quality checks, write JSONL locally, no BigQuery
uv run data_pipeline.py run --start-date 2016-08-01 --end-date 2016-08-01 --dry-run
```

(`python data_pipeline.py …` works identically inside any environment with
the dependencies installed.)

The pipeline creates the dataset and tables automatically (idempotently);
`ga_pipeline/sql/ddl.sql` documents the exact definitions.

### Idempotency & duplicate strategy

* `ga_sessions_flat`: atomic per-date partition replacement. Each date's
  rows load into the `ga_sessions_flat$YYYYMMDD` partition decorator with
  `WRITE_TRUNCATE`. Re-running any date converges to the same state; no
  duplicates, no cleanup DML, no partial-state window.
* `daily_visits`: MERGE on `visit_date` from an expiring temp staging
  table: a range load touches many one-row partitions, so an upsert is the
  simpler idempotent shape at this size.
* Within a batch, sessions are deduplicated on the natural grain
  `(full_visitor_id, visit_id, visit_start_time)` (first occurrence wins);
  the dropped count is logged and re-verified post-load.
* Both loads are therefore safe to rerun from the CLI or from Airflow retries
  at any time.

### Partitioning, clustering, and what we would not full-scan

* `ga_sessions_flat`: `PARTITION BY session_date`, `CLUSTER BY geo_country,
  device_category`, mirroring the API's own filters and the expected
  dashboard slices. Date-bounded queries prune partitions; country/device
  predicates benefit from clustering.
* `daily_visits`: partitioned by `visit_date` for uniform per-date operations;
  at ~366 rows this is about operational symmetry rather than cost.
* Not to be full-scanned: any query on `ga_sessions_flat` without a
  `session_date` predicate, and `SELECT *` anywhere; BigQuery bills per
  column scanned. The Step 5 summary queries follow both rules.

### Data quality: the concrete list

Pre-load (in memory, per batch; violations are aggregated into one failure):

1. non-empty batch for the requested date/range
2. required fields present: `session_date`, `full_visitor_id`, `visit_id` /
   `visit_date`, `visits`
3. every session row belongs to the partition date being loaded
4. grain uniqueness after dedupe (a residual duplicate fails the run)
5. non-negative metrics (`visits`, `hits`, `pageviews`, `time_on_site`, `transactions`)
6. daily-visits: no duplicate dates in batch, dates within the requested window
7. schema-drift gate on raw payloads: missing required keys → hard fail;
   unexpected new keys → logged warning (forward-compatible)

Post-load (SQL against the destination, before the table is trusted):

8. loaded row count equals the deduped batch count
9. grain uniqueness holds in the destination partition
10. no NULL keys in the destination partition
11. cross-endpoint reconciliation: `daily_visits.visits` vs summed session
    visits per date. A hard failure only when one side is entirely missing
    (a broken/partial load); metric drift is recorded and warned on, because
    the endpoints demonstrably count visits differently (32% gap on
    2016-08-01, see [docs/API_EXPLORATION.md](docs/API_EXPLORATION.md))

Any violation raises `DataQualityError` and fails the CLI/Airflow task; by
construction reconciliation runs *after* both loads landed, so even its
failure mode leaves data present but flagged untrusted.

### Error handling & retries

* HTTP: retrying adapter (exponential backoff, honours `Retry-After`) for
  429/5xx inside each attempt; explicit timeouts on every request.
* Taxonomy: `TransientApiError`/`TransientLoadError` (retry-worthy) vs
  `FatalApiError`/`SchemaDriftError`/`DataQualityError` (fail fast), so Airflow's
  2 retries are spent only where a retry can help.
* BigQuery: transient 429/5xx retried with exponential backoff (4 attempts);
  staging tables carry a 1-hour expiry so failed runs cannot leak storage.

## Testing

```bash
uv run pytest                        # unit tests; no network, GCP, or Airflow needed
uv sync --group airflow && uv run pytest   # + DAG contract tests against real Airflow 2.10
uv run ruff check .                  # lint (config in pyproject.toml)
uv run ruff format --check .         # formatting
```

Covers: flattening (camelCase + snake_case), type coercion, dedupe, schema
drift, pagination/envelope/error taxonomy of the client, every DQ check (pass
and fail paths), dry-run end-to-end, anomaly detection, failure-triage
redaction and fallbacks, the NL→SQL guardrails (DML/allowlist/partition/cost
refusals), and, where Airflow is installed, DAG contract tests (schedule,
retries, timeouts, dependencies, and the redacting failure callback).

## Step 3: Airflow

`dags/etl_google_analytics_dag.py`: Wednesdays 06:00 & 18:00 Europe/Berlin
(`0 6,18 * * 3`), `retries=2` / 5-minute delay, 3-minute `execution_timeout`
per task, loads in parallel then a dedicated reconciliation task. Metrics,
failure behaviour, alerting, and the PII stance for custom dimensions are in
[`docs/DAG.md`](docs/DAG.md).

## Step 4: Docker

```bash
docker build -t ga-pipeline:1.0.0 .
docker run --rm \
  --env-file .env \
  -v "$PWD/service-account.json:/secrets/sa.json:ro" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/sa.json \
  ga-pipeline:1.0.0 \
  run --start-date 2016-08-01 --end-date 2016-08-07
```

Locked dependencies (`uv sync --locked` from `uv.lock`), non-root user, no
dev tooling in the image, no secrets in the image or its layers; credentials
enter at `docker run` time only.

## Step 5: LLM integration (bonus)

Three integrations, sharing one principle: **the LLM is never load-bearing
and never trusted.** All calls go through `ga_pipeline/llm_client.py`
(key/model from environment variables, 30 s timeout, 2 retries), and every
deterministic guarantee holds with the LLM absent, down, or wrong.

### 5a. Traffic summary

```bash
uv run data_pipeline.py summarize --start-date 2016-08-01 --end-date 2016-08-31
```

Sends aggregates only (never row-level data) to an LLM for a
stakeholder-friendly narrative. Anomaly detection itself is deterministic
Python (±30% day-over-day); the LLM narrates, it does not compute. Without
`ANTHROPIC_API_KEY` the command still works and returns a rule-based summary.

### 5b. Failure triage in alerting (wired into Step 3)

The DAG's `on_failure_callback` calls `ga_pipeline/llm_triage.py`: regex
redaction strips secrets and PII-shaped strings (emails, API keys, tokens)
*before* anything is logged or sent anywhere, a fixed playbook maps the
pipeline's exception taxonomy to a concrete first response (e.g.
`DataQualityError` → "inspect the failing checks before any rerun"), and the
LLM adds a short narrative labeled *advisory*. `triage_failure` never raises
and falls back to the deterministic message alone, so a broken or absent LLM
can never eat an alert. Details in [`docs/DAG.md`](docs/DAG.md).

### 5c. Guardrailed NL→SQL

```bash
uv run data_pipeline.py ask "Which device had the highest bounce rate in August 2016?"
```

The LLM writes BigQuery SQL over the two destination tables; deterministic
guardrails in `ga_pipeline/nl_sql.py` decide whether it runs. Read-only: one
SELECT statement, DML/DDL keywords refused. Table allowlist: only the two
destination tables, no `INFORMATION_SCHEMA`. Partition discipline: the query
must filter `session_date`/`visit_date` — the same "never full-scan" rule
documented above, now enforced in code. Cost cap: a dry run estimates bytes
scanned and anything over the cap (default 100 MB, `--max-scanned-mb`) is
refused *before* execution, with `maximum_bytes_billed` set on the real run
so BigQuery enforces the same limit server-side. A `LIMIT` is appended when
missing. Unlike 5a/5b there is no fallback: without a working LLM the
command refuses instead of guessing.

## Known limitations / next steps

* The schema was validated against live responses (see
  [docs/API_EXPLORATION.md](docs/API_EXPLORATION.md));
  the drift gate still turns any future contract change into an explicit
  error rather than silent NULLs.
* The API exposes only a truncated `hits_sample`, so no hit-level data is
  loaded (`totals_hits` carries the true count); a hit-level fact table
  (partitioned by date, clustered by session key) is the natural next model
  if the API ever serves complete hits.
* `device_category` is derived from `isMobile` when the API omits it;
  tablets are indistinguishable from phones in that fallback.
* Alerting callback logs a redacted, playbook-based triage message (LLM
  narrative when configured) but ships without a delivery channel; wire the
  Slack/PagerDuty webhook in `_notify_failure` for production.
