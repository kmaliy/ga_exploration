# Pipeline design (Step 2)

The design decisions behind `ga_pipeline/` — why the loads are shaped the way
they are, what the quality gates actually check, and which failures are worth
retrying.

## Running it

```bash
# Load one week of both endpoints into BigQuery
uv run data_pipeline.py run --start-date 2016-08-01 --end-date 2016-08-07

# One endpoint only
uv run data_pipeline.py run --start-date 2016-08-01 --end-date 2016-08-07 --endpoint ga-sessions

# Dry-run: extract + transform + quality checks, write JSONL locally, no BigQuery
uv run data_pipeline.py run --start-date 2016-08-01 --end-date 2016-08-01 --dry-run
```

(`python data_pipeline.py …` works identically inside any environment with the
dependencies installed.) Dry-run output lands in `artifacts/samples/`.

The pipeline creates the dataset and tables automatically (idempotently);
[`ga_pipeline/sql/ddl.sql`](../ga_pipeline/sql/ddl.sql) documents the exact
definitions, and `tests/unit/test_schema_ddl_parity.py` keeps that file honest
against `ga_pipeline/load/schemas.py`.

## Idempotency & duplicate strategy

* `ga_sessions_flat`: atomic per-date partition replacement. Each date's rows
  load into the `ga_sessions_flat$YYYYMMDD` partition decorator with
  `WRITE_TRUNCATE`. Re-running any date converges to the same state; no
  duplicates, no cleanup DML, no partial-state window.
* `daily_visits`: MERGE on `visit_date` from an expiring temp staging table. A
  range load touches many one-row partitions, so an upsert is the simpler
  idempotent shape at this size.
* Within a batch, sessions are deduplicated on the natural grain
  `(full_visitor_id, visit_id, visit_start_time)` (first occurrence wins); the
  dropped count is logged and re-verified post-load.
* Both loads are therefore safe to rerun from the CLI or from Airflow retries at
  any time.

## Partitioning, clustering, and what we would not full-scan

* `ga_sessions_flat`: `PARTITION BY session_date`, `CLUSTER BY geo_country,
  device_category`, mirroring the API's own filters and the expected dashboard
  slices. Date-bounded queries prune partitions; country/device predicates
  benefit from clustering.
* `daily_visits`: partitioned by `visit_date` for uniform per-date operations;
  at ~366 rows this is about operational symmetry rather than cost.
* Not to be full-scanned: any query on `ga_sessions_flat` without a
  `session_date` predicate, and `SELECT *` anywhere — BigQuery bills per column
  scanned. The Step 5 summary queries follow both rules, and the NL→SQL
  guardrails enforce them in code.

## Data quality: the concrete list

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
11. cross-endpoint reconciliation: `daily_visits.visits` vs summed session visits
    per date. A hard failure only when one side is entirely missing (a
    broken/partial load); metric drift is recorded and warned on, because the
    endpoints demonstrably count visits differently (32% gap on 2016-08-01, see
    [01-api-exploration.md](01-api-exploration.md))

Any violation raises `DataQualityError` and fails the CLI/Airflow task. By
construction reconciliation runs *after* both loads landed, so even its failure
mode leaves data present but flagged untrusted. The standalone query in
[`ga_pipeline/sql/reconciliation.sql`](../ga_pipeline/sql/reconciliation.sql)
mirrors the in-pipeline gate so the ad-hoc answer and the automated one agree.

## Error handling & retries

* HTTP: retrying adapter (exponential backoff, honours `Retry-After`) for
  429/5xx inside each attempt; explicit timeouts on every request.
* Taxonomy: `TransientApiError` / `TransientLoadError` (retry-worthy) vs
  `FatalApiError` / `SchemaDriftError` / `DataQualityError` (fail fast), so
  Airflow's 2 retries are spent only where a retry can help.
* BigQuery: transient 429/5xx retried with exponential backoff (4 attempts);
  staging tables carry a 1-hour expiry so failed runs cannot leak storage.
