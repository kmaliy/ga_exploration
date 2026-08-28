# Pipeline design

## Running it

```bash
uv run data_pipeline.py run --start-date 2016-08-01 --end-date 2016-08-07
uv run data_pipeline.py run --start-date 2016-08-01 --end-date 2016-08-07 --endpoint ga-sessions
uv run data_pipeline.py run --start-date 2016-08-01 --end-date 2016-08-01 --dry-run
```

A dry run extracts, transforms and checks, then writes JSONL to
`artifacts/samples/` without touching BigQuery.

Dataset and tables are created on first run.
[`ga_pipeline/sql/ddl.sql`](../ga_pipeline/sql/ddl.sql) holds the same
definitions for reading, and `tests/unit/test_schema_ddl_parity.py` fails if
the two ever disagree.

## Idempotency

`ga_sessions_flat` loads one date at a time into the
`ga_sessions_flat$YYYYMMDD` partition decorator with `WRITE_TRUNCATE`. Rerunning
a date replaces exactly that partition. No cleanup DML, no window where the
table is half-written.

`daily_visits` is upserted with MERGE on `visit_date` from a staging table with
a one-hour expiry. A range load touches many one-row partitions, so an upsert
is simpler than partition replacement at this size.

Within a batch, sessions are deduplicated on `(full_visitor_id, visit_id,
visit_start_time)`, first occurrence winning. The dropped count is logged and
re-checked after the load.

Both loads are safe to rerun from the CLI or an Airflow retry.

## Partitioning and clustering

`ga_sessions_flat` is `PARTITION BY session_date`, `CLUSTER BY geo_country,
device_category` — the API's own filters, and the slices a dashboard asks for.

`daily_visits` is partitioned by `visit_date`. At ~366 rows that buys nothing on
cost; it keeps per-date operations uniform across both tables.

What not to scan: any query on `ga_sessions_flat` without a `session_date`
predicate, and `SELECT *` anywhere, since BigQuery bills per column. The
summary queries follow both rules and the NL→SQL guardrails enforce them.

## Quality gates

Pre-load, in memory, aggregated into a single failure:

1. batch is not empty
2. required fields present: `session_date`, `full_visitor_id`, `visit_id`, or
   `visit_date`, `visits`
3. every session row belongs to the date being loaded
4. grain is unique after dedupe
5. metrics are non-negative
6. daily-visits has no duplicate dates and stays inside the requested window
7. schema drift: missing required keys fail, unexpected new keys warn

Post-load, in SQL, before the table is trusted:

8. loaded row count matches the deduped batch
9. grain is unique in the destination partition
10. no NULL keys in the destination partition
11. reconciliation against `/daily-visits`

Reconciliation counts sessions by `DATE(visit_start_time)` across the `D-1` and
`D` partitions, because that is the day `/daily-visits` counts —
see [Two clocks](api.md#two-clocks). Counted that way the two sides agree
exactly, so the tolerance is 0 and drift means a missed page or a partial load.
The warning distinguishes three causes: an adjacent partition not yet loaded, a
NULL `visit_start_time` that cannot be placed on a UTC day, and a genuine gap.
Only a wholly missing side fails the run.

Any violation raises `DataQualityError` and fails the task. Reconciliation runs
after both loads land, so even its failure leaves the data present and flagged
rather than absent.
[`ga_pipeline/sql/reconciliation.sql`](../ga_pipeline/sql/reconciliation.sql)
is the same query to run by hand.

## Errors and retries

HTTP 429 and 5xx are retried inside a single attempt with exponential backoff,
honouring `Retry-After`. Every request has an explicit timeout.

The exception taxonomy decides what a retry is worth. `TransientApiError` and
`TransientLoadError` are worth retrying. `FatalApiError`, `SchemaDriftError` and
`DataQualityError` are not, so Airflow's two retries are not spent delaying an
alert.

BigQuery transient errors get four attempts with backoff. Staging tables expire
after an hour, so a failed run cannot leak storage.
