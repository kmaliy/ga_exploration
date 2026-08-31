# Airflow

`dags/etl_google_analytics_dag.py` runs Wednesdays at 06:00 and 18:00
Europe/Berlin (`0 6,18 * * 3`). Both endpoints load in parallel, then a separate
reconciliation task:

```
extract_load_daily_visits ──┐
                            ├──> reconcile_endpoints
extract_load_ga_sessions ───┘
```

Reconciliation is its own task because a cross-endpoint mismatch is a separate
signal and must not stop either load from landing.

Contract tests live in `tests/dag/` and run against a real Airflow 2.10 install
(`uv sync --group airflow`).

Run airflow UI if needed

```bash
cd ~/personal_projects/ga_exploration
uv sync --group airflow

set -a; source .env; set +a

export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

uv run airflow standalone
```

user: admin
password: saved to ./ga_exploration/.airflow/standalone_admin_password.txt

![Screenshot 2026-08-30 at 23.02.27.png](../../../../../var/folders/7n/z7rjtn3d69x8z_d1lk0h3kr80000gp/T/TemporaryItems/NSIRD_screencaptureui_6IUfLf/Screenshot%202026-08-30%20at%2023.02.27.png)

## Schedule and windowing

The task gives 06:00 and 18:00 without a timezone; Berlin is the assumption for the sake of this task,
and `LOCAL_TZ` in the DAG changes it. Each run processes its own data interval,
everything since the previous scheduled run, clamped to the dataset's range of
2016-08-01 to 2017-08-01. Backfills with `catchup=True` are deterministic and
out-of-range runs are no-ops.

GA's day starts seven hours after UTC's, so the sessions from one UTC day sit
in two partitions: the end of the previous day and most of the current one.
Reconciliation reads both. Days load in order, so the previous one is already
there. The first day of a backfill has no previous day loaded, so the count
comes up short. The check notices that partition is empty and says so, instead
of reporting a mismatch that looks like a broken load.

## Retries and timeouts

`retries=2`, `retry_delay=5 minutes`, `execution_timeout=3 minutes` per task.

Those retries only help because the error taxonomy separates what a retry can
fix. HTTP 429/5xx, timeouts and transient BigQuery errors surface as
`TransientApiError` or `TransientLoadError`. Auth failures, `SchemaDriftError`
and `DataQualityError` are fatal, so retrying them twice would only delay the
alert by ten minutes. When retries run out the task fails, reconciliation is
skipped, and the failure callback fires.

Reruns are safe at any point. `airflow tasks clear` and UI retries cannot
duplicate data, because sessions reload by partition replacement and
daily-visits by MERGE.

The 3-minute budget bounds how wide a window one run may process. The Wednesday
cadence, a 3.5-day interval at page size 500, fits comfortably. For long
historical backfills use `catchup=True` rather than widening a single run.

## Metrics

Each task pushes a small summary to XCom — `sessions_loaded` per date,
`daily_visits_loaded`, `duplicates_dropped`, `drift_warnings` — metadata only,
never rows. In production, forward to StatsD or OpenTelemetry:

* `rows_extracted` / `rows_loaded` per endpoint per date. Sudden drops mean
  upstream trouble.
* `duplicates_dropped`, normally ~0. A spike means the source started
  double-serving.
* `drift_warnings`, non-empty when the API contract is moving.
* Reconciliation drift, expected to be exactly 0. Anything else is a missed page
  or a partial load.
* Task duration against the 3-minute timeout; alert above 70% sustained.
* Run success rate and time since last success. Freshness SLA: one interval
  plus 2h.

## Alerting

`on_failure_callback` fires once retries are exhausted. The shipped hook logs
task metadata and marks where to post to Slack or PagerDuty, with the webhook
URL from an environment variable. Alerts carry dag, task and run ids plus a
redacted error, never payload rows.

Suggested policy: page on two consecutive failed runs or any
`DataQualityError`; ticket without paging on drift warnings and single
transient failures.

The hook enriches the alert through `ga_pipeline/llm/triage.py`, in decreasing
order of trust. Regex redaction strips secrets and PII-shaped strings before
anything is logged or leaves the process. A fixed playbook maps the exception
taxonomy to a first response: `SchemaDriftError` to "do not rerun until schemas
are updated", `TransientLoadError` to "check BigQuery status, then clear, loads
are idempotent". Only if `ANTHROPIC_API_KEY` is set does an LLM add a short
narrative, labelled advisory. `triage_failure` never raises and degrades to the
deterministic message, so the enrichment cannot break the alert path.
`tests/unit/test_llm_triage.py` and `tests/dag/test_dag.py` cover that contract.

## PII

The brief asks about `customDimensions`, but the loaded columns carry risk of
their own. Four groups, most sensitive first.

**Not loaded at all.** `customDimensions` and `hits[]` are free-form containers
filled by whoever implemented the tracking, so they routinely hold user ids,
email addresses or order numbers, and nothing in the API contract constrains
them. `flatten_session` reads only `totals`, `trafficSource`, `device` and
`geoNetwork`, so they cannot reach BigQuery. `userId` and `clientId` are treated
the same way. All four are on the schema-drift allowlist: if the API starts
sending them, the run does not warn and still does not store them. Raw payloads
are never logged or pushed to XCom.

**Loaded, and personal data.** `full_visitor_id` is a pseudonymous cookie id.
It is the session grain, so it has to be stored, but it is personal data under
GDPR: restricted IAM, a policy tag on the column where column-level security is
available, and partition expiry matching the retention policy. The 1,711
sessions on 2016-08-01 come from 1,569 distinct visitors.

**Loaded, free text that could carry PII incidentally.** `traffic_keyword` is
whatever someone typed into a search box, a well-known leak path — people type
their own email address or order number into site search. On 2016-08-01 the
values are harmless (387 non-null, 361 of them `(not provided)`), but the
six-day profile turned up identifier-shaped values like `1X4Me6ZKNV0zg-jV`.
`traffic_referral_path` is the same risk, lower: URL paths can embed
identifiers. Both are stored as-is. On real traffic they would need
pattern-based redaction on ingest.

**Loaded, quasi-identifying in combination.** `geo_city`, `geo_metro`,
`geo_region`, `device_browser` and `device_operating_system` identify nobody
alone but narrow a population fast together: 37 distinct cities across 1,569
visitors in one day. They stay because country and device are the clustering
keys and the point of the table, but they are why the dataset should not be
broadly readable.

`geo_network_domain` sits at the sensitive end of that group. Consumer ISP
domains are unremarkable (`virginm.net`, `cox.net`), but corporate and
institutional ones name the visitor's organisation outright — `kingston.ac.uk`
in the sample identifies a specific university. It is loaded because network
domain is a standard GA dimension and this is a public sample. On real traffic
it should be policy-tagged like `full_visitor_id`, or reduced at ingest to a
category: consumer, corporate, education.

### Why not load everything and mask it in BigQuery?

Policy tags and dynamic masking are the right tool for `full_visitor_id`: the
column is needed, so store it and control access.

They are the wrong tool for `customDimensions`. Masking is column-level, and
`customDimensions` is a repeated `{index, value}` structure, so there is no way
to mask only the identifier-like values inside it. The choice is the whole
column or nothing, and a fully masked column gives analysts nothing while the
raw values still sit in the table. Masking also governs access rather than
storage, and retention obligations, right-to-erasure exposure and breach blast
radius all attach to what is stored. GDPR Art. 5(1)(c) asks for the field not to
be collected when it has no defined purpose, which is the case here.

The usual objection, that you cannot recover what you never collected, does not
apply. The source is a static public dataset over a fixed range, so a backfill
can add the field if a real use case appears.

If custom dimensions become a requirement: allowlist specific indexes after a
privacy review, hash or tokenize identifier-like values before they reach
BigQuery, route them to a separate table with restricted IAM and a matching
partition expiry, and document the lawful basis.
