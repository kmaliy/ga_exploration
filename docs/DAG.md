# DAG documentation — `etl_google_analytics`

## Schedule & windowing

Cron `0 6,18 * * 3` in **Europe/Berlin** (the assessment states 06:00/18:00
without a timezone; Berlin is the documented assumption — change `LOCAL_TZ` in
the DAG for UTC). Each run processes the days of its own data interval —
everything since the previous scheduled run — clamped to the dataset's
documented range (2016-08-01 … 2017-08-01), so backfills (`catchup=True`) are
deterministic and out-of-range runs are clean no-ops.

## Task graph

```
extract_load_daily_visits ──┐
                            ├──> reconcile_endpoints
extract_load_ga_sessions ───┘
```

Reconciliation is deliberately a separate downstream task: a cross-endpoint
mismatch is its own signal and must not prevent either load from landing.

## Retries, timeouts, failure behaviour

Per task: `retries=2`, `retry_delay=5 minutes`, `execution_timeout=3 minutes`.

The pipeline's error taxonomy makes those retries meaningful: HTTP 429/5xx,
timeouts and transient BigQuery errors surface as `TransientApiError` /
`TransientLoadError` (worth retrying — and the HTTP client additionally does
short exponential-backoff retries inside a single task attempt). Auth
failures, contract changes (`SchemaDriftError`) and `DataQualityError` are
fatal by design — retrying them twice with 5-minute delays would only delay
the alert. If retries are exhausted the task fails, downstream reconciliation
is skipped, and the failure callback fires.

**Reruns are safe at any time**: `ga_sessions_flat` reloads via atomic
per-date partition replacement and `daily_visits` via MERGE, so `airflow tasks
clear` / UI-retry never duplicates data.

The 3-minute per-task budget bounds the window size a run may process. The
regular Wednesday cadence (a 3.5-day interval, page size 500) fits
comfortably; for large historical backfills run `catchup=True` (one interval
per run) rather than widening a single run's window.

## Metrics to watch

Each ETL task pushes a small summary dict to XCom (`sessions_loaded` per date,
`daily_visits_loaded`, `duplicates_dropped`, `drift_warnings`) — metadata
only, never row data. In production, forward to StatsD/OpenTelemetry:

- `rows_extracted` / `rows_loaded` per endpoint per date (sudden drops = upstream trouble)
- `duplicates_dropped` (should be ~0; a spike means upstream started double-serving)
- `drift_warnings` count (non-empty = API contract evolving)
- reconciliation drift % between `daily_visits.visits` and session counts
- task duration vs the 3-minute timeout (alert at >70% sustained)
- DAG-level: run success rate, time since last success (freshness SLA: 1 scheduled interval + 2h)

## Alerting

`on_failure_callback` fires after retries are exhausted; the shipped hook logs
task metadata and marks where to post to Slack/PagerDuty (webhook URL from an
environment variable, never in code). Recommended policy: page on two
consecutive failed runs or any `DataQualityError`; ticket (no page) on drift
warnings and single transient failures. Alert messages carry dag/task/run ids
and a *redacted* error only — never payload rows.

The hook enriches the alert via `ga_pipeline/llm_triage.py` (Step 5 bonus),
in that order of trust: regex redaction strips secrets and PII-shaped strings
(emails, API keys, bearer tokens, `key=value` credentials) before anything is
logged or leaves the process; a fixed playbook maps the exception taxonomy to
a first response (`SchemaDriftError` → "do not rerun until schemas are
updated", `TransientLoadError` → "check BigQuery status, then clear — loads
are idempotent", …); and, only when `ANTHROPIC_API_KEY` is set, an LLM adds a
short narrative that is explicitly labeled advisory. `triage_failure` never
raises and degrades to the deterministic message, so the enrichment can never
break the alert path itself — a contract covered by tests
(`tests/test_llm_triage.py`, `tests/test_dag.py`).

## PII

The brief asks specifically about `customDimensions`, so start there, but the
loaded columns carry risk of their own. Four groups, from most to least
sensitive.

**Not loaded at all.** `customDimensions` and `hits[]` are free-form
containers filled by whoever implemented the tracking, so they routinely hold
user IDs, email addresses or order numbers. Nothing in the API contract
constrains them, which makes the risk unbounded. The pipeline never reads
them, so they cannot reach BigQuery: `flatten_session` pulls only from
`totals`, `trafficSource`, `device` and `geoNetwork`. `userId` and `clientId`
are handled the same way. All four are on the schema-drift allowlist, so if
the API starts sending them the run does not warn and still does not store
them. Raw payloads are never logged and never pushed to XCom; alert messages
carry task metadata only.

**Loaded, and personal data.** `full_visitor_id` is a pseudonymous cookie
identifier. It is the session grain, so it has to be stored, but it is
personal data under GDPR and should be treated as such at dataset level:
restricted IAM, a BigQuery policy tag on the column where column-level
security is available, and partition expiry aligned to the retention policy.
1,569 distinct visitors appear in the 1,711 sessions on 2016-08-01.

**Loaded, and free text that could incidentally carry PII.**
`traffic_keyword` holds whatever a user typed into a search box, which is a
well-known leak path — people type their own email address or order number
into site search. On 2016-08-01 the values are harmless (387 non-null, 361 of
them `(not provided)`, the rest product searches), but the six-day profile
turned up opaque identifier-shaped values such as `1X4Me6ZKNV0zg-jV`.
`traffic_referral_path` is the same kind of risk, lower: URL paths can embed
identifiers. Both are loaded as-is. The risk is accepted rather than
mitigated, because this dataset is a public sample; on real traffic they
would need pattern-based redaction on ingest.

**Loaded, and quasi-identifying in combination.** `geo_city`, `geo_metro`,
`geo_region`, `device_browser` and `device_operating_system` identify nobody
alone, but narrow a population fast in combination — 37 distinct cities across
1,569 visitors on a single day. They are kept because country and device are
the clustering keys and the point of the table, but they are the reason the
dataset should not be broadly readable.

`geo_network_domain` belongs in this group and sits at the sensitive end of
it. The ISP domain is unremarkable for consumer networks (`virginm.net`,
`cox.net`), but corporate and institutional domains name the visitor's
organisation outright — `kingston.ac.uk` in the sample identifies a specific
university. It is loaded because network domain is a standard GA reporting
dimension and this dataset is a public sample. On real traffic it should be
treated like `full_visitor_id`: policy-tagged, or reduced to a category
(consumer ISP / corporate / education) at ingest rather than stored raw.

### Why not load everything and mask it in BigQuery?

BigQuery can restrict a column with policy tags and dynamic data masking, and
that is the right tool for `full_visitor_id` above: the column is needed, so
it is stored and access-controlled. It is the wrong tool for
`customDimensions`. Masking is column-level, and `customDimensions` is a
repeated `{index, value}` structure, so there is no way to mask only the
identifier-like values inside it — the choice is the whole column or nothing,
and a fully masked column gives analysts nothing while the raw values sit in
the table. Masking also governs access, not storage: retention obligations,
right-to-erasure exposure and breach blast radius all attach to what is
stored, and the set of roles that can read unmasked drifts over time. Data
minimisation (GDPR Art. 5(1)(c)) asks for the field not to be collected when
it has no defined purpose, which is the case here.

The usual counter-argument — you cannot recover what you never collected —
does not apply: the source is a static public dataset over a fixed date range,
so a backfill can add the field later if a real use case appears.

If custom dimensions later become a requirement: allowlist specific indexes
after a privacy review, hash or tokenize identifier-like values on ingest
before they reach BigQuery, route them to a separate table with restricted IAM
and a partition expiry matching the retention policy, and document the lawful
basis.
