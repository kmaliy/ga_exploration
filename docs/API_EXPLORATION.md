# API Exploration: Findings

Step 1 results. Two tools were used against the live API:

- `scripts/explore_api.sh`: nine curl probes covering the contract, pagination
  and error handling. Log: `sample_outputs/api_exploration.log`
- `scripts/profile_api.py`: profiles 600 records across six days (including
  Black Friday and Christmas) and probes limits, ordering and rate limiting.
  Report: `sample_outputs/api_profile_report.md`

## Endpoints at a glance

| | `/daily-visits` | `/ga-sessions-data` |
|---|---|---|
| Shape | flat: `visit_date`, `total_visits` | nested, camelCase (GA export style) |
| Date param | `start_date`/`end_date`, `YYYY-MM-DD` | `date`, `YYYYMMDD` (formats differ between endpoints) |
| Data range | 2016-08-01 to 2017-08-02 (367 days) | 2016-08-01 to 2017-08-01 |
| Filters | none | `country`, `device_category`, plus `channel_grouping`: not in the brief, but verified to filter (see below) |
| Auth | `X-API-Key` header | `X-API-Key` header |

Both endpoints proxy `bigquery-public-data.google_analytics_sample`
(`metadata.dataset` says so directly). Sessions come from day-sharded tables
(`ga_sessions_YYYYMMDD`), which explains why the endpoint takes a single date.

## Envelope and pagination

Every response has the shape `{filters_applied, metadata, pagination, records}`.

- `pagination.has_next` is the only reliable stop condition: requesting a page
  past the end returns HTTP 500, so a client must not probe past the last page.
- `limit` is silently capped at 500 (`limit=5000` returns 500 rows). Default is 50.
- Results are ordered newest first. Ordering is stable across identical calls
  and pages do not overlap, so page-based crawling works on this static dataset.
- Daily visits has 367 records total, matching the documented range.

## Field-level findings (600 records profiled)

- All expected top-level keys are present in every record; naming is camelCase.
- Nullable metrics use `null`, not 0: `totals.bounces` is null in 51% of
  records, `totals.newVisits` in 18%, `trafficSource.keyword` in 72%,
  `adContent` in 99%. `totals.visits` is always 1.
- `device` has no `deviceCategory` field (only `browser`, `operatingSystem`,
  `isMobile`), although the endpoint accepts a `device_category` filter.
- Hits arrive as `hits_sample`, truncated to at most 3 entries while
  `totals.hits` goes up to 67. Only `totals.hits` reflects the real count.
- Geo fields the demo dataset does not provide contain the literal string
  `"not available in demo dataset"` (always for cityId, latitude, longitude,
  networkLocation; often for city, region, metro).
- `customDimensions` holds at most one entry: `{index: 4, value: <region>}`.
- The undocumented `channel_grouping` filter really filters: on 2017-08-01 it
  narrows 2,556 sessions to 1,346 for "Organic Search", and every returned
  record matches the filter value.
- Value domains look sane: 7 channel groupings, 5 continents,
  `visitNumber` up to 52.
- `date` and `visitStartTime` can fall on different days. GA stamps `date` in
  the property's own timezone (Pacific for this store) and `visitStartTime` in
  UTC, so late-evening sessions land on the next UTC day: 415 of the 1,711
  sessions on 2016-08-01 (24%) carry a `visit_start_time` dated 2016-08-02.
  `session_date` is the partition column and the one to filter on;
  `DATE(visit_start_time)` would silently miss a quarter of the day.

## Fields never observed

Across 600 records, including peak shopping days, five fields never appeared:
`totals.transactions`, `totals.transactionRevenue`, `totals.timeOnSite`,
`trafficSource.campaign` and `trafficSource.isTrueDirect`.

All five exist and are populated in the underlying source,
`bigquery-public-data.google_analytics_sample`, which `metadata.dataset`
names directly. This API just does not return them. The columns are therefore
kept: the table models the source, and the fields would start arriving if the
API exposed them. They stay NULL until then, and
`test_ga_export_only_fields_map_when_present` feeds them in to check they land
in the right columns, which is the only coverage possible without live data.

The table is not a straight copy of the export: `customDimensions` and
`hits[]` are deliberately left out because they can carry client-injected PII
(see `docs/DAG.md`).

Three fields the API does populate are kept after a second pass over the
profile: `trafficSource.referralPath` (about 47% of the sessions on
2016-08-01) as `traffic_referral_path`, and `geoNetwork.metro` and
`geoNetwork.networkDomain` as `geo_metro` and `geo_network_domain`.
`geo_metro` completes the geo hierarchy between region and city;
`geo_network_domain` is a quasi-identifier and carries a caveat in
`docs/DAG.md`.

Dropped on purpose: `adContent`, populated in under 1% of records, and
`cityId`, `latitude`, `longitude` and `networkLocation`, which contain nothing
but the demo placeholder in every record profiled.

## Errors, limits, latency

| Input | Response |
|---|---|
| Missing or empty API key | 401 UNAUTHENTICATED |
| Date outside range | 500 |
| Malformed date (`2016-08-01` where `20160801` is expected) | 500 |
| Page past the end | 500 |
| `limit=0` | 400 |

Most invalid input comes back as HTTP 500, i.e. client errors reported as
server errors. A retry policy that treats 500 as transient would retry a
typo'd date until the backoff budget is exhausted, so the pipeline validates
and clamps date windows before calling the API.

No rate limiting was observed (20 rapid requests, all 200, no `Retry-After`
header). Latency is roughly 2-2.5s per call, up to ~7.5s on the first call.

## What this changed in the pipeline

1. Pagination stops on `has_next` instead of probing for an empty page.
2. The daily-visits transform reads `visit_date`/`total_visits`; the
   spec-style `date`/`visits` names are kept as fallbacks.
3. `device_category` is derived from `isMobile` (mobile/desktop; tablets are
   not distinguishable), since the API omits the field and the clustering
   column would otherwise be entirely NULL.
4. The `hits_count` column was dropped: a count based on the truncated
   `hits_sample` would be wrong, and `totals_hits` already has the number.
5. The `"not available in demo dataset"` placeholder is converted to NULL on
   ingest so it cannot show up as a value in group-bys.
6. Date windows are validated against the documented ranges before any
   request is made.
7. `page_size=500` matches the server's actual maximum.
8. Cross-endpoint reconciliation was changed from a hard failure to a logged
   warning, because the two endpoints date a session differently.

   `/ga-sessions-data` is sharded on GA's `date` field, which is stamped in
   the property's local timezone (Pacific, for this store).
   `/daily-visits` counts by UTC date. A session at 17:00 Pacific on
   2016-08-01 is 00:00 UTC on 2016-08-02, so it sits in the Aug 1 session
   shard but in the Aug 2 daily total. The arithmetic is exact: of the 1,711
   sessions dated 2016-08-01, 415 start after midnight UTC, and
   1,711 - 415 = 1,296, which is precisely `daily_visits` for that date.

   On a normal day the sessions spilling forward are replaced by sessions
   spilling in from the day before, so the two counts agree to about a
   percent (2017-08-01: 2,556 vs 2,587, 1.2%). 2016-08-01 is the first day of
   the dataset, so nothing spills in and the whole 415 shows up as a 32% gap.
   It is the only day in the year that behaves this way.

   Regrouping the loaded sessions by `DATE(visit_start_time)` instead of by
   shard date matches `daily_visits` on every day of 2016-08-01..07, which
   confirms it. The reconciliation check therefore warns on drift and fails
   only when one side is entirely missing.

## Not tested, on purpose

- Filter semantics (case sensitivity, unknown values, combined filters): the
  pipeline never uses the filters, it always pulls whole days.
- A full matrix of invalid inputs: three probes were enough to establish that
  bad input generally returns 500.
- The full-range daily sweep with the per-day reconciliation curve can be run
  with `uv run python scripts/profile_api.py --sections sweep` (~366 calls).
