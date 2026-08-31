# What the API actually does

Findings from two probes against the live API:

* `scripts/explore_api.sh` — nine curl calls covering the contract, pagination
  and errors. Log: `artifacts/reports/api_exploration.log`
* `scripts/profile_api.py` — profiles 600 records across six days, including
  Christmas and Black Friday, and probes limits, ordering and rate limiting.
  Report: `artifacts/reports/api_profile_report.md`

Regenerate both:

```bash
set -a; source .env; set +a
./scripts/explore_api.sh | tee artifacts/reports/api_exploration.log
uv run python scripts/profile_api.py --sections profile,limits,stability,ratelimit
```

## The two endpoints

| | `/daily-visits` | `/ga-sessions-data` |
|---|---|---|
| Shape | flat: `visit_date`, `total_visits` | nested, camelCase (GA export style) |
| Date param | `start_date`/`end_date`, `YYYY-MM-DD` | `date`, `YYYYMMDD` |
| Range | 2016-08-01 to 2017-08-02 (367 days) | 2016-08-01 to 2017-08-01 |
| Filters | none | `country`, `device_category`, and `channel_grouping` |
| Auth | `X-API-Key` header | `X-API-Key` header |

Both proxy `bigquery-public-data.google_analytics_sample` — `metadata.dataset`
says so. Sessions come from day-sharded tables, which is why that endpoint takes
a single date.

## Envelope and pagination

Responses are `{filters_applied, metadata, pagination, records}`.

* `pagination.has_next` is the only safe stop condition. A page past the end
  returns HTTP 500, so a client must not probe for an empty page.
* `limit` is capped at 500 without saying so: `limit=5000` returns 500 rows.
  Default is 50.
* Results come back newest first. Ordering is stable across identical calls and
  pages do not overlap, so page-based crawling is safe on this static dataset.

## Fields

From 600 profiled records:

* Every expected top-level key is present in every record.
* Nullable metrics use `null`, not 0. `totals.bounces` is null in 51% of
  records, `totals.newVisits` in 18%, `trafficSource.keyword` in 72%,
  `adContent` in 99%. `totals.visits` is always 1.
* `device` has no `deviceCategory`, only `browser`, `operatingSystem` and
  `isMobile` — even though the endpoint accepts a `device_category` filter.
* Hits arrive as `hits_sample`, truncated to 3 entries, while `totals.hits`
  goes up to 67. Only `totals.hits` is the real count.
* Geo fields the sample dataset lacks hold the literal string
  `"not available in demo dataset"`: always for cityId, latitude, longitude and
  networkLocation, often for city, region and metro.
* `customDimensions` holds at most one entry, `{index: 4, value: <region>}`.
* `channel_grouping` really filters. On 2017-08-01 it narrows 2,556 sessions to
  1,346 for "Organic Search", and every record matches.
* `date` and `visitStartTime` can fall on different days. See
  [Two clocks](#two-clocks) below.

### Fields never observed

Five fields never appeared in 600 records, including peak shopping days:
`totals.transactions`, `totals.transactionRevenue`, `totals.timeOnSite`,
`trafficSource.campaign` and `trafficSource.isTrueDirect`.

All five exist and are populated in the underlying public dataset. This API
simply does not return them. The columns are kept anyway: the table models the
source, and the fields would start arriving if the API exposed them.
`test_ga_export_only_fields_map_when_present` feeds them in to check they land
in the right columns, which is all the coverage possible without live data.

`customDimensions` and `hits[]` are left out on purpose — they can carry
client-injected PII. See [airflow.md](airflow.md).

Three fields the API does populate are kept after a second look at the profile:
`trafficSource.referralPath` (~47% of sessions on 2016-08-01),
`geoNetwork.metro` and `geoNetwork.networkDomain`. `geo_metro` completes the geo
hierarchy between region and city. `geo_network_domain` is a quasi-identifier
and carries a caveat in [airflow.md](airflow.md).

Dropped: `adContent`, populated in under 1% of records, and `cityId`,
`latitude`, `longitude` and `networkLocation`, which held nothing but the
placeholder string in every record.

## Errors and limits

| Input | Response |
|---|---|
| Missing or empty API key | 401 UNAUTHENTICATED |
| Date outside range | 500 |
| Malformed date | 500 |
| Page past the end | 500 |
| `limit=0` | 400 |

Most bad input comes back as 500, so client errors arrive dressed as server
errors. A retry policy treating 500 as transient would retry a typo'd date
until the backoff budget ran out. The pipeline validates and clamps date
windows before calling.

No rate limiting showed up: 20 rapid requests, all 200, no `Retry-After`.
Latency is 2-2.5s per call, up to ~7.5s on the first.

## Two clocks

The two endpoints disagree about when a day starts, and this drives the
reconciliation design.

`/ga-sessions-data` shards on GA's `date`, stamped in the property's local
timezone. Every shard runs 07:00Z to 07:00Z the next day, so the property is on
UTC-7. `/daily-visits` counts by UTC date. A session at 17:00 local on
2016-08-01 is 00:00 UTC on 2016-08-02: Aug 1 in the session shard, Aug 2 in the
daily total.

The calculation confirmed it. Of the 1,711 sessions dated 2016-08-01, 415 start
after midnight UTC, and 1,711 − 415 = 1,296, which is `daily_visits` for that
date.

Sessions spilling forward are replaced by sessions spilling in from the day
before, but the two only cancel when volume is flat, so the gap is really the
day-over-day *change* in evening traffic. Across 2016-08-02..07 it ran 9.0,
6.9, 0.5, 9.8, 2.6 and 6.4 percent. 2017-08-01, a flat day, came in at 1.2%.
2016-08-01 is the extreme: nothing spills in at all, so the whole 415 shows up
as a 32% gap.

Two consequences:

* **Loading** filters on `session_date`, the partition column. It matches the
  shard the API served and is what the table is partitioned by.
* **Reconciliation** counts sessions by `DATE(visit_start_time)` across the
  `D-1` and `D` partitions, because that is the day `/daily-visits` counts.
  Counted that way the two agree exactly on every day of 2016-08-01..07, so any
  drift is a real load problem rather than a definition mismatch. Compared by
  shard date it exceeded 5% on six of those seven days and meant nothing.

## How the API exploration findings affected pipeline

1. Pagination stops on `has_next` rather than probing for an empty page.
2. The daily-visits transform reads `visit_date`/`total_visits`. The spec's
   `date`/`visits` are kept as fallbacks.
3. `device_category` is derived from `isMobile`, since the API omits the field
   and the clustering column would otherwise be entirely NULL. Tablets are not
   distinguishable.
4. The `hits_count` column was dropped. A count from the truncated
   `hits_sample` would be wrong and `totals_hits` already has it.
5. `"not available in demo dataset"` becomes NULL on ingest, so it cannot turn
   up as a value in a group-by.
6. Date windows are validated against the documented ranges before any request.
7. `page_size=500` matches the real server maximum.
8. Reconciliation compares UTC days, per [Two clocks](#two-clocks). It fails
   only when one side is missing entirely.

## Not tested, on purpose

* Filter semantics. The pipeline never uses filters, it pulls whole days.
* A full matrix of invalid inputs. Three probes established that bad input
  generally returns 500.
* The full-year daily sweep. Run it with
  `uv run python scripts/profile_api.py --sections sweep` (~366 calls).
