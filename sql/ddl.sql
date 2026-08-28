-- DDL for the two destination tables.
-- The pipeline creates these automatically (idempotently) via ga_pipeline/bq_loader.py;
-- this file documents the exact partitioning/clustering intent for reviewers.
--
-- ${BQ_PROJECT} / ${BQ_DATASET} are placeholders matching the environment
-- variables in .env.example — substitute before running by hand, e.g.:
--   envsubst < sql/ddl.sql | bq query --use_legacy_sql=false

CREATE TABLE IF NOT EXISTS `${BQ_PROJECT}.${BQ_DATASET}.daily_visits` (
  visit_date DATE   NOT NULL OPTIONS (description = 'Calendar date of the visits total'),
  visits     INT64  NOT NULL OPTIONS (description = 'Total visits on visit_date'),
  loaded_at  TIMESTAMP NOT NULL OPTIONS (description = 'UTC load timestamp (lineage)')
)
PARTITION BY visit_date
OPTIONS (
  description = 'Daily total visits from /daily-visits. Tiny table; partitioned for uniform per-date operations, not cost.'
);

CREATE TABLE IF NOT EXISTS `${BQ_PROJECT}.${BQ_DATASET}.ga_sessions_flat` (
  session_date                       DATE    NOT NULL,
  full_visitor_id                    STRING  NOT NULL,
  visit_id                           INT64   NOT NULL,
  visit_number                       INT64,
  visit_start_time                   TIMESTAMP,
  channel_grouping                   STRING,
  totals_visits                      INT64,
  totals_hits                        INT64,
  totals_pageviews                   INT64,
  totals_bounces                     INT64,
  totals_new_visits                  INT64,
  -- The next four columns, plus traffic_campaign below, are populated in the
  -- source dataset (bigquery-public-data.google_analytics_sample) but are not
  -- returned by this API, so they stay NULL. See docs/API_EXPLORATION.md.
  totals_time_on_site_seconds        INT64,
  totals_transactions                INT64,
  totals_transaction_revenue_micros  INT64,
  traffic_source                     STRING,
  traffic_medium                     STRING,
  traffic_campaign                   STRING,
  traffic_keyword                    STRING,
  traffic_referral_path              STRING,
  traffic_is_true_direct             BOOL,
  device_category                    STRING,
  device_browser                     STRING,
  device_operating_system            STRING,
  device_is_mobile                   BOOL,
  geo_continent                      STRING,
  geo_sub_continent                  STRING,
  geo_country                        STRING,
  geo_region                         STRING,
  geo_metro                          STRING,
  geo_city                           STRING,
  geo_network_domain                 STRING,
  loaded_at                          TIMESTAMP NOT NULL
)
PARTITION BY session_date
CLUSTER BY geo_country, device_category
OPTIONS (
  description = 'Flattened GA sessions (one row per session). Grain: (full_visitor_id, visit_id, visit_start_time). Never query without a session_date predicate.'
);

-- Access-pattern notes (what we would NOT full-scan):
--   * Always constrain session_date — day partitioning makes date-bounded queries cheap.
--   * Country / device dashboards benefit from CLUSTER BY (geo_country, device_category).
--   * Avoid SELECT * — columnar billing charges per column scanned.
