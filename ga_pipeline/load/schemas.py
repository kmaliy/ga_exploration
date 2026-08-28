"""BigQuery table specifications: schemas, partitioning, clustering.

Partitioning / clustering intent
--------------------------------
* ``ga_sessions_flat`` is day-partitioned on ``session_date`` and clustered by
  ``geo_country, device_category``, mirroring the API's own filters and
  the expected analytical access patterns. Date-bounded queries prune
  partitions; country/device slices benefit from clustering. What we would NOT
  full-scan: any query without a ``session_date`` predicate should be treated
  as a smell; dashboards must always constrain the partition column.
* ``daily_visits`` is tiny (one row per day); partitioning adds nothing for
  cost, but we still partition by ``visit_date`` so operational tooling
  (partition expiry, per-date reloads) works uniformly across both tables.
"""

from dataclasses import dataclass, field

from google.cloud import bigquery

DAILY_VISITS_TABLE = "daily_visits"
GA_SESSIONS_TABLE = "ga_sessions_flat"


@dataclass(frozen=True)
class TableSpec:
    """Everything needed to create/load one destination table."""

    name: str
    schema: list[bigquery.SchemaField]
    partition_field: str
    clustering_fields: list[str] = field(default_factory=list)
    description: str = ""


DAILY_VISITS_SPEC = TableSpec(
    name=DAILY_VISITS_TABLE,
    partition_field="visit_date",
    description="Daily total visits from the /daily-visits endpoint.",
    schema=[
        bigquery.SchemaField("visit_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("visits", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
    ],
)

GA_SESSIONS_SPEC = TableSpec(
    name=GA_SESSIONS_TABLE,
    partition_field="session_date",
    clustering_fields=["geo_country", "device_category"],
    description="Flattened GA sessions from the /ga-sessions-data endpoint (one row per session).",
    schema=[
        # Identity / grain: one row per (full_visitor_id, visit_id, visit_start_time)
        bigquery.SchemaField("session_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("full_visitor_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("visit_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("visit_number", "INTEGER"),
        bigquery.SchemaField("visit_start_time", "TIMESTAMP"),
        bigquery.SchemaField("channel_grouping", "STRING"),
        # totals.*
        bigquery.SchemaField("totals_visits", "INTEGER"),
        bigquery.SchemaField("totals_hits", "INTEGER"),
        bigquery.SchemaField("totals_pageviews", "INTEGER"),
        bigquery.SchemaField("totals_bounces", "INTEGER"),
        bigquery.SchemaField("totals_new_visits", "INTEGER"),
        # Populated in the source dataset (google_analytics_sample) but never
        # returned by this API, so NULL in practice: time_on_site, transactions,
        # transaction_revenue_micros, traffic_campaign, traffic_is_true_direct.
        bigquery.SchemaField("totals_time_on_site_seconds", "INTEGER"),
        bigquery.SchemaField("totals_transactions", "INTEGER"),
        bigquery.SchemaField("totals_transaction_revenue_micros", "INTEGER"),
        # trafficSource.*
        bigquery.SchemaField("traffic_source", "STRING"),
        bigquery.SchemaField("traffic_medium", "STRING"),
        bigquery.SchemaField("traffic_campaign", "STRING"),
        bigquery.SchemaField("traffic_keyword", "STRING"),
        bigquery.SchemaField("traffic_referral_path", "STRING"),
        bigquery.SchemaField("traffic_is_true_direct", "BOOLEAN"),
        # device.*
        bigquery.SchemaField("device_category", "STRING"),
        bigquery.SchemaField("device_browser", "STRING"),
        bigquery.SchemaField("device_operating_system", "STRING"),
        bigquery.SchemaField("device_is_mobile", "BOOLEAN"),
        # geoNetwork.*
        bigquery.SchemaField("geo_continent", "STRING"),
        bigquery.SchemaField("geo_sub_continent", "STRING"),
        bigquery.SchemaField("geo_country", "STRING"),
        bigquery.SchemaField("geo_region", "STRING"),
        bigquery.SchemaField("geo_metro", "STRING"),
        bigquery.SchemaField("geo_city", "STRING"),
        bigquery.SchemaField("geo_network_domain", "STRING"),
        # lineage
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
    ],
)

ALL_SPECS = (DAILY_VISITS_SPEC, GA_SESSIONS_SPEC)
