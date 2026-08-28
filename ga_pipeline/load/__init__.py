"""Load layer: BigQuery destination tables and the loader that writes them.

``schemas`` lives here rather than at package top level because the table
specs exist to drive table creation and load jobs — this is their only
consumer. The reference DDL in ``ga_pipeline/sql/ddl.sql`` documents the same
shape for reviewers.
"""

from ga_pipeline.load.bq_loader import BigQueryLoader
from ga_pipeline.load.schemas import (
    ALL_SPECS,
    DAILY_VISITS_SPEC,
    DAILY_VISITS_TABLE,
    GA_SESSIONS_SPEC,
    GA_SESSIONS_TABLE,
    TableSpec,
)

__all__ = [
    "ALL_SPECS",
    "DAILY_VISITS_SPEC",
    "DAILY_VISITS_TABLE",
    "GA_SESSIONS_SPEC",
    "GA_SESSIONS_TABLE",
    "BigQueryLoader",
    "TableSpec",
]
