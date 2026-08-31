"""BigQuery loading with idempotent write strategies.

``ga_sessions_flat`` uses per-date partition replacement: each run loads one
day into the ``table$YYYYMMDD`` partition decorator with ``WRITE_TRUNCATE``,
so rerunning a date replaces exactly that partition and cannot duplicate
rows. ``daily_visits`` is upserted with MERGE from a temporary staging table
keyed on ``visit_date``; the staging table has a short expiry so failed runs
cannot leak storage.

Transient BigQuery errors (429/5xx) are retried with exponential backoff;
everything else fails fast as ``LoadError``.
"""

import logging
import time
import uuid
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any, TypeVar

from google.api_core import exceptions as gexc
from google.cloud import bigquery

from ga_pipeline.config import BigQuerySettings
from ga_pipeline.exceptions import LoadError, TransientLoadError
from ga_pipeline.load.schemas import (
    ALL_SPECS,
    DAILY_VISITS_SPEC,
    GA_SESSIONS_SPEC,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

_TRANSIENT_EXCEPTIONS = (
    gexc.TooManyRequests,
    gexc.InternalServerError,
    gexc.BadGateway,
    gexc.ServiceUnavailable,
    gexc.GatewayTimeout,
    gexc.DeadlineExceeded,
)
_MAX_ATTEMPTS = 4
_BACKOFF_BASE_SECONDS = 2.0
_STAGING_EXPIRY_MS = 60 * 60 * 1000  # 1 hour safety net for orphaned staging tables


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


class BigQueryLoader:
    """Owns dataset/table lifecycle and all writes to BigQuery."""

    def __init__(self, settings: BigQuerySettings, client: bigquery.Client | None = None) -> None:
        self._settings = settings
        self._client = client or bigquery.Client(project=settings.project, location=settings.location)

    def ensure_dataset(self) -> None:
        """Create the destination dataset if it does not exist (idempotent)."""
        dataset_ref = bigquery.Dataset(self._dataset_id)
        dataset_ref.location = self._settings.location
        self._with_retry(
            lambda: self._client.create_dataset(dataset_ref, exists_ok=True),
            context="create dataset",
        )

    def ensure_tables(self) -> None:
        """Create both destination tables with partitioning/clustering (idempotent)."""
        for spec in ALL_SPECS:
            table = bigquery.Table(self._table_id(spec.name), schema=spec.schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field=spec.partition_field
            )
            if spec.clustering_fields:
                table.clustering_fields = spec.clustering_fields
            table.description = spec.description
            self._with_retry(
                lambda t=table: self._client.create_table(t, exists_ok=True),
                context=f"create table {spec.name}",
            )

    def replace_sessions_partition(self, rows: list[dict[str, Any]], partition_date: date) -> int:
        """Atomically replace one day's partition of ``ga_sessions_flat``.

        Idempotent: re-running the same date always converges to the same
        state. An empty ``rows`` still truncates the partition: a rerun of a
        day that now legitimately has no data must not keep stale rows.
        """
        decorator = f"{self._table_id(GA_SESSIONS_SPEC.name)}${partition_date.strftime('%Y%m%d')}"
        job_config = bigquery.LoadJobConfig(
            schema=GA_SESSIONS_SPEC.schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        self._run_load_job(rows, decorator, job_config, context=f"sessions {partition_date}")
        logger.info("Replaced partition %s with %d row(s)", decorator, len(rows))
        return len(rows)

    def merge_daily_visits(self, rows: list[dict[str, Any]]) -> int:
        """Upsert daily-visit rows via a temporary staging table + MERGE."""
        if not rows:
            logger.info("No daily-visit rows to merge; skipping")
            return 0
        staging_id = self._table_id(f"_stg_{DAILY_VISITS_SPEC.name}_{uuid.uuid4().hex[:12]}")
        staging = bigquery.Table(staging_id, schema=DAILY_VISITS_SPEC.schema)
        staging.expires = _utcnow() + timedelta(milliseconds=_STAGING_EXPIRY_MS)
        try:
            self._with_retry(lambda: self._client.create_table(staging), context="create staging table")
            job_config = bigquery.LoadJobConfig(
                schema=DAILY_VISITS_SPEC.schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            self._run_load_job(rows, staging_id, job_config, context="daily visits staging")
            self._execute(self._merge_sql(staging_id), context="merge daily visits")
            logger.info("Merged %d daily-visit row(s)", len(rows))
            return len(rows)
        finally:
            self._client.delete_table(staging_id, not_found_ok=True)

    def query_rows(
        self,
        sql: str,
        params: list[bigquery.ScalarQueryParameter] | None = None,
        maximum_bytes_billed: int | None = None,
    ) -> list[dict[str, Any]]:
        """Run a parameterized query and return rows as dicts.

        ``maximum_bytes_billed`` makes BigQuery itself refuse the query if it
        would bill more than the cap (server-side cost guardrail for NL->SQL).
        """
        job_config = bigquery.QueryJobConfig(query_parameters=params or [])
        if maximum_bytes_billed is not None:
            job_config.maximum_bytes_billed = maximum_bytes_billed
        job = self._with_retry(lambda: self._client.query(sql, job_config=job_config), context="query")
        return [dict(row) for row in job.result()]

    def dry_run_bytes(self, sql: str) -> int:
        """Estimate bytes scanned by ``sql`` without running it (cost guardrail)."""
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = self._with_retry(
            lambda: self._client.query(sql, job_config=job_config), context="dry-run query"
        )
        return int(job.total_bytes_processed or 0)

    @property
    def dataset_id(self) -> str:
        """Fully qualified dataset id (project.dataset)."""
        return self._dataset_id

    def table_id(self, name: str) -> str:
        """Fully qualified table id for a table in the destination dataset."""
        return self._table_id(name)

    @property
    def _dataset_id(self) -> str:
        return f"{self._settings.project}.{self._settings.dataset}"

    def _table_id(self, name: str) -> str:
        return f"{self._dataset_id}.{name}"

    def _merge_sql(self, staging_id: str) -> str:
        target = self._table_id(DAILY_VISITS_SPEC.name)
        return f"""
        MERGE `{target}` AS target
        USING `{staging_id}` AS source
        ON target.visit_date = source.visit_date
        WHEN MATCHED THEN
          UPDATE SET visits = source.visits, loaded_at = source.loaded_at
        WHEN NOT MATCHED THEN
          INSERT (visit_date, visits, loaded_at)
          VALUES (source.visit_date, source.visits, source.loaded_at)
        """  # noqa: S608 - table ids from config

    def _run_load_job(
        self,
        rows: list[dict[str, Any]],
        destination: str,
        job_config: bigquery.LoadJobConfig,
        context: str,
    ) -> None:
        def _load() -> None:
            job = self._client.load_table_from_json(rows, destination, job_config=job_config)
            job.result()  # blocks; raises on failure
            if job.errors:
                raise LoadError(f"Load job for {context} reported errors: {job.errors}")

        self._with_retry(_load, context=f"load {context}")

    def _execute(self, sql: str, context: str) -> None:
        self._with_retry(lambda: self._client.query(sql).result(), context=context)

    @staticmethod
    def _with_retry(operation: Callable[[], T], context: str) -> T:
        """Retry transient BigQuery errors with exponential backoff."""
        last_exc: Exception | None = None
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                return operation()
            except _TRANSIENT_EXCEPTIONS as exc:
                last_exc = exc
                if attempt == _MAX_ATTEMPTS:
                    break
                sleep_for = _BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    "Transient BigQuery error on %s (attempt %d/%d): %s, retrying in %.0fs",
                    context,
                    attempt,
                    _MAX_ATTEMPTS,
                    exc,
                    sleep_for,
                )
                time.sleep(sleep_for)
            except gexc.GoogleAPICallError as exc:
                raise LoadError(f"BigQuery error on {context}: {exc}") from exc
        raise TransientLoadError(
            f"BigQuery {context} still failing after {_MAX_ATTEMPTS} attempts: {last_exc}"
        ) from last_exc
