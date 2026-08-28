"""Airflow DAG for the GA ETL.

Requirements implemented
------------------------
* Schedule: 06:00 and 18:00 on Wednesdays only, cron ``0 6,18 * * 3`` in
  Europe/Berlin (the task states clock times without a timezone; Berlin is
  the stated assumption, change ``LOCAL_TZ`` if the business runs on UTC).
* Retries: 2 per task with a 5-minute delay.
* Timeout: 3 minutes per task (``execution_timeout``).

Design notes
------------
* Each run processes the days of its own data interval (i.e. everything since
  the previous scheduled run), clamped to the dataset's documented range,
  so ``catchup=True`` backfills deterministically and reruns are idempotent
  (partition replacement / MERGE inside the pipeline module).
* Tasks only retry on transient errors by design: the pipeline raises
  ``TransientApiError`` / ``TransientLoadError`` for retryable conditions and
  fatal errors otherwise; retrying a 401 twice with 5-minute delays would
  only delay the alert.
* Reconciliation is a separate task downstream of both loads, so a
  cross-endpoint mismatch never blocks either load from landing and is
  reported as its own failure.

See docs/airflow.md for metrics, failure behaviour, alerting, and PII handling.
"""

import logging
from datetime import date, timedelta
from typing import Any

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from ga_pipeline.config import DAILY_VISITS_RANGE, GA_SESSIONS_RANGE

logger = logging.getLogger(__name__)

LOCAL_TZ = pendulum.timezone("Europe/Berlin")

# The dataset is fixed history; runs outside it become no-ops.
# Single source of truth: ga_pipeline.config, which the CLI validates against
# too. Both endpoints are clamped to the narrower (sessions) end date so the
# two tasks always share one window and reconciliation has both sides. The
# cost is the 2017-08-02 daily-visits row, which has no session counterpart to
# reconcile against; load it via the CLI if it is ever needed.
#
# Reconciliation counts sessions by UTC date, so day D reads the D-1 and D
# session partitions. Within a run the loop loads D-1 first; on the very
# first date of a backfill D-1 is absent and the check reports that rather
# than reading it as drift.
DATASET_MIN_DATE = min(DAILY_VISITS_RANGE[0], GA_SESSIONS_RANGE[0])
DATASET_MAX_DATE = GA_SESSIONS_RANGE[1]

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=3),
}


def _notify_failure(context: dict) -> None:
    """Failure hook: deterministic alert plus optional LLM triage.

    Keep payloads out of the message: task metadata and a *redacted* error
    only, no row data (PII hygiene, see docs/airflow.md). ``triage_failure``
    redacts secrets/PII-shaped strings before logging or any LLM call, maps
    the pipeline's exception taxonomy to a first response, and never raises;
    without ``ANTHROPIC_API_KEY`` the deterministic message is the alert.
    """
    from ga_pipeline.llm.triage import triage_failure

    task_instance = context["ti"]
    metadata = {
        "dag": task_instance.dag_id,
        "task": task_instance.task_id,
        "run": context.get("run_id"),
        "try": task_instance.try_number,
    }
    logger.error(
        "ETL failure dag=%s task=%s run=%s try=%s",
        metadata["dag"],
        metadata["task"],
        metadata["run"],
        metadata["try"],
    )
    exc = context.get("exception")
    error = f"{type(exc).__name__}: {exc}" if isinstance(exc, BaseException) else str(exc or "")
    summary = triage_failure(metadata, error=error)
    logger.error("Triage:\n%s", summary)
    # Production: post to an incident channel, e.g.
    # requests.post(os.environ["SLACK_WEBHOOK_URL"], json={...summary and task metadata...})


def _window_from_context(context: dict) -> tuple[date, date] | None:
    """Days covered by this run's data interval, clamped to the dataset range."""
    start = context["data_interval_start"].date()
    end = context["data_interval_end"].date() - timedelta(days=1)
    start, end = max(start, DATASET_MIN_DATE), min(end, DATASET_MAX_DATE)
    if start > end:
        return None
    return start, end


def _run_etl(endpoint: str, **context: Any) -> dict:
    from ga_pipeline.pipeline import run_range

    window = _window_from_context(context)
    if window is None:
        logger.info("Data interval outside dataset range; nothing to do")
        return {}
    start, end = window
    result = run_range(start_date=start, end_date=end, endpoint=endpoint, reconcile=False)
    return result.as_dict()  # XCom: small metric dict only, never row data


def _run_reconciliation(**context: Any) -> None:
    from ga_pipeline.pipeline import run_reconciliation

    window = _window_from_context(context)
    if window is None:
        return
    run_reconciliation(*window)


with DAG(
    dag_id="etl_google_analytics",
    description="Assessment GA API -> BigQuery: daily visits + flattened sessions",
    schedule="0 6,18 * * 3",  # Wednesdays 06:00 and 18:00 (Europe/Berlin)
    start_date=pendulum.datetime(2016, 8, 3, tz=LOCAL_TZ),  # first Wednesday in range
    catchup=False,  # flip to True to backfill the historical window
    default_args=DEFAULT_ARGS,
    on_failure_callback=_notify_failure,
    max_active_runs=1,  # loads are idempotent, but serialize to keep DQ signal clean
    tags=["etl", "bigquery", "google-analytics"],
    doc_md=__doc__,
) as dag:
    extract_load_daily_visits = PythonOperator(
        task_id="extract_load_daily_visits",
        python_callable=_run_etl,
        op_kwargs={"endpoint": "daily-visits"},
    )

    extract_load_ga_sessions = PythonOperator(
        task_id="extract_load_ga_sessions",
        python_callable=_run_etl,
        op_kwargs={"endpoint": "ga-sessions"},
    )

    reconcile_endpoints = PythonOperator(
        task_id="reconcile_endpoints",
        python_callable=_run_reconciliation,
    )

    [extract_load_daily_visits, extract_load_ga_sessions] >> reconcile_endpoints
