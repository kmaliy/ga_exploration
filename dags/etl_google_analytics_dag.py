"""Airflow DAG for the GA ETL.

Requirements implemented
------------------------
* Schedule: 06:00 and 18:00 on Wednesdays only, cron ``0 6,18 * * 3`` in
  Europe/Berlin.
  Can be changed via ``LOCAL_TZ`` var.
* Retries: 2 per task with a 5-minute delay.
* Timeout: 3 minutes per task (``execution_timeout``).

Design notes
------------
* Each run processes every day its own data interval touches (everything
  since the previous scheduled run, including the day in progress for the
  18:00 run), clamped to the dataset's documented range, so ``catchup=True``
  backfills deterministically and reruns are idempotent (partition
  replacement / MERGE inside the pipeline module).
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
DATASET_MIN_DATE = min(DAILY_VISITS_RANGE[0], GA_SESSIONS_RANGE[0])
DATASET_MAX_DATE = GA_SESSIONS_RANGE[1]


def _notify_failure(context: dict) -> None:
    """Failure hook: deterministic alert plus optional LLM triage.

    Registered per task via ``DEFAULT_ARGS`` (not on the DAG object): a
    task-level callback fires once the failing task has exhausted its
    retries, with the *failed* task's context — ``context["exception"]`` set
    and ``ti`` pointing at the failed task instance. A DAG-level callback
    gets neither (no exception, and ``ti`` is just the run's last task), so
    the triage playbook could never match the exception class there.

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


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=3),
    "on_failure_callback": _notify_failure,  # task-level: see _notify_failure docstring
}


def _window_from_context(context: dict) -> tuple[date, date] | None:
    """Days touched by this run's data interval, clamped to the dataset range.

    The interval is the half-open ``[data_interval_start, data_interval_end)``,
    so its last covered instant is one microsecond before the end — which is
    what decides the final day, NOT ``data_interval_end.date() - 1 day``. The
    two differ for intervals that end mid-day: with this schedule the 18:00
    run covers Wednesday 06:00-18:00, and subtracting a whole day made its
    window empty, so the second run of the day never processed anything. Now
    it (re)loads Wednesday; loads are idempotent, and the day's final state
    lands with the next run that touches it.
    """
    start = context["data_interval_start"].date()
    end = (context["data_interval_end"] - timedelta(microseconds=1)).date()
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
    description="GA API -> BigQuery: daily visits + flattened sessions",
    schedule="0 6,18 * * 3",  # Wednesdays 06:00 and 18:00 (Europe/Berlin)
    start_date=pendulum.datetime(2016, 8, 3, tz=LOCAL_TZ),  # first Wednesday in range
    catchup=False,  # flip to True to backfill the historical window
    default_args=DEFAULT_ARGS,  # includes the per-task failure callback
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
