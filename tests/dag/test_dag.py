"""DAG contract tests. Skipped automatically where Airflow is not installed."""

import importlib.util
from datetime import date, timedelta

import pytest

pytestmark = pytest.mark.dag

airflow = pytest.importorskip("airflow")

import pendulum  # noqa: E402 - Airflow dependency, only importable past the skip
from airflow.models import DagBag  # noqa: E402

BERLIN = "Europe/Berlin"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder="dags", include_examples=False)
    assert not bag.import_errors, f"DAG import errors: {bag.import_errors}"
    return bag.dags["etl_google_analytics"]


@pytest.fixture(scope="module")
def window_fn():
    spec = importlib.util.spec_from_file_location("etl_dag_under_test", "dags/etl_google_analytics_dag.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module._window_from_context  # noqa: SLF001 - the window contract is what these tests pin down


def _interval(start, end):
    return {"data_interval_start": start, "data_interval_end": end}


def test_schedule_is_wednesdays_six_and_eighteen(dag):
    assert dag.schedule_interval == "0 6,18 * * 3"
    assert str(dag.timezone) == "Europe/Berlin"


def test_retry_and_timeout_policy(dag):
    for task in dag.tasks:
        assert task.retries == 2, task.task_id
        assert task.retry_delay == timedelta(minutes=5), task.task_id
        assert task.execution_timeout == timedelta(minutes=3), task.task_id


def test_reconciliation_runs_after_both_loads(dag):
    reconcile = dag.get_task("reconcile_endpoints")
    assert reconcile.upstream_task_ids == {
        "extract_load_daily_visits",
        "extract_load_ga_sessions",
    }


def test_no_catchup_and_serialized_runs(dag):
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_morning_run_covers_everything_since_the_previous_run(window_fn):
    window = window_fn(
        _interval(
            pendulum.datetime(2016, 8, 3, 18, tz=BERLIN),
            pendulum.datetime(2016, 8, 10, 6, tz=BERLIN),
        )
    )
    assert window == (date(2016, 8, 3), date(2016, 8, 10))


def test_evening_run_processes_its_own_day(window_fn):
    # Regression: ``end.date() - 1 day`` made the 18:00 run's window empty
    # every single week, so the second scheduled run never did anything.
    window = window_fn(
        _interval(
            pendulum.datetime(2016, 8, 10, 6, tz=BERLIN),
            pendulum.datetime(2016, 8, 10, 18, tz=BERLIN),
        )
    )
    assert window == (date(2016, 8, 10), date(2016, 8, 10))


def test_midnight_aligned_interval_excludes_the_untouched_day(window_fn):
    # [Aug 8 00:00, Aug 9 00:00) touches only Aug 8: the half-open end must
    # not drag in a day the interval never covered.
    window = window_fn(
        _interval(
            pendulum.datetime(2016, 8, 8, tz=BERLIN),
            pendulum.datetime(2016, 8, 9, tz=BERLIN),
        )
    )
    assert window == (date(2016, 8, 8), date(2016, 8, 8))


def test_interval_outside_dataset_range_is_none(window_fn):
    window = window_fn(
        _interval(
            pendulum.datetime(2026, 8, 5, 6, tz=BERLIN),
            pendulum.datetime(2026, 8, 5, 18, tz=BERLIN),
        )
    )
    assert window is None


def test_failure_callback_is_task_level_on_every_task(dag):
    """Task-level (via default_args), not DAG-level: only the task-failure
    context carries ``exception`` and the failed ti, which triage needs to
    match the exception class to its playbook."""
    for task in dag.tasks:
        assert task.on_failure_callback, f"{task.task_id} has no failure callback"


def test_failure_callback_triages_and_redacts(dag, monkeypatch, caplog):
    """The failure hook must alert (with triage), never raise, and never leak secrets."""
    import logging

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)  # deterministic path, no network

    class FakeTI:
        dag_id = "etl_google_analytics"
        task_id = "extract_load_ga_sessions"
        try_number = 3

    callback = dag.get_task("extract_load_ga_sessions").on_failure_callback
    if isinstance(callback, list):
        callback = callback[0]
    context = {
        "ti": FakeTI(),
        "run_id": "manual__2016-08-03",
        "exception": ValueError("401 for key AIzaSyC_DGSGabcdefghij, contact ops@example.com"),
    }
    with caplog.at_level(logging.ERROR):
        callback(context)  # must not raise
    assert "extract_load_ga_sessions" in caplog.text
    assert "Next step" in caplog.text  # triage message made it into the alert
    assert "AIzaSyC_DGSG" not in caplog.text  # secrets redacted
    assert "ops@example.com" not in caplog.text  # PII redacted
