"""DAG contract tests. Skipped automatically where Airflow is not installed."""

from datetime import timedelta

import pytest

pytestmark = pytest.mark.dag

airflow = pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder="dags", include_examples=False)
    assert not bag.import_errors, f"DAG import errors: {bag.import_errors}"
    return bag.dags["etl_google_analytics"]


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
