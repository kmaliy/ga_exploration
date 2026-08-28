from datetime import date

import pytest

from ga_pipeline import quality
from ga_pipeline.exceptions import DataQualityError

DAY = date(2016, 8, 1)


def make_session_row(**overrides):
    row = {
        "session_date": "2016-08-01",
        "full_visitor_id": "1",
        "visit_id": 100,
        "visit_start_time": "2016-08-01T10:00:00+00:00",
        "totals_visits": 1,
        "totals_hits": 3,
        "totals_pageviews": 2,
        "totals_time_on_site_seconds": 30,
        "totals_transactions": None,
    }
    row.update(overrides)
    return row


class TestSessionsPreLoad:
    def test_clean_batch_passes(self):
        rows = [make_session_row(), make_session_row(full_visitor_id="2")]
        quality.check_sessions_pre_load(rows, DAY, dropped_duplicates=0).raise_if_failed()

    def test_empty_batch_fails(self):
        with pytest.raises(DataQualityError, match="empty batch"):
            quality.check_sessions_pre_load([], DAY, 0).raise_if_failed()

    def test_null_required_field_fails(self):
        rows = [make_session_row(full_visitor_id=None)]
        with pytest.raises(DataQualityError, match="full_visitor_id"):
            quality.check_sessions_pre_load(rows, DAY, 0).raise_if_failed()

    def test_wrong_partition_date_fails(self):
        rows = [make_session_row(session_date="2016-08-02")]
        with pytest.raises(DataQualityError, match="outside partition date"):
            quality.check_sessions_pre_load(rows, DAY, 0).raise_if_failed()

    def test_duplicate_grain_fails(self):
        rows = [make_session_row(), make_session_row()]
        with pytest.raises(DataQualityError, match="dedupe is broken"):
            quality.check_sessions_pre_load(rows, DAY, 0).raise_if_failed()

    def test_negative_metric_fails(self):
        rows = [make_session_row(totals_hits=-1)]
        with pytest.raises(DataQualityError, match="negative"):
            quality.check_sessions_pre_load(rows, DAY, 0).raise_if_failed()

    def test_violations_are_aggregated(self):
        rows = [make_session_row(totals_hits=-1, session_date="2016-08-02")]
        with pytest.raises(DataQualityError, match="2 data-quality violation"):
            quality.check_sessions_pre_load(rows, DAY, 0).raise_if_failed()


class TestDailyVisitsPreLoad:
    def test_clean_batch_passes(self):
        rows = [
            {"visit_date": "2016-08-01", "visits": 10, "loaded_at": "x"},
            {"visit_date": "2016-08-02", "visits": 0, "loaded_at": "x"},
        ]
        quality.check_daily_visits_pre_load(rows, DAY, date(2016, 8, 2)).raise_if_failed()

    def test_duplicate_date_fails(self):
        rows = [
            {"visit_date": "2016-08-01", "visits": 10, "loaded_at": "x"},
            {"visit_date": "2016-08-01", "visits": 11, "loaded_at": "x"},
        ]
        with pytest.raises(DataQualityError, match="duplicate visit_date"):
            quality.check_daily_visits_pre_load(rows, DAY, DAY).raise_if_failed()

    def test_out_of_window_date_fails(self):
        rows = [{"visit_date": "2016-09-01", "visits": 1, "loaded_at": "x"}]
        with pytest.raises(DataQualityError, match="outside requested window"):
            quality.check_daily_visits_pre_load(rows, DAY, DAY).raise_if_failed()

    def test_negative_visits_fails(self):
        rows = [{"visit_date": "2016-08-01", "visits": -5, "loaded_at": "x"}]
        with pytest.raises(DataQualityError, match="negative visits"):
            quality.check_daily_visits_pre_load(rows, DAY, DAY).raise_if_failed()


class FakeLoader:
    """Stub of BigQueryLoader.query_rows driven by a scripted result list."""

    def __init__(self, results):
        self._results = list(results)
        self.queries = []

    def table_id(self, name):
        return f"p.d.{name}"

    def query_rows(self, sql, params=None):
        self.queries.append(sql)
        return self._results.pop(0)


class TestSessionsPostLoad:
    def test_all_green(self):
        loader = FakeLoader(
            [
                [{"n": 2}],
                [{"dupes": 0}],
                [{"null_visitor": 0, "null_visit": 0}],
            ]
        )
        quality.check_sessions_post_load(loader, DAY, expected_count=2).raise_if_failed()

    def test_count_mismatch_fails(self):
        loader = FakeLoader(
            [
                [{"n": 1}],
                [{"dupes": 0}],
                [{"null_visitor": 0, "null_visit": 0}],
            ]
        )
        with pytest.raises(DataQualityError, match="loaded count 1 != expected 2"):
            quality.check_sessions_post_load(loader, DAY, expected_count=2).raise_if_failed()

    def test_destination_duplicates_fail(self):
        loader = FakeLoader(
            [
                [{"n": 2}],
                [{"dupes": 1}],
                [{"null_visitor": 0, "null_visit": 0}],
            ]
        )
        with pytest.raises(DataQualityError, match="duplicate session grain"):
            quality.check_sessions_post_load(loader, DAY, expected_count=2).raise_if_failed()


class TestReconciliation:
    def test_within_tolerance_passes(self):
        loader = FakeLoader([[{"daily_visits": 100, "session_visits": 97}]])
        quality.check_reconciliation(loader, DAY).raise_if_failed()

    def test_beyond_tolerance_warns_but_does_not_fail(self):
        # Live finding: the endpoints count visits differently (2016-08-01:
        # 1711 sessions vs 1296 daily visits) — drift is a signal, not a gate.
        loader = FakeLoader([[{"daily_visits": 100, "session_visits": 132}]])
        report = quality.check_reconciliation(loader, DAY)
        report.raise_if_failed()  # must not raise
        assert any("exceeds" in warning for warning in report.warnings)

    def test_zero_sessions_with_visits_fails(self):
        loader = FakeLoader([[{"daily_visits": 100, "session_visits": 0}]])
        with pytest.raises(DataQualityError, match="zero sessions loaded"):
            quality.check_reconciliation(loader, DAY).raise_if_failed()

    def test_zero_visits_with_sessions_fails(self):
        loader = FakeLoader([[{"daily_visits": 0, "session_visits": 50}]])
        with pytest.raises(DataQualityError, match="daily_visits=0"):
            quality.check_reconciliation(loader, DAY).raise_if_failed()

    def test_missing_daily_row_is_warning_only(self):
        loader = FakeLoader([[{"daily_visits": None, "session_visits": 50}]])
        quality.check_reconciliation(loader, DAY).raise_if_failed()  # warns, no raise
