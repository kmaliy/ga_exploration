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

    def test_row_missing_the_date_field_fails_not_crashes(self):
        # Regression: this used to KeyError inside the check instead of
        # recording a violation.
        row = make_session_row()
        del row["session_date"]
        with pytest.raises(DataQualityError):
            quality.check_sessions_pre_load([row], DAY, 0).raise_if_failed()

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
    """Sessions are counted by the UTC date of visit_start_time, which is the day
    /daily-visits is counting, so the two sides are expected to agree exactly.
    """

    @staticmethod
    def _row(daily, sessions, prior_rows=500, undated=0):
        return [
            {
                "daily_visits": daily,
                "session_visits": sessions,
                "prior_partition_rows": prior_rows,
                "undated_rows": undated,
            }
        ]

    def test_exact_match_passes_silently(self):
        loader = FakeLoader([self._row(1963, 1963)])
        report = quality.check_reconciliation(loader, DAY)
        report.raise_if_failed()
        assert report.warnings == []

    def test_query_counts_by_utc_date_over_both_partitions(self):
        loader = FakeLoader([self._row(100, 100)])
        quality.check_reconciliation(loader, DAY)
        sql = loader.queries[0]
        assert "DATE(visit_start_time) = @d" in sql
        assert "session_date BETWEEN DATE_SUB(@d, INTERVAL 1 DAY) AND @d" in sql
        # partition predicate is still present, so this does not full-scan
        assert "session_date" in sql

    def test_any_drift_warns_but_does_not_fail(self):
        loader = FakeLoader([self._row(100, 97)])
        report = quality.check_reconciliation(loader, DAY)
        report.raise_if_failed()  # must not raise
        assert any("genuine discrepancy" in w for w in report.warnings)

    def test_drift_with_empty_prior_partition_says_so(self):
        loader = FakeLoader([self._row(1963, 1548, prior_rows=0)])
        report = quality.check_reconciliation(loader, DAY)
        report.raise_if_failed()
        assert any("backfilled" in w for w in report.warnings)

    def test_drift_with_undated_rows_says_so(self):
        loader = FakeLoader([self._row(100, 95, undated=5)])
        report = quality.check_reconciliation(loader, DAY)
        report.raise_if_failed()
        assert any("NULL visit_start_time" in w for w in report.warnings)

    def test_tolerance_can_absorb_a_known_gap(self):
        loader = FakeLoader([self._row(100, 97)])
        report = quality.check_reconciliation(loader, DAY, tolerance_pct=5.0)
        report.raise_if_failed()
        assert report.warnings == []

    def test_zero_sessions_with_visits_fails(self):
        loader = FakeLoader([self._row(100, 0)])
        with pytest.raises(DataQualityError, match="zero sessions loaded"):
            quality.check_reconciliation(loader, DAY).raise_if_failed()

    def test_zero_visits_with_sessions_fails(self):
        loader = FakeLoader([self._row(0, 50)])
        with pytest.raises(DataQualityError, match="daily_visits=0"):
            quality.check_reconciliation(loader, DAY).raise_if_failed()

    def test_missing_daily_row_is_warning_only(self):
        loader = FakeLoader([self._row(None, 50)])
        quality.check_reconciliation(loader, DAY).raise_if_failed()  # warns, no raise
