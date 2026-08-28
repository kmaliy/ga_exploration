import json
from datetime import date

import pytest

from ga_pipeline.extract.api_client import AssessmentApiClient
from ga_pipeline.pipeline import run_range


class FixtureBackedClient(AssessmentApiClient):
    """Client whose transport is replaced by fixtures — no network."""

    def __init__(self, sessions, daily_visits):
        # Deliberately no super().__init__: transport methods are overridden.
        self._sessions = sessions
        self._daily = daily_visits

    def iter_daily_visits(self, start_date, end_date):
        yield from [r for r in self._daily if start_date <= r["visit_date"] <= end_date]

    def iter_ga_sessions(self, ga_date):
        yield from [r for r in self._sessions if r["date"] == ga_date]


@pytest.fixture
def client(ga_sessions_raw, daily_visits_raw):
    return FixtureBackedClient(ga_sessions_raw, daily_visits_raw)


class TestDryRun:
    def test_full_dry_run_writes_jsonl_and_reports(self, client, tmp_path):
        result = run_range(
            start_date=date(2016, 8, 1),
            end_date=date(2016, 8, 1),
            dry_run=True,
            output_dir=tmp_path,
            client=client,
        )
        assert result.dry_run is True
        assert result.daily_visits_loaded == 1
        assert result.sessions_loaded == {"2016-08-01": 2}
        assert result.duplicates_dropped == 1  # fixture contains one exact dupe

        sessions_file = tmp_path / "ga_sessions_flat_2016-08-01.jsonl"
        lines = sessions_file.read_text().strip().splitlines()
        assert len(lines) == 2
        row = json.loads(lines[0])
        assert row["session_date"] == "2016-08-01"
        assert row["geo_country"] == "United Kingdom"

        visits_file = tmp_path / "daily_visits_2016-08-01_2016-08-01.jsonl"
        assert json.loads(visits_file.read_text().splitlines()[0])["visits"] == 1296

    def test_invalid_range_rejected(self, client):
        with pytest.raises(ValueError, match="before start_date"):
            run_range(
                start_date=date(2016, 8, 2),
                end_date=date(2016, 8, 1),
                dry_run=True,
                client=client,
            )

    def test_window_outside_available_range_is_skipped(self, client, tmp_path):
        # Live finding: out-of-range dates return HTTP 500 from the API, so
        # the pipeline clamps client-side and never issues those requests.
        result = run_range(
            start_date=date(2020, 1, 1),
            end_date=date(2020, 1, 2),
            dry_run=True,
            output_dir=tmp_path,
            client=client,
        )
        assert result.daily_visits_loaded == 0
        assert result.sessions_loaded == {}

    def test_endpoint_filter_daily_only(self, client, tmp_path):
        result = run_range(
            start_date=date(2016, 8, 1),
            end_date=date(2016, 8, 5),
            endpoint="daily-visits",
            dry_run=True,
            output_dir=tmp_path,
            client=client,
        )
        assert result.daily_visits_loaded == 5
        assert result.sessions_loaded == {}
