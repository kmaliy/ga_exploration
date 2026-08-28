from datetime import UTC, date, datetime

import pytest

from ga_pipeline import transform
from ga_pipeline.exceptions import FatalApiError, SchemaDriftError

LOADED_AT = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)


class TestParseGaDate:
    def test_yyyymmdd(self):
        assert transform.parse_ga_date("20160801") == date(2016, 8, 1)

    def test_iso(self):
        assert transform.parse_ga_date("2016-08-01") == date(2016, 8, 1)

    def test_garbage_raises(self):
        with pytest.raises(FatalApiError):
            transform.parse_ga_date("01/08/2016")


class TestFlattenSession:
    def test_happy_path_live_shape(self, ga_sessions_raw):
        row = transform.flatten_session(ga_sessions_raw[0], LOADED_AT)
        assert row["session_date"] == "2016-08-01"
        assert row["full_visitor_id"] == "1234567890123456789"
        assert row["visit_id"] == 1470046245
        assert row["visit_number"] == 1
        assert row["totals_pageviews"] == 4
        assert row["totals_time_on_site_seconds"] == 121
        assert row["device_category"] == "mobile"  # derived from isMobile (API omits it)
        assert row["device_is_mobile"] is True
        assert row["geo_country"] == "United Kingdom"
        assert row["geo_city"] == "London"
        assert row["geo_metro"] == "London"
        assert row["geo_network_domain"] == "virginm.net"
        assert row["visit_start_time"] == "2016-08-01T10:10:45+00:00"
        assert row["loaded_at"] == LOADED_AT.isoformat()

    def test_demo_sentinel_strings_become_null(self, ga_sessions_raw):
        row = transform.flatten_session(ga_sessions_raw[1], LOADED_AT)
        assert row["geo_city"] is None  # "not available in demo dataset"
        assert row["geo_region"] is None
        assert row["geo_metro"] is None  # "not available in demo dataset"
        assert row["geo_country"] == "United States"

    def test_referral_path_is_kept(self, ga_sessions_raw):
        assert transform.flatten_session(ga_sessions_raw[1], LOADED_AT)[
            "traffic_referral_path"
        ] == "/yt/about/"
        assert (
            transform.flatten_session(ga_sessions_raw[0], LOADED_AT)["traffic_referral_path"]
            is None
        )

    def test_ga_export_only_fields_map_when_present(self):
        """Columns kept for GA-export compatibility. This API never sends them,
        so this is the only coverage they get."""
        record = {
            "date": "20160801",
            "fullVisitorId": "1",
            "visitId": 2,
            "totals": {"transactions": 3, "transactionRevenue": 41990000},
            "trafficSource": {"campaign": "Data Share Promo", "isTrueDirect": True},
        }
        row = transform.flatten_session(record, LOADED_AT)
        assert row["totals_transactions"] == 3
        assert row["totals_transaction_revenue_micros"] == 41990000
        assert row["traffic_campaign"] == "Data Share Promo"
        assert row["traffic_is_true_direct"] is True

    def test_device_category_derived_desktop(self, ga_sessions_raw):
        row = transform.flatten_session(ga_sessions_raw[1], LOADED_AT)
        assert row["device_category"] == "desktop"  # isMobile False, no deviceCategory

    def test_explicit_device_category_wins_over_derivation(self):
        record = {
            "date": "20160801",
            "fullVisitorId": "1",
            "visitId": 2,
            "device": {"deviceCategory": "tablet", "isMobile": True},
        }
        row = transform.flatten_session(record, LOADED_AT)
        assert row["device_category"] == "tablet"

    def test_snake_case_variant(self):
        record = {
            "date": "20160801",
            "full_visitor_id": "42",
            "visit_id": "7",
            "traffic_source": {"source": "bing", "is_true_direct": "true"},
            "geo_network": {"country": "France"},
        }
        row = transform.flatten_session(record, LOADED_AT)
        assert row["full_visitor_id"] == "42"
        assert row["visit_id"] == 7
        assert row["traffic_source"] == "bing"
        assert row["traffic_is_true_direct"] is True
        assert row["geo_country"] == "France"

    def test_missing_optionals_are_null(self):
        record = {"date": "20160801", "fullVisitorId": "1", "visitId": 2}
        row = transform.flatten_session(record, LOADED_AT)
        assert row["totals_visits"] is None
        assert row["traffic_source"] is None
        assert row["device_category"] is None

    def test_missing_visit_id_raises(self):
        with pytest.raises(FatalApiError):
            transform.flatten_session({"date": "20160801", "fullVisitorId": "1"}, LOADED_AT)


class TestDedupe:
    def test_exact_duplicates_dropped(self, ga_sessions_raw):
        rows = [transform.flatten_session(r, LOADED_AT) for r in ga_sessions_raw]
        unique, dropped = transform.dedupe_sessions(rows)
        assert dropped == 1
        assert len(unique) == 2
        # First occurrence wins: the dupe carries bounces=1, the original null
        assert unique[1]["totals_bounces"] is None

    def test_no_duplicates(self):
        rows = [
            {"full_visitor_id": "a", "visit_id": 1, "visit_start_time": None},
            {"full_visitor_id": "b", "visit_id": 1, "visit_start_time": None},
        ]
        unique, dropped = transform.dedupe_sessions(rows)
        assert dropped == 0
        assert len(unique) == 2


class TestSchemaDrift:
    def test_clean_records_no_drift(self, ga_sessions_raw):
        report = transform.detect_schema_drift(ga_sessions_raw)
        assert report["unexpected_keys"] == []

    def test_unexpected_key_reported_not_fatal(self, ga_sessions_raw):
        record = {**ga_sessions_raw[0], "brandNewField": 1}
        report = transform.detect_schema_drift([record])
        assert report["unexpected_keys"] == ["brandNewField"]

    def test_missing_required_key_raises(self):
        with pytest.raises(SchemaDriftError):
            transform.detect_schema_drift([{"fullVisitorId": "1", "visitId": 2}])


class TestDailyVisits:
    def test_transform_live_field_names(self, daily_visits_raw):
        row = transform.transform_daily_visit(daily_visits_raw[0], LOADED_AT)
        assert row == {
            "visit_date": "2016-08-05",
            "visits": 2994,
            "loaded_at": LOADED_AT.isoformat(),
        }

    def test_transform_spec_style_aliases(self):
        row = transform.transform_daily_visit({"date": "2016-08-01", "visits": 1711}, LOADED_AT)
        assert row["visit_date"] == "2016-08-01"
        assert row["visits"] == 1711

    def test_non_numeric_visits_raises(self):
        with pytest.raises(FatalApiError):
            transform.transform_daily_visit({"visit_date": "2016-08-01", "total_visits": "n/a"}, LOADED_AT)
