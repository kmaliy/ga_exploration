from datetime import date

from ga_pipeline import llm_summary


class FakeLoader:
    def __init__(self, results):
        self._results = list(results)

    def table_id(self, name):
        return f"p.d.{name}"

    def query_rows(self, sql, params=None):
        return self._results.pop(0)


AGG_RESULTS = [
    # daily
    [
        {"visit_date": date(2016, 8, 1), "visits": 1000},
        {"visit_date": date(2016, 8, 2), "visits": 1050},
        {"visit_date": date(2016, 8, 3), "visits": 400},  # -62% anomaly
    ],
    # by_device
    [{"device_category": "desktop", "sessions": 900, "bounce_rate_pct": 40.0}],
    # top_countries
    [{"geo_country": "United States", "sessions": 700}],
]


class TestRuleBasedFallback:
    def test_no_api_key_uses_rule_based_summary(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        text = llm_summary.summarize_range(date(2016, 8, 1), date(2016, 8, 3), loader=FakeLoader(AGG_RESULTS))
        assert "2450 total visits" in text
        assert "desktop" in text
        assert "United States" in text
        assert "2016-08-03" in text  # anomaly surfaced
        assert "down 62%" in text

    def test_no_anomalies_reported_when_stable(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        stable = [
            [
                {"visit_date": date(2016, 8, 1), "visits": 1000},
                {"visit_date": date(2016, 8, 2), "visits": 1010},
            ],
            [],
            [],
        ]
        text = llm_summary.summarize_range(date(2016, 8, 1), date(2016, 8, 2), loader=FakeLoader(stable))
        assert "none" in text


class TestAnomalyDetection:
    def test_threshold_boundary(self):
        daily = [
            {"date": "2016-08-01", "visits": 100},
            {"date": "2016-08-02", "visits": 130},  # exactly +30%
            {"date": "2016-08-03", "visits": 155},  # +19% — below threshold
        ]
        anomalies = llm_summary.detect_anomalies(daily)
        assert len(anomalies) == 1
        assert "2016-08-02" in anomalies[0]

    def test_zero_baseline_skipped(self):
        daily = [{"date": "d1", "visits": 0}, {"date": "d2", "visits": 50}]
        assert llm_summary.detect_anomalies(daily) == []
