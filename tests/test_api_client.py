import pytest
import requests

from ga_pipeline.api_client import AssessmentApiClient
from ga_pipeline.config import ApiSettings
from ga_pipeline.exceptions import FatalApiError, TransientApiError

SETTINGS = ApiSettings(base_url="https://api.test", api_key="test-key", page_size=2)


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, text=""):
        self.status_code = status_code
        self._json = json_data
        self.text = text

    def json(self):
        if self._json is None:
            raise ValueError("no json")
        return self._json


class FakeSession:
    """Scripted session: pops one canned response (or exception) per call."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def make_client(responses):
    session = FakeSession(responses)
    return AssessmentApiClient(SETTINGS, session=session), session


class TestPagination:
    def test_stops_on_short_page(self):
        client, session = make_client(
            [
                FakeResponse(json_data=[{"a": 1}, {"a": 2}]),  # full page (size 2)
                FakeResponse(json_data=[{"a": 3}]),  # short page -> stop
            ]
        )
        records = list(client.iter_daily_visits("2016-08-01", "2016-08-05"))
        assert [r["a"] for r in records] == [1, 2, 3]
        assert [c["params"]["page"] for c in session.calls] == [1, 2]

    def test_stops_on_empty_page(self):
        client, _ = make_client(
            [
                FakeResponse(json_data=[{"a": 1}, {"a": 2}]),
                FakeResponse(json_data=[]),
            ]
        )
        assert len(list(client.iter_ga_sessions("20160801"))) == 2

    def test_has_next_false_stops_despite_full_page(self):
        # Live finding: past-the-end pages return HTTP 500, so has_next is
        # authoritative — a full last page must NOT trigger one more request.
        client, session = make_client(
            [
                FakeResponse(
                    json_data={
                        "records": [{"a": 1}, {"a": 2}],  # full page (size 2)
                        "pagination": {"has_next": False},
                    }
                )
            ]
        )
        records = list(client.iter_daily_visits("2016-08-01", "2016-08-05"))
        assert [r["a"] for r in records] == [1, 2]
        assert len(session.calls) == 1

    def test_has_next_true_continues_despite_short_page(self):
        client, session = make_client(
            [
                FakeResponse(json_data={"records": [{"a": 1}], "pagination": {"has_next": True}}),
                FakeResponse(json_data={"records": [{"a": 2}], "pagination": {"has_next": False}}),
            ]
        )
        records = list(client.iter_ga_sessions("20160801"))
        assert [r["a"] for r in records] == [1, 2]
        assert len(session.calls) == 2

    def test_empty_page_with_has_next_true_stops(self):
        # Defensive: a lying API must not cause an infinite loop.
        client, session = make_client(
            [FakeResponse(json_data={"records": [], "pagination": {"has_next": True}})]
        )
        assert list(client.iter_ga_sessions("20160801")) == []
        assert len(session.calls) == 1

    def test_sends_auth_header_and_params(self):
        client, session = make_client([FakeResponse(json_data=[])])
        list(client.iter_ga_sessions("20160801"))
        call = session.calls[0]
        assert call["headers"]["X-API-Key"] == "test-key"
        assert call["params"]["date"] == "20160801"
        assert call["url"].endswith("/ga-sessions-data")


class TestEnvelopes:
    @pytest.mark.parametrize("key", ["data", "items", "results", "records", "rows"])
    def test_dict_envelope(self, key):
        client, _ = make_client([FakeResponse(json_data={key: [{"a": 1}]})])
        assert list(client.iter_daily_visits("2016-08-01", "2016-08-01")) == [{"a": 1}]

    def test_unknown_envelope_raises(self):
        client, _ = make_client([FakeResponse(json_data={"weird": 1})])
        with pytest.raises(FatalApiError, match="envelope"):
            list(client.iter_daily_visits("2016-08-01", "2016-08-01"))


class TestErrorTaxonomy:
    def test_connection_error_is_transient(self):
        client, _ = make_client([requests.ConnectionError("boom")])
        with pytest.raises(TransientApiError):
            list(client.iter_daily_visits("2016-08-01", "2016-08-01"))

    def test_timeout_is_transient(self):
        client, _ = make_client([requests.Timeout("slow")])
        with pytest.raises(TransientApiError):
            list(client.iter_ga_sessions("20160801"))

    def test_5xx_after_adapter_retries_is_transient(self):
        client, _ = make_client([FakeResponse(status_code=503)])
        with pytest.raises(TransientApiError):
            list(client.iter_ga_sessions("20160801"))

    @pytest.mark.parametrize("status", [401, 403])
    def test_auth_errors_are_fatal(self, status):
        client, _ = make_client([FakeResponse(status_code=status)])
        with pytest.raises(FatalApiError, match="GA_API_KEY"):
            list(client.iter_ga_sessions("20160801"))

    def test_400_is_fatal(self):
        client, _ = make_client([FakeResponse(status_code=400, text="bad date")])
        with pytest.raises(FatalApiError, match="bad date"):
            list(client.iter_ga_sessions("2016-08-01"))

    def test_non_json_body_is_fatal(self):
        client, _ = make_client([FakeResponse(status_code=200, json_data=None)])
        with pytest.raises(FatalApiError, match="non-JSON"):
            list(client.iter_ga_sessions("20160801"))
