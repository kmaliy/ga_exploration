"""HTTP client for the assessment API.

Uses one requests.Session with retry/backoff for 429 and 5xx responses.
Errors that are worth retrying at the task level become TransientApiError,
everything else (auth, bad requests, contract changes) becomes FatalApiError.
The client only fetches raw records; flattening and typing happen in
transform.py.
"""

import logging
from collections.abc import Iterator
from typing import Any

import requests
from requests.adapters import HTTPAdapter, Retry

from ga_pipeline.config import ApiSettings
from ga_pipeline.exceptions import FatalApiError, TransientApiError

logger = logging.getLogger(__name__)

DAILY_VISITS_PATH = "/daily-visits"
GA_SESSIONS_PATH = "/ga-sessions-data"

_RETRY_STATUSES = (429, 500, 502, 503, 504)
# Keys under which APIs commonly nest their record list; probed in order.
_RECORD_CONTAINER_KEYS = ("data", "items", "results", "records", "rows")


class AssessmentApiClient:
    """Thin, retrying client for the two assessment endpoints."""

    def __init__(self, settings: ApiSettings, session: requests.Session | None = None) -> None:
        self._settings = settings
        self._session = session or self._build_session(settings)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def iter_daily_visits(self, start_date: str, end_date: str) -> Iterator[dict[str, Any]]:
        """Yield raw daily-visit records for ``start_date``..``end_date`` (YYYY-MM-DD)."""
        params = {"start_date": start_date, "end_date": end_date}
        yield from self._iter_paginated(DAILY_VISITS_PATH, params)

    def iter_ga_sessions(self, date: str) -> Iterator[dict[str, Any]]:
        """Yield raw GA session records for one ``date`` (YYYYMMDD)."""
        yield from self._iter_paginated(GA_SESSIONS_PATH, {"date": date})

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _iter_paginated(self, path: str, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        """Iterate pages using the API's pagination metadata.

        The API returns HTTP 500 for pages past the end, so the loop stops on
        ``pagination.has_next`` instead of requesting one page too many. If a
        response carries no pagination metadata, a short page ends the loop.
        """
        page = 1
        page_size = self._settings.page_size
        while True:
            payload = self._get(path, {**params, "page": page, "limit": page_size})
            records, has_next = self._parse_page(payload)
            yield from records
            logger.debug("Fetched %s page=%d records=%d has_next=%s", path, page, len(records), has_next)
            if has_next is not None:
                if not has_next or not records:  # empty page: stop even if has_next says otherwise
                    return
            elif len(records) < page_size:
                return
            page += 1

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        url = f"{self._settings.base_url}{path}"
        try:
            response = self._session.get(
                url,
                params=params,
                headers={"X-API-Key": self._settings.api_key},
                timeout=self._settings.timeout_seconds,
            )
        except (requests.ConnectionError, requests.Timeout) as exc:
            raise TransientApiError(f"GET {path} failed after retries: {exc}") from exc
        except requests.RequestException as exc:  # pragma: no cover - defensive
            raise FatalApiError(f"GET {path} failed: {exc}") from exc

        if response.status_code in _RETRY_STATUSES:
            # Retries exhausted inside the adapter but status still transient.
            raise TransientApiError(f"GET {path} -> HTTP {response.status_code} (retries exhausted)")
        if response.status_code in (401, 403):
            raise FatalApiError(f"GET {path} -> HTTP {response.status_code}: check GA_API_KEY")
        if response.status_code >= 400:
            raise FatalApiError(f"GET {path} -> HTTP {response.status_code}: {response.text[:500]}")

        try:
            return response.json()
        except ValueError as exc:
            raise FatalApiError(f"GET {path} returned non-JSON body") from exc

    @classmethod
    def _parse_page(cls, payload: Any) -> tuple[list[dict[str, Any]], bool | None]:
        """Return ``(records, has_next)``; ``has_next`` is None without metadata."""
        records = cls._extract_records(payload)
        has_next = None
        if isinstance(payload, dict):
            pagination = payload.get("pagination")
            if isinstance(pagination, dict) and isinstance(pagination.get("has_next"), bool):
                has_next = pagination["has_next"]
        return records, has_next

    @staticmethod
    def _extract_records(payload: Any) -> list[dict[str, Any]]:
        """Normalize the envelope: accept a bare list or a {container: [...]} dict."""
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in _RECORD_CONTAINER_KEYS:
                value = payload.get(key)
                if isinstance(value, list):
                    return value
        raise FatalApiError(
            "Unrecognized response envelope; expected a list or a dict containing "
            f"one of {_RECORD_CONTAINER_KEYS}. Got: {type(payload).__name__}"
        )

    @staticmethod
    def _build_session(settings: ApiSettings) -> requests.Session:
        retry = Retry(
            total=settings.max_retries,
            backoff_factor=settings.backoff_factor,
            status_forcelist=_RETRY_STATUSES,
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session
