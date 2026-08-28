"""Environment-driven configuration.

Secrets are read from environment variables only, never hardcoded and never
committed. Missing variables are reported all at once.
"""

import os
from dataclasses import dataclass
from datetime import date
from typing import Self

from ga_pipeline.exceptions import ConfigError

DEFAULT_BASE_URL = "https://dish-second-course-gateway-2tximoqc.nw.gateway.dev"

# Available date ranges per endpoint. Dates outside these ranges return
# HTTP 500 from the API, so windows are validated before any request is made.
DAILY_VISITS_RANGE = (date(2016, 8, 1), date(2017, 8, 2))
GA_SESSIONS_RANGE = (date(2016, 8, 1), date(2017, 8, 1))


@dataclass(frozen=True)
class ApiSettings:
    """Settings for the analytics API."""

    base_url: str
    api_key: str
    timeout_seconds: float = 30.0
    page_size: int = 500
    max_retries: int = 5
    backoff_factor: float = 1.0

    @classmethod
    def from_env(cls) -> Self:
        """Build settings from environment variables; fail fast if any are missing."""
        missing = _missing(["GA_API_KEY"])
        if missing:
            raise ConfigError(_missing_message(missing))
        return cls(
            base_url=os.environ.get("GA_API_BASE_URL", DEFAULT_BASE_URL).rstrip("/"),
            api_key=os.environ["GA_API_KEY"],
        )


@dataclass(frozen=True)
class BigQuerySettings:
    """Settings for the BigQuery destination."""

    project: str
    dataset: str
    location: str

    @classmethod
    def from_env(cls) -> Self:
        """Build settings from environment variables; fail fast if any are missing."""
        missing = _missing(["BQ_PROJECT"])
        if missing:
            raise ConfigError(_missing_message(missing))
        return cls(
            project=os.environ["BQ_PROJECT"],
            dataset=os.environ.get("BQ_DATASET", "ga_analytics"),
            location=os.environ.get("BQ_LOCATION", "EU"),
        )


def _missing(names: list[str]) -> list[str]:
    return [name for name in names if not os.environ.get(name)]


def _missing_message(missing: list[str]) -> str:
    joined = ", ".join(missing)
    return f"Missing required environment variable(s): {joined}. See .env.example for the full list."
