import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def ga_sessions_raw() -> list[dict]:
    return json.loads((FIXTURES / "ga_sessions_sample.json").read_text())


@pytest.fixture
def daily_visits_raw() -> list[dict]:
    return json.loads((FIXTURES / "daily_visits_sample.json").read_text())
