import pytest

from ga_pipeline.exceptions import ConfigError, SqlGuardrailError
from ga_pipeline.llm import nl_sql

ALLOWED = {"p.d.daily_visits", "p.d.ga_sessions_flat"}
PARTITIONS = {"visit_date", "session_date"}

GOOD_SQL = (
    "SELECT device_category, COUNT(*) AS sessions FROM `p.d.ga_sessions_flat` "
    "WHERE session_date BETWEEN '2016-08-01' AND '2016-08-07' GROUP BY device_category"
)


class FakeLoader:
    def __init__(self, rows=None, dry_run=1_000_000):
        self.rows = rows or []
        self.dry_run = dry_run
        self.executed = None
        self.billed_cap = None

    def table_id(self, name):
        return f"p.d.{name}"

    def dry_run_bytes(self, sql):
        return self.dry_run

    def query_rows(self, sql, params=None, maximum_bytes_billed=None):
        self.executed = sql
        self.billed_cap = maximum_bytes_billed
        return self.rows


class TestValidateSql:
    def test_accepts_partition_filtered_select(self):
        out = nl_sql.validate_sql(GOOD_SQL, ALLOWED, PARTITIONS)
        assert out.startswith("SELECT")

    def test_appends_limit_when_missing(self):
        out = nl_sql.validate_sql(GOOD_SQL, ALLOWED, PARTITIONS)
        assert f"LIMIT {nl_sql.MAX_RESULT_ROWS}" in out

    def test_keeps_existing_limit(self):
        out = nl_sql.validate_sql(GOOD_SQL + " LIMIT 10", ALLOWED, PARTITIONS)
        assert out.upper().count("LIMIT") == 1

    def test_trailing_semicolon_tolerated(self):
        out = nl_sql.validate_sql(GOOD_SQL + ";", ALLOWED, PARTITIONS)
        assert ";" not in out

    @pytest.mark.parametrize(
        ("sql", "reason"),
        [
            ("DELETE FROM `p.d.daily_visits` WHERE visit_date = '2016-08-01'", "dml"),
            ("DROP TABLE `p.d.daily_visits`", "ddl"),
            (
                "SELECT 1; DELETE FROM `p.d.daily_visits` WHERE visit_date = '2016-08-01'",
                "multiple statements",
            ),
            (
                "SELECT visits FROM `p.d.other_table` WHERE visit_date = '2016-08-01'",
                "table not on allowlist",
            ),
            (
                "SELECT table_name FROM `p.d.daily_visits`, p.d.INFORMATION_SCHEMA.TABLES "
                "WHERE visit_date = '2016-08-01'",
                "information schema",
            ),
            ("SELECT visits FROM `p.d.daily_visits`", "no partition filter"),
            ("SELECT visits FROM daily_visits WHERE visit_date = '2016-08-01'", "unbackticked table"),
        ],
    )
    def test_rejects(self, sql, reason):
        with pytest.raises(SqlGuardrailError):
            nl_sql.validate_sql(sql, ALLOWED, PARTITIONS)


class TestAsk:
    def _with_llm(self, monkeypatch, sql_text):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr(nl_sql.llm_client, "try_complete", lambda prompt, **kw: sql_text)

    def test_requires_api_key(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        with pytest.raises(ConfigError):
            nl_sql.ask("how many visits in august", loader=FakeLoader())

    def test_happy_path_strips_fences_validates_and_caps_billing(self, monkeypatch):
        self._with_llm(monkeypatch, "```sql\n" + GOOD_SQL + "\n```")
        loader = FakeLoader(rows=[{"device_category": "desktop", "sessions": 10}])
        result = nl_sql.ask("sessions by device, first week of Aug 2016", loader=loader)
        assert result.rows[0]["sessions"] == 10
        assert loader.executed.startswith("SELECT")
        assert "```" not in loader.executed
        assert loader.billed_cap == nl_sql.DEFAULT_MAX_SCANNED_BYTES  # server-side cap set
        assert result.estimated_bytes == 1_000_000

    def test_refuses_over_scan_cap_before_running(self, monkeypatch):
        self._with_llm(monkeypatch, GOOD_SQL)
        loader = FakeLoader(dry_run=500 * 1024 * 1024)
        with pytest.raises(SqlGuardrailError):
            nl_sql.ask("everything", loader=loader, max_scanned_bytes=100 * 1024 * 1024)
        assert loader.executed is None  # refused at dry-run stage, nothing ran

    def test_generated_dml_never_executes(self, monkeypatch):
        self._with_llm(monkeypatch, "DELETE FROM `p.d.daily_visits` WHERE visit_date = '2016-08-01'")
        loader = FakeLoader()
        with pytest.raises(SqlGuardrailError):
            nl_sql.ask("delete everything", loader=loader)
        assert loader.executed is None
