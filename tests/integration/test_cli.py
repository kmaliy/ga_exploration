from datetime import date

from typer.testing import CliRunner

from ga_pipeline.cli import app
from ga_pipeline.exceptions import DataQualityError
from ga_pipeline.pipeline import RunResult

runner = CliRunner()


class TestRun:
    def test_run_parses_dates_and_calls_pipeline(self, monkeypatch):
        captured = {}

        def fake_run_range(**kwargs):
            captured.update(kwargs)
            return RunResult(dry_run=True)

        monkeypatch.setattr("ga_pipeline.pipeline.run_range", fake_run_range)
        result = runner.invoke(
            app,
            ["run", "--start-date", "2016-08-01", "--end-date", "2016-08-03", "--dry-run"],
        )
        assert result.exit_code == 0, result.output
        assert captured["start_date"] == date(2016, 8, 1)
        assert captured["end_date"] == date(2016, 8, 3)
        assert captured["endpoint"] == "all"
        assert captured["dry_run"] is True

    def test_bad_date_format_rejected(self):
        result = runner.invoke(app, ["run", "--start-date", "20160801", "--end-date", "2016-08-03"])
        assert result.exit_code != 0

    def test_end_before_start_rejected(self):
        result = runner.invoke(app, ["run", "--start-date", "2016-08-03", "--end-date", "2016-08-01"])
        assert result.exit_code != 0
        assert "must not be before" in result.output

    def test_pipeline_error_maps_to_exit_code_1(self, monkeypatch):
        def failing_run_range(**kwargs):
            raise DataQualityError("empty batch")

        monkeypatch.setattr("ga_pipeline.pipeline.run_range", failing_run_range)
        result = runner.invoke(app, ["run", "--start-date", "2016-08-01", "--end-date", "2016-08-01"])
        assert result.exit_code == 1

    def test_invalid_endpoint_rejected(self):
        result = runner.invoke(
            app,
            [
                "run",
                "--start-date",
                "2016-08-01",
                "--end-date",
                "2016-08-01",
                "--endpoint",
                "nope",
            ],
        )
        assert result.exit_code != 0


class TestSummarize:
    def test_summarize_prints_result(self, monkeypatch):
        monkeypatch.setattr(
            "ga_pipeline.llm.summary.summarize_range",
            lambda start, end: f"summary {start}..{end}",
        )
        result = runner.invoke(app, ["summarize", "--start-date", "2016-08-01", "--end-date", "2016-08-31"])
        assert result.exit_code == 0
        assert "summary 2016-08-01..2016-08-31" in result.output
