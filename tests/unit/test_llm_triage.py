from ga_pipeline.llm import triage as llm_triage

META = {
    "dag": "etl_google_analytics",
    "task": "extract_load_ga_sessions",
    "run": "manual__2016-08-03",
    "try": 3,
}


class TestRedaction:
    def test_redacts_emails_api_keys_and_tokens(self):
        text = (
            "user bob@example.com api_key=AIzaSyC_DGSGabcdefghij "
            "Authorization: Bearer abc.def-123 password=hunter2"
        )
        red = llm_triage.redact(text)
        assert "bob@example.com" not in red
        assert "AIzaSyC_DGSG" not in red
        assert "abc.def-123" not in red
        assert "hunter2" not in red

    def test_plain_error_text_unchanged(self):
        text = "DataQualityError: 3 check(s) failed for 2016-08-03"
        assert llm_triage.redact(text) == text


class TestDeterministicFallback:
    def test_without_api_key_uses_playbook(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        text = llm_triage.triage_failure(META, "DataQualityError: null visit_id on 2016-08-03")
        assert "extract_load_ga_sessions" in text
        assert "DataQualityError" in text
        assert "inspect the failing checks" in text  # playbook hint, not a generic line

    def test_unknown_error_still_produces_message(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        text = llm_triage.triage_failure(META, "ZeroDivisionError: boom")
        assert "extract_load_ga_sessions" in text
        assert "task log" in text


class TestPlaybookLookup:
    def test_transient_load_error_is_not_shadowed_by_load_error(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        text = llm_triage.triage_failure(META, "TransientLoadError: 503 from BigQuery")
        assert "loads are idempotent" in text  # TransientLoadError hint
        assert "check dataset/table permissions" not in text  # plain LoadError hint

    def test_plain_load_error_still_matches_its_own_hint(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        text = llm_triage.triage_failure(META, "LoadError: schema mismatch")
        assert "check dataset/table permissions" in text

    def test_class_name_wins_over_a_mention_in_the_message(self):
        """The leading class name decides, not whichever name appears anywhere.

        A scan over the playbook would match ConfigError here and give the
        wrong first response.
        """
        hint = llm_triage.playbook_hint("TransientLoadError: gave up, see ConfigError notes")
        assert "loads are idempotent" in hint

    def test_unrecognised_class_falls_back(self):
        assert "task log" in llm_triage.playbook_hint("ZeroDivisionError: boom")


class TestLlmPath:
    def test_llm_narrative_appended_and_labeled_advisory(self, monkeypatch):
        monkeypatch.setattr(
            llm_triage.llm_client, "try_complete", lambda prompt, **kw: "Likely upstream API outage."
        )
        text = llm_triage.triage_failure(META, "TransientApiError: 502 after retries")
        assert "Likely upstream API outage." in text
        assert "advisory" in text
        assert "TransientApiError" in text  # deterministic part still present

    def test_prompt_only_ever_sees_redacted_error(self, monkeypatch):
        seen = {}

        def fake(prompt, **kw):
            seen["prompt"] = prompt
            return None

        monkeypatch.setattr(llm_triage.llm_client, "try_complete", fake)
        llm_triage.triage_failure(META, "FatalApiError: 401 for key AIzaSyC_DGSGabcdefghij")
        assert "AIzaSyC_DGSG" not in seen["prompt"]
        assert "[api-key]" in seen["prompt"]

    def test_never_raises_even_if_llm_call_explodes(self, monkeypatch):
        def boom(prompt, **kw):
            raise RuntimeError("llm exploded")

        monkeypatch.setattr(llm_triage.llm_client, "try_complete", boom)
        text = llm_triage.triage_failure(META, "LoadError: schema mismatch")
        assert "LoadError" in text  # falls back to the deterministic message
