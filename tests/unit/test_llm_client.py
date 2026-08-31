from ga_pipeline.llm import client as llm_client


class TestIsConfigured:
    def test_true_with_key(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
        assert llm_client.is_configured()

    def test_false_without_key(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        assert not llm_client.is_configured()


class TestWorkspaceHeaders:
    """Identity-linked keys that are not workspace-scoped need this header,
    or the API rejects the request with a 400.
    """

    def test_none_when_unset(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_WORKSPACE_ID", raising=False)
        assert llm_client.workspace_headers() is None

    def test_none_when_empty(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_WORKSPACE_ID", "")
        assert llm_client.workspace_headers() is None

    def test_header_set_when_present(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_WORKSPACE_ID", "wrkspc_01ABC")
        assert llm_client.workspace_headers() == {"anthropic-workspace-id": "wrkspc_01ABC"}


class TestTryCompleteNeverRaises:
    def test_returns_none_without_key(self, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        assert llm_client.try_complete("hello") is None

    def test_returns_none_when_the_sdk_raises(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
        import anthropic

        def boom(**kwargs):
            raise RuntimeError("400 from the API")

        monkeypatch.setattr(anthropic, "Anthropic", boom)
        assert llm_client.try_complete("hello") is None

    def test_passes_the_workspace_header_to_the_sdk(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
        monkeypatch.setenv("ANTHROPIC_WORKSPACE_ID", "wrkspc_01ABC")
        seen = {}
        import anthropic

        class FakeClient:
            def __init__(self, **kwargs):
                seen.update(kwargs)
                self.messages = self

            def create(self, **kwargs):
                raise RuntimeError("stop here; construction is what we assert on")

        monkeypatch.setattr(anthropic, "Anthropic", FakeClient)
        llm_client.try_complete("hello")
        assert seen["default_headers"] == {"anthropic-workspace-id": "wrkspc_01ABC"}


class TestTracing:
    """Tracing must never be able to break an LLM call."""

    def setup_method(self):
        llm_client.enable_tracing.cache_clear()

    def teardown_method(self):
        llm_client.enable_tracing.cache_clear()

    def test_noop_without_langfuse_key(self, monkeypatch):
        monkeypatch.delenv("LANGFUSE_PUBLIC_KEY", raising=False)
        assert llm_client.enable_tracing() is False

    def test_survives_a_missing_extra(self, monkeypatch):
        monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-lf-test")
        monkeypatch.setitem(__import__("sys").modules, "opentelemetry.instrumentation.anthropic", None)
        assert llm_client.enable_tracing() is False  # logged, not raised

    def test_survives_an_instrumentor_that_explodes(self, monkeypatch):
        monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-lf-test")
        import sys
        import types

        module = types.ModuleType("opentelemetry.instrumentation.anthropic")

        class Boom:
            def instrument(self):
                raise RuntimeError("collector unreachable")

        module.AnthropicInstrumentor = Boom
        monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation.anthropic", module)
        assert llm_client.enable_tracing() is False

    def test_only_attempted_once(self, monkeypatch):
        monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-lf-test")
        import sys
        import types

        calls = []
        module = types.ModuleType("opentelemetry.instrumentation.anthropic")

        class Recorder:
            def instrument(self):
                calls.append(1)

        module.AnthropicInstrumentor = Recorder
        monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation.anthropic", module)
        assert llm_client.enable_tracing() is True
        assert llm_client.enable_tracing() is True  # cached, not re-instrumented
        assert len(calls) == 1

    def test_broken_tracing_does_not_stop_the_llm_call(self, monkeypatch):
        """The whole point: a dead collector must not cost you the answer."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
        monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-lf-test")
        import sys
        import types

        import anthropic

        module = types.ModuleType("opentelemetry.instrumentation.anthropic")

        class Boom:
            def instrument(self):
                raise RuntimeError("collector unreachable")

        module.AnthropicInstrumentor = Boom
        monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation.anthropic", module)

        class FakeClient:
            def __init__(self, **kwargs):
                self.messages = self

            def create(self, **kwargs):
                block = types.SimpleNamespace(type="text", text="the answer")
                return types.SimpleNamespace(content=[block])

        monkeypatch.setattr(anthropic, "Anthropic", FakeClient)
        assert llm_client.try_complete("hello") == "the answer"


class TestTraced:
    """`traced` must be transparent: inert when off, harmless when broken."""

    def setup_method(self):
        llm_client.enable_tracing.cache_clear()

    def teardown_method(self):
        llm_client.enable_tracing.cache_clear()

    def test_inert_without_tracing(self, monkeypatch):
        monkeypatch.delenv("LANGFUSE_PUBLIC_KEY", raising=False)
        with llm_client.traced("summarize-traffic", tags=["summarize"], inputs={"a": 1}) as trace:
            trace.output("anything")  # must not raise
        assert True

    def test_body_still_runs_when_langfuse_is_broken(self, monkeypatch):
        monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-lf-test")
        import sys
        import types

        module = types.ModuleType("opentelemetry.instrumentation.anthropic")

        class Recorder:
            def instrument(self):
                pass

        module.AnthropicInstrumentor = Recorder
        monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation.anthropic", module)

        broken = types.ModuleType("langfuse")

        def get_client():
            raise RuntimeError("langfuse is down")

        broken.get_client = get_client
        broken.propagate_attributes = None
        monkeypatch.setitem(sys.modules, "langfuse", broken)

        ran = []
        with llm_client.traced("answer-question") as trace:
            ran.append(True)
            trace.output("result")
        assert ran == [True]

    def test_exceptions_from_the_body_propagate(self, monkeypatch):
        """A guardrail refusal must still reach the caller, traced or not."""
        monkeypatch.delenv("LANGFUSE_PUBLIC_KEY", raising=False)
        import pytest

        with pytest.raises(ValueError), llm_client.traced("answer-question"):
            raise ValueError("guardrail refused")
