"""Shared Anthropic client wrapper for the optional LLM features.

One place owns how the pipeline talks to the LLM: the key, model and optional
workspace come from environment variables, calls are time-boxed with bounded
retries,
matching the retry posture of the HTTP client and BigQuery loader.
``try_complete`` never raises; callers decide what a missing or failing LLM
means — "fall back to a deterministic answer" (summary, triage) or "refuse"
(NL->SQL, where there is nothing safe to fall back to).

Tracing is optional. With ``LANGFUSE_PUBLIC_KEY`` set and the ``tracing`` extra
installed, every call is exported to Langfuse (prompt, response, latency,
tokens), wrapped in a named trace per feature via :func:`traced`. Unset, or
with the extra missing, or with the collector unreachable, nothing changes
here: failures are logged and swallowed. Observability must not be able to
break the pipeline.
"""

import logging
import os
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from functools import cache
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "claude-sonnet-4-5"
TIMEOUT_SECONDS = 30.0
MAX_RETRIES = 2


def workspace_headers() -> dict[str, str] | None:
    """Header required by identity-linked keys not scoped to a workspace.

    A personal or service-account key that is not tied to one workspace must
    say which workspace each request acts in, otherwise the API returns 400.
    Workspace-scoped keys carry it themselves and need nothing here, so this
    returns ``None`` unless ``ANTHROPIC_WORKSPACE_ID`` is set.
    """
    workspace = os.environ.get("ANTHROPIC_WORKSPACE_ID")
    return {"anthropic-workspace-id": workspace} if workspace else None


@cache
def enable_tracing() -> bool:
    """Instrument the Anthropic SDK for Langfuse. Safe to call repeatedly.

    Returns True when tracing is active. Cached, so instrumentation is
    attempted once per process; ``enable_tracing.cache_clear()`` re-attempts.
    A no-op unless ``LANGFUSE_PUBLIC_KEY`` is set, and never raises: a missing
    extra or an unreachable collector must not stop an LLM call being made.
    """
    if not os.environ.get("LANGFUSE_PUBLIC_KEY"):
        return False
    try:
        from opentelemetry.instrumentation.anthropic import AnthropicInstrumentor

        AnthropicInstrumentor().instrument()
    except Exception as exc:  # tracing is best-effort, like the LLM itself
        logger.warning("LLM tracing unavailable, continuing untraced: %s", exc)
        return False
    logger.info("LLM tracing enabled (Langfuse at %s)", os.environ.get("LANGFUSE_BASE_URL", "cloud"))
    return True


class _Trace:
    """Handle yielded by :func:`traced`. Inert when tracing is off."""

    def __init__(self) -> None:
        self._span: Any = None
        self._client: Any = None

    def bind(self, client: Any, span: Any) -> None:
        self._client, self._span = client, span

    def output(self, value: Any) -> None:
        """Record the trace's result. Silently ignored when tracing is off."""
        if self._span is None:
            return
        try:
            self._span.update(output=value)
            self._client.set_current_trace_io(output=value)
        except Exception as exc:  # never let tracing break a caller
            logger.warning("Could not record trace output: %s", exc)


@contextmanager
def traced(name: str, *, tags: list[str] | None = None, inputs: Any = None) -> Iterator[_Trace]:
    """Run a block inside a named Langfuse trace, flushing on the way out.

    ``name`` should be a stable verb-first label (``summarize-traffic``), never
    an interpolated value, because trace names are what dashboards filter on.
    ``inputs`` is set explicitly rather than inferred from the call, so loaders,
    settings objects and keys never end up in the trace.

    Yields an inert handle and does nothing at all when tracing is disabled,
    the extra is missing, or Langfuse cannot be reached.
    """
    trace = _Trace()
    if not enable_tracing():
        yield trace
        return
    try:
        from langfuse import get_client, propagate_attributes

        client = get_client()
        stack = ExitStack()
        span = stack.enter_context(client.start_as_current_observation(as_type="span", name=name))
        stack.enter_context(propagate_attributes(trace_name=name, tags=tags or None))
        client.set_current_trace_io(input=inputs)
        span.update(input=inputs)
        trace.bind(client, span)
    except Exception as exc:  # tracing is best-effort, like the LLM itself
        logger.warning("LLM tracing unavailable for %r: %s", name, exc)
        yield trace
        return
    try:
        yield trace
    finally:
        # short-lived CLI: without a flush the batch exporter never ships
        for step in (stack.close, client.flush):
            try:
                step()
            except Exception as exc:
                logger.warning("Tracing teardown issue: %s", exc)


def is_configured() -> bool:
    """Return True when an Anthropic API key is present in the environment."""
    return bool(os.environ.get("ANTHROPIC_API_KEY"))


def try_complete(prompt: str, *, max_tokens: int = 500) -> str | None:
    """Send one prompt to the LLM; return its text, or ``None`` on any failure.

    Never raises: LLM availability must not be able to break a pipeline code
    path. A missing key, missing package, timeout or API error all simply
    return ``None``.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.info("ANTHROPIC_API_KEY not set; skipping LLM call")
        return None
    enable_tracing()
    try:
        import anthropic
    except ImportError:
        logger.warning("anthropic package not installed (install the 'llm' extra); skipping LLM call")
        return None
    try:
        client = anthropic.Anthropic(
            api_key=api_key,
            timeout=TIMEOUT_SECONDS,
            max_retries=MAX_RETRIES,
            default_headers=workspace_headers(),
        )
        response = client.messages.create(
            model=os.environ.get("ANTHROPIC_MODEL", DEFAULT_MODEL),
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        texts = [block.text for block in response.content if getattr(block, "type", "") == "text"]
        return "\n".join(texts).strip() or None
    except Exception as exc:  # deliberate: the LLM is best-effort by design
        logger.warning("LLM call failed: %s", exc)
        return None
