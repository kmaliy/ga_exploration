"""Shared Anthropic client wrapper for the Step 5 bonus features.

One place owns how the pipeline talks to the LLM: the key and model come
from environment variables, and calls are time-boxed with bounded retries,
matching the retry posture of the HTTP client and BigQuery loader.
``try_complete`` never raises; callers decide what a missing or failing LLM
means — "fall back to a deterministic answer" (summary, triage) or "refuse"
(NL->SQL, where there is nothing safe to fall back to).
"""

import logging
import os

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "claude-sonnet-4-5"
TIMEOUT_SECONDS = 30.0
MAX_RETRIES = 2


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
    try:
        import anthropic
    except ImportError:
        logger.warning("anthropic package not installed (install the 'llm' extra); skipping LLM call")
        return None
    try:
        client = anthropic.Anthropic(api_key=api_key, timeout=TIMEOUT_SECONDS, max_retries=MAX_RETRIES)
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
