"""Exception taxonomy for the pipeline.

Transient vs fatal matters operationally: Airflow (and the CLI) may retry a
``TransientApiError`` or ``TransientLoadError``, while fatal errors should fail
fast without burning retries.
"""


class PipelineError(Exception):
    """Base class for all pipeline errors."""


class ConfigError(PipelineError):
    """Required configuration is missing or invalid."""


class TransientApiError(PipelineError):
    """Retryable API failure (timeouts, 429, 5xx after client-side retries)."""


class FatalApiError(PipelineError):
    """Non-retryable API failure (auth failure, bad request, contract change)."""


class SchemaDriftError(PipelineError):
    """The API payload no longer matches the expected schema."""


class DataQualityError(PipelineError):
    """A data-quality check failed; the load must not be trusted."""


class TransientLoadError(PipelineError):
    """Retryable BigQuery failure (rate limits, transient backend errors)."""


class LoadError(PipelineError):
    """Non-retryable BigQuery load failure."""


class SqlGuardrailError(PipelineError):
    """LLM-generated SQL violated a safety guardrail and was not executed."""
