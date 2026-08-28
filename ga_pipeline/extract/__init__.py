"""Extract layer: talking to the analytics API.

Re-exported here so callers can depend on the layer rather than the module:
``from ga_pipeline.extract import AssessmentApiClient``.
"""

from ga_pipeline.extract.api_client import AssessmentApiClient

__all__ = ["AssessmentApiClient"]
