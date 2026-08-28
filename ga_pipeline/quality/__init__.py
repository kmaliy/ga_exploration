"""Data quality gates, pre- and post-load.

Every name the flat ``quality`` module used to expose is re-exported here, so
``from ga_pipeline import quality; quality.check_sessions_pre_load(...)`` keeps
working unchanged.
"""

from ga_pipeline.quality.checks import (
    QualityReport,
    check_daily_visits_pre_load,
    check_reconciliation,
    check_sessions_post_load,
    check_sessions_pre_load,
)

__all__ = [
    "QualityReport",
    "check_daily_visits_pre_load",
    "check_reconciliation",
    "check_sessions_post_load",
    "check_sessions_pre_load",
]
