"""ETL pipeline: Google Analytics API -> BigQuery.

Layout follows the pipeline stages:

* :mod:`ga_pipeline.extract` — analytics API client
* :mod:`ga_pipeline.transform` — raw records to destination row shapes
* :mod:`ga_pipeline.quality` — pre- and post-load data quality gates
* :mod:`ga_pipeline.load` — BigQuery table specs and the loader
* :mod:`ga_pipeline.llm` — optional features behind the ``[llm]`` extra
* :mod:`ga_pipeline.sql` — reference DDL and reconciliation SQL

with :mod:`~ga_pipeline.pipeline` orchestrating them, :mod:`~ga_pipeline.cli`
as the interface, and :mod:`~ga_pipeline.config` / :mod:`~ga_pipeline.exceptions`
cutting across every layer.
"""

__version__ = "1.0.0"
