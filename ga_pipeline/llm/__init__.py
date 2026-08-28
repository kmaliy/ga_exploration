"""Optional LLM features (assessment Step 5).

Everything in this subpackage depends on the ``llm`` extra
(``pip install -e '.[llm]'``) and degrades to a deterministic, rule-based path
when ``ANTHROPIC_API_KEY`` is unset. Nothing in the core ETL path imports it at
module scope — the CLI and the DAG import from here lazily — so the pipeline
runs with this directory removed.

* :mod:`~ga_pipeline.llm.client` — thin, time-boxed Anthropic wrapper
* :mod:`~ga_pipeline.llm.summary` — 5a, natural-language traffic summary
* :mod:`~ga_pipeline.llm.triage` — 5b, failure triage for alerting
* :mod:`~ga_pipeline.llm.nl_sql` — 5c, guardrailed natural language to SQL
"""
