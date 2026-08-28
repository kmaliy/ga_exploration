# tests/

| Directory | Contents | Needs |
|---|---|---|
| `unit/` | Pure functions: transform, quality rules, NL→SQL guardrails, LLM fallbacks | dev group only |
| `integration/` | API client over mocked HTTP, pipeline orchestration, CLI | dev group only |
| `dag/` | Airflow DAG contract: schedule, retries, task graph | the `airflow` group |

`conftest.py` and `fixtures/` sit at this level, shared by all three.

```bash
pytest                 # everything; DAG tests skip without Airflow
pytest -m "not dag"    # skip them explicitly
pytest tests/unit      # fast inner loop
```
