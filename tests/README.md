# tests/

| Directory | What lives here | Needs |
|---|---|---|
| `unit/` | Pure functions: transform, quality rules, NL→SQL guardrails, LLM prompt/fallback logic | nothing beyond the dev group |
| `integration/` | Wiring across modules: API client over mocked HTTP, pipeline orchestration, CLI | nothing beyond the dev group |
| `dag/` | Airflow DAG contract (schedule, retries, task graph) | the `airflow` group |

`conftest.py` and `fixtures/` sit at this level so all three directories share them.

1. everything; DAG tests self-skip if Airflow is absent
2. explicitly exclude the Airflow-dependent tests
3. fast inner loop

```bash
pytest
pytest -m "not dag"
pytest tests/unit
```
