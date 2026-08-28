# Setup

The project is managed with [uv](https://docs.astral.sh/uv/): `pyproject.toml`
declares dependencies and `uv.lock` pins the exact resolved versions (committed
for reproducibility).

`requirements.txt` only added because it was part of the requirements set by the assessment, it 
plays no role in the project and can be deleted. Can be generated on demand by the following command.

```bash
uv export --format requirements-txt --no-dev --extra llm -o requirements.txt
```

## Bootstrap

```bash
./scripts/setup.sh              # one-shot bootstrap (idempotent, safe to re-run)
./scripts/setup.sh --airflow    # also installs the Airflow group (DAG tests, `airflow dags test`)
./scripts/setup.sh --no-verify  # skip the lint + test verification
```

`setup.sh` executes, in order:

1. checks `uv` is installed (prints the install command and exits if not);
2. installs the pinned Python from `.python-version` if missing;
3. creates `.env` from `.env.example` if absent (an existing `.env` is never touched);
4. `uv sync` from `uv.lock`: exact pinned deps, dev group + `llm` extra;
5. verifies the environment: `ruff check`, `ruff format --check`, `pytest`;
6. checks for Google Cloud ADC (warning only; dry-runs need no GCP);
7. prints next steps.

Manual equivalent of the whole script:

```bash
uv sync --extra llm && cp .env.example .env
```

Afterwards, fill in `.env` and load it into your shell yourself:

```bash
set -a; source .env; set +a
```

## Secrets policy

Everything sensitive — `GA_API_KEY`, the service-account path,
`ANTHROPIC_API_KEY` — comes from environment variables. Nothing secret is in
git or in the image; `.env` and `*.json` credentials are gitignored.
`setup.sh` never reads, prints, or transmits secrets.

## Testing

1. everything; DAG tests self-skip without Airflow
2. explicitly exclude the Airflow-dependent tests
3. fast inner loop
4. fast inner loop and DAG contract tests against real Airflow 2.10 
5. lint (config in pyproject.toml)
6. formatting

```bash
pytest                                # everything; DAG tests self-skip without Airflow
pytest -m "not dag"                   # explicitly exclude the Airflow-dependent tests
pytest tests/unit                     # fast inner loop
uv sync --group airflow && pytest     # + DAG contract tests against real Airflow 2.10
ruff check .                          # lint (config in pyproject.toml)
ruff format --check .                 # formatting
```

See [`tests/README.md`](../tests/README.md) for how the suite is organised.
