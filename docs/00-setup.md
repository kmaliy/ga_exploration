# Setup

The project is managed with [uv](https://docs.astral.sh/uv/): `pyproject.toml`
declares dependencies and `uv.lock` pins the exact resolved versions (committed
for reproducibility).

There is deliberately no `requirements.txt` — the lockfile supersedes it as the
pinned-dependency manifest. Plain-pip consumers can generate one on demand:

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

(a child process cannot export variables into your shell, so this step is manual).

## Secrets policy

Everything sensitive — `GA_API_KEY`, the service-account path,
`ANTHROPIC_API_KEY` — comes from environment variables. Nothing secret is in
git or in the image; `.env` and `*.json` credentials are gitignored.
`setup.sh` never reads, prints, or transmits secrets.

The key that appeared in the assessment PDF should be treated as compromised
and rotated.

## Testing

```bash
pytest                                # everything; DAG tests self-skip without Airflow
pytest -m "not dag"                   # explicitly exclude the Airflow-dependent tests
pytest tests/unit                     # fast inner loop
uv sync --group airflow && pytest     # + DAG contract tests against real Airflow 2.10
ruff check .                          # lint (config in pyproject.toml)
ruff format --check .                 # formatting
```

Coverage spans: flattening (camelCase + snake_case), type coercion, dedupe,
schema drift, pagination/envelope/error taxonomy of the client, every DQ check
(pass and fail paths), DDL/spec parity, dry-run end-to-end, anomaly detection,
failure-triage redaction and fallbacks, the NL→SQL guardrails
(DML/allowlist/partition/cost refusals), and — where Airflow is installed — DAG
contract tests (schedule, retries, timeouts, dependencies, and the redacting
failure callback).

See [`tests/README.md`](../tests/README.md) for how the suite is organised.
