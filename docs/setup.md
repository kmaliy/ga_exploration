# Setup

Dependencies are managed with [uv](https://docs.astral.sh/uv/). `pyproject.toml`
declares them, `uv.lock` pins exact versions and is committed.

There is no `requirements.txt`. Generate one if a consumer needs it:

```bash
uv export --format requirements-txt --no-dev --extra llm -o requirements.txt
```

## Install

```bash
./scripts/setup.sh              # idempotent, safe to re-run
./scripts/setup.sh --airflow    # also install the Airflow group, for the DAG tests
./scripts/setup.sh --no-verify  # skip the lint and test check
```

It installs the Python version in `.python-version`, syncs from `uv.lock`,
creates `.env` from `.env.example` if missing (never overwriting an existing
one), runs lint and tests, and warns if Google Cloud ADC is absent. Dry runs
need no GCP credentials.

The equivalent by hand:

```bash
uv sync --extra llm && cp .env.example .env
```

Then fill in `.env` and load it. A child process cannot export into your shell,
so this part is manual:

```bash
set -a; source .env; set +a
```

## Secrets

`GA_API_KEY`, the service-account path and `ANTHROPIC_API_KEY` come from the
environment. `.env` and `*.json` credentials are gitignored, and nothing secret
goes into the image. `setup.sh` never reads or prints their values.

## Test

```bash
pytest                                # everything; DAG tests skip without Airflow
pytest -m "not dag"                   # skip them explicitly
pytest tests/unit                     # fast inner loop
uv sync --group airflow && pytest     # include the DAG contract tests
ruff check . && ruff format --check .
```

[`tests/README.md`](../tests/README.md) describes the layout.

## Docker

```bash
docker build -t ga-pipeline:1.0.0 .
docker run --rm \
  --env-file .env \
  -v "$PWD/service-account.json:/secrets/sa.json:ro" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/sa.json \
  ga-pipeline:1.0.0 \
  run --start-date 2016-08-01 --end-date 2016-08-07
```

The image installs from `uv.lock` with `--locked`, runs as a non-root user, and
carries no dev tooling. Credentials arrive at `docker run` time, never in a
layer. Mount a host directory over `/app/artifacts/samples` to keep dry-run
output.
