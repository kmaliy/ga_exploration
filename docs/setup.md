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

`GA_API_KEY` and `ANTHROPIC_API_KEY` come from the environment. BigQuery uses
Application Default Credentials, so there is no key file to manage:

```bash
gcloud auth application-default login
```

`.env` and credential JSON are gitignored, and nothing secret goes into the
image. `setup.sh` never reads or prints their values.

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
```

A dry run needs the API key and nothing else:

```bash
mkdir -p out
docker run --rm -e GA_API_KEY="$GA_API_KEY" \
  -v "$PWD/out:/app/artifacts/samples" \
  ga-pipeline:1.0.0 run --start-date 2016-08-01 --end-date 2016-08-01 --dry-run
```

A real load needs BigQuery credentials. Mount your ADC file read-only rather
than baking a key into the image:

```bash
set -a; source .env; set +a
ADC="$HOME/.config/gcloud/application_default_credentials.json"
test -f "$ADC" || gcloud auth application-default login

docker run --rm \
  --env-file .env \
  -v "$ADC:/gcloud/adc.json:ro" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/gcloud/adc.json \
  -e GOOGLE_CLOUD_PROJECT="$BQ_PROJECT" \
  ga-pipeline:1.0.0 \
  run --start-date 2016-08-01 --end-date 2016-08-07
```

Check the ADC file exists before mounting. Docker silently creates a *directory*
at a missing bind-mount source, and the client then fails with
`IsADirectoryError`.

The image installs from `uv.lock` with `--locked`, runs as a non-root user, and
carries no dev tooling. Credentials arrive at `docker run` time, never in a
layer. Mount a host directory over `/app/artifacts/samples` to keep dry-run
output.
