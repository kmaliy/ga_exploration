#!/usr/bin/env bash
# Project setup script, installs deps, idempotent (safe to rerun).
#
# Usage:
#   ./scripts/setup.sh              # deps (runtime + dev + llm extra) + verification
#   ./scripts/setup.sh --airflow    # also install the Airflow group (DAG tests / dags test)
#   ./scripts/setup.sh --no-verify  # skip ruff + pytest at the end
#
# It DOES NOT:
#   * put any secret anywhere, it only scaffolds .env from .env.example
#   * export env vars into your shell
set -euo pipefail

cd "$(dirname "$0")/.."

WITH_AIRFLOW=false
VERIFY=true
for arg in "$@"; do
  case "$arg" in
    --airflow)   WITH_AIRFLOW=true ;;
    --no-verify) VERIFY=false ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

step() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

step "Checking prerequisites"
if ! command -v uv >/dev/null 2>&1; then
  cat >&2 <<'EOF'
uv is required but not installed. Install it first:

  curl -LsSf https://astral.sh/uv/install.sh | sh     # official installer
  # or: brew install uv

then re-run ./scripts/setup.sh
EOF
  exit 1
fi
echo "uv $(uv --version | awk '{print $2}') found"

step "Ensuring pinned Python is available"
uv python install
echo "python $(cat .python-version) available"

step "Scaffolding .env"
if [ -f .env ]; then
  echo ".env already exists, leaving it untouched"
else
  cp .env.example .env
  echo "created .env from .env.example. Fill in GA_API_KEY and BQ_PROJECT before running the pipeline"
fi

step "Installing dependencies (uv sync)"
SYNC_ARGS=(--extra llm)
if $WITH_AIRFLOW; then
  SYNC_ARGS+=(--group airflow)
  echo "including Airflow group (DAG contract tests, 'airflow dags test')"
fi
uv sync "${SYNC_ARGS[@]}"

if $VERIFY; then
  step "Verifying: lint"
  uv run ruff check .
  uv run ruff format --check .
  step "Verifying: tests"
  uv run pytest
else
  echo "verification skipped (--no-verify)"
fi

step "Checking Google Cloud auth (only needed for real BigQuery loads)"
if command -v gcloud >/dev/null 2>&1; then
  if gcloud auth application-default print-access-token >/dev/null 2>&1; then
    echo "Application Default Credentials found, BigQuery loads will work"
  else
    echo "gcloud installed but no ADC. Run: gcloud auth application-default login"
  fi
else
  echo "gcloud not installed. Dry-runs work without it; install it for real BigQuery loads"
fi

step "Done. Next steps"
cat <<'EOF'
1. Fill in .env (GA_API_KEY, BQ_PROJECT), then load it into your shell:

     set -a; source .env; set +a


2. Smoke-test without any cloud access:

     uv run data_pipeline.py run --start-date 2016-08-01 --end-date 2016-08-01 --dry-run

3. Real BigQuery load (needs BQ_PROJECT + ADC):

     uv run data_pipeline.py run --start-date 2016-08-01 --end-date 2016-08-01

4. Explore the API / regenerate the exploration reports:

     ./scripts/explore_api.sh | tee artifacts/reports/api_exploration.log
     uv run python scripts/profile_api.py --sections profile,limits,stability,ratelimit
EOF
