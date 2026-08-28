# Step 4 — container image for the ETL module (uv-managed).
#
# Build:
#   docker build -t ga-pipeline:1.0.0 .
#
# Run (secrets via environment only — never baked into the image):
#   docker run --rm \
#     --env-file .env \
#     -v "$PWD/service-account.json:/secrets/sa.json:ro" \
#     -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/sa.json \
#     ga-pipeline:1.0.0 \
#     run --start-date 2016-08-01 --end-date 2016-08-07
#
# Dry-run without any cloud credentials:
#   docker run --rm -e GA_API_KEY=... -v "$PWD/out:/app/artifacts/samples" \
#     ga-pipeline:1.0.0 run --start-date 2016-08-01 --end-date 2016-08-01 --dry-run

FROM python:3.11-slim

# uv binary only — pinned minor, nothing else from that image.
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /uvx /bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

# Dependency layer first (cache-friendly): exact versions from uv.lock,
# runtime + llm extra only — no dev/test tooling in the image.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-install-project --no-dev --extra llm

# Project layer.
COPY README.md data_pipeline.py ./
COPY ga_pipeline/ ga_pipeline/
RUN uv sync --locked --no-dev --extra llm

# Never run as root; writable dir only for dry-run output.
RUN useradd --create-home --uid 10001 etl \
    && mkdir -p /app/artifacts/samples \
    && chown -R etl:etl /app
USER etl

ENV PATH="/app/.venv/bin:$PATH"
ENTRYPOINT ["python", "data_pipeline.py"]
CMD ["--help"]
