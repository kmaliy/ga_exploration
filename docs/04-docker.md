# Docker (Step 4)

```bash
docker build -t ga-pipeline:1.0.0 .
docker run --rm \
  --env-file .env \
  -v "$PWD/service-account.json:/secrets/sa.json:ro" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/sa.json \
  ga-pipeline:1.0.0 \
  run --start-date 2016-08-01 --end-date 2016-08-07
```

Properties of the image:

* Locked dependencies — `uv sync --locked` from `uv.lock`, so the image
  resolves to the same versions as local development.
* Non-root user.
* No dev tooling in the image.
* No secrets in the image or its layers; credentials enter at `docker run` time
  only, via `--env-file` and a mounted read-only service-account file.

To keep dry-run output, mount a host directory over `/app/artifacts/samples`.
