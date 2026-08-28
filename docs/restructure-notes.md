> **Status: applied 2026-08-28.** This is the review that produced the current
> layout; it describes the flat structure the repository had before, and the
> reasoning behind each change. Kept as a record — the README is the
> authority on how things are laid out now.

# Repository structure review — `ga_exploration`

**Constraint respected:** `data_pipeline.py` stays at the root as the graded entry point, and the
README's Step 1–5 mapping stays obvious. Everything below reshapes the *inside* of the project,
not the surface a reviewer first sees.

---

## Where it stands today

```
ga_exploration/
├── data_pipeline.py            entry-point shim → ga_pipeline.cli:main
├── README.md                   13.8 KB, all five steps in one file
├── pyproject.toml  ruff.toml  requirements.txt (53 KB)  uv.lock
├── Dockerfile  .dockerignore  .env.example  .python-version
├── ga_pipeline/                15 modules, one flat level
├── dags/                       1 DAG
├── docs/                       API_EXPLORATION.md, DAG.md
├── scripts/                    setup.sh, explore_api.sh, profile_api.py
├── sql/                        ddl.sql, reconciliation.sql
├── sample_outputs/             16 MB — evidence + bulk JSONL mixed
└── tests/                      9 flat test files + fixtures/
```

~3,700 lines of Python. The code itself is well-factored — clean exception hierarchy, frozen
config dataclasses, `from __future__ import annotations` everywhere, no circular imports. The
problem is purely **navigational**: nothing in the layout tells you what depends on what.

---

## The eight things worth fixing

### 1. Nothing is committed

`git ls-files` returns exactly two files: `README.md` and `.gitignore`. All 3,700 lines of
pipeline code, every test, the DAG, the Dockerfile — untracked. This outranks every other item
on this list. No structure is professional if it isn't in version control.

### 2. `ga_pipeline/` is the actual flatness

Fifteen sibling modules with no grouping. Reading the directory, you cannot tell that
`api_client → transform → quality → bq_loader` is the ETL spine while `llm_*` are optional
extras. `nl_sql.py` sits next to `schemas.py` and reads like a SQL utility — it's actually an
LLM feature behind the `llm` extra.

### 3. The Step 5 bonus is scattered across four files

`llm_client.py`, `llm_summary.py`, `llm_triage.py`, `nl_sql.py` are one coherent, *optional*
feature set (`pip install .[llm]`) diffused through the core package. A reviewer looking for
"the bonus work" has to hunt; a reader of the core pipeline has to mentally filter it out.

### 4. `sql/` is orphaned from its consumers, and duplicates `schemas.py`

`ddl.sql` declares table shape in SQL; `schemas.py` declares it again in Python `TableSpec`
objects. Two sources of truth that can drift. And because `sql/` sits outside the package, it
won't ship in a wheel or land reliably in the Docker image.

### 5. `sample_outputs/` mixes two different kinds of thing

`api_profile_report.md`, `api_profile_raw.json`, `api_exploration.log` are **Step 1 deliverables** —
evidence you want a reviewer to read. The nine `*.jsonl` files are **bulk generated output** —
16 MB of dry-run dumps that should never enter git. Same folder, opposite lifecycles.

### 6. Three dependency files disagree

`pyproject.toml` (the real declaration), `uv.lock` (the real lock), and a 53 KB
`requirements.txt` that looks like a full `uv pip freeze`. A reviewer can't tell which is
authoritative. Pick two.

### 7. `tests/` doesn't separate what needs what

Nine flat files. `test_dag.py` needs Airflow installed; the rest don't. Nothing signals that,
so a fresh clone runs `pytest` and gets a confusing collection error.

### 8. The README buries its best material

The strongest content — idempotency strategy, partitioning and clustering rationale, the
concrete DQ check list, retry semantics — is design reasoning sitting three levels deep under
"Step 2". That's the part worth reading, and it's the hardest part to find.

---

## Proposed structure

```
ga_exploration/
├── README.md                       trimmed: what it does, quickstart, step map, links out
├── data_pipeline.py                UNCHANGED — graded entry point
├── pyproject.toml                  absorbs ruff.toml
├── uv.lock
├── Dockerfile  .dockerignore
├── .env.example  .python-version  .gitignore
│
├── ga_pipeline/
│   ├── __init__.py
│   ├── cli.py                      interface layer
│   ├── config.py                   settings — read by every layer
│   ├── exceptions.py               error taxonomy — read by every layer
│   ├── pipeline.py                 orchestration: the spine
│   │
│   ├── extract/
│   │   ├── __init__.py             re-exports AssessmentApiClient
│   │   └── api_client.py
│   │
│   ├── transform/
│   │   ├── __init__.py             re-exports the current public surface
│   │   ├── sessions.py             flatten_session, dedupe_sessions, detect_schema_drift
│   │   ├── daily_visits.py         transform_daily_visit
│   │   └── coercion.py             _to_int/_to_bool/_snake/parse_ga_date helpers
│   │
│   ├── load/
│   │   ├── __init__.py
│   │   ├── bq_loader.py
│   │   └── schemas.py              TableSpec, ALL_SPECS — moves next to its only consumer
│   │
│   ├── quality/
│   │   ├── __init__.py
│   │   └── checks.py               from quality.py
│   │
│   ├── llm/                        Step 5 — the whole optional extra, in one place
│   │   ├── __init__.py
│   │   ├── client.py               was llm_client.py
│   │   ├── summary.py              was llm_summary.py
│   │   ├── triage.py               was llm_triage.py
│   │   └── nl_sql.py
│   │
│   └── sql/                        packaged — loaded via importlib.resources
│       ├── ddl.sql
│       └── reconciliation.sql
│
├── dags/
│   └── etl_google_analytics_dag.py
│
├── tests/
│   ├── conftest.py
│   ├── fixtures/
│   ├── unit/                       transform, quality, schemas, nl_sql guardrails
│   ├── integration/                api_client (mocked HTTP), pipeline, cli
│   └── dag/                        test_dag.py — marked, needs the airflow extra
│
├── scripts/
│   ├── setup.sh  explore_api.sh  profile_api.py
│
├── docs/
│   ├── 01-api-exploration.md       was docs/01-api-exploration.md
│   ├── 02-pipeline-design.md       lifted out of README: idempotency, partitioning, DQ, retries
│   ├── 03-airflow.md               was docs/03-airflow.md
│   ├── 04-docker.md
│   └── 05-llm-features.md
│
└── artifacts/                      was sample_outputs/
    ├── reports/                    api_profile_report.md, api_profile_raw.json, .log — committed
    └── samples/                    *.jsonl — gitignored
```

### Why this shape

**Grouped by pipeline stage, not by file type.** `extract / transform / load / quality` is the
vocabulary the README already uses and the vocabulary any data engineer reads the repo with.
The directory listing becomes the architecture diagram.

**The optional feature looks optional.** `ga_pipeline/llm/` maps 1:1 to the `[llm]` extra and to
Step 5. Delete the directory and the core pipeline still runs — which is exactly the property
you want a reviewer to be able to verify at a glance.

**`schemas.py` moves next to `bq_loader.py`** because `load/` is its only real consumer, and
`sql/` moves inside the package so DDL ships with the code instead of hoping the working
directory is right.

**Config and exceptions stay at the top level.** They're cross-cutting — every layer imports
them. Pushing them into a `core/` or `common/` folder would be structure for its own sake.

### The migration is import-safe

Because each new subpackage gets an `__init__.py` that re-exports the old names, every existing
import keeps working with **no changes to callers**:

```python
# ga_pipeline/transform/__init__.py
from ga_pipeline.transform.sessions import (
    flatten_session,
    session_key,
    dedupe_sessions,
    detect_schema_drift,
)
from ga_pipeline.transform.daily_visits import transform_daily_visit
from ga_pipeline.transform.coercion import parse_ga_date, utcnow

__all__ = [...]
```

`from ga_pipeline.transform import flatten_session` — unchanged. Same for `quality`, `extract`,
`load`.

The **only** import paths that genuinely break are the four LLM modules
(`ga_pipeline.llm_client` → `ga_pipeline.llm.client`, and so on). Those are referenced in
`cli.py`, `pipeline.py`, the DAG, and four test files — a bounded, mechanical rename.

---

## Do it in tiers — stop wherever the value runs out

### Tier 1 — highest value, ~30 minutes

1. **`git add -A && git commit`.** Verify `.gitignore` covers `.venv/`, `.idea/`, `*.egg-info/`,
   and add `artifacts/samples/`. Nothing else matters until this is done.
2. **Split `sample_outputs/`** into `artifacts/reports/` (committed evidence) and
   `artifacts/samples/` (gitignored).
3. **Delete `requirements.txt`** — `pyproject.toml` + `uv.lock` are the real story. If you need
   a pinned file for the Docker build, generate it there rather than committing 53 KB.
4. **Fold `ruff.toml` into `pyproject.toml`** as `[tool.ruff]`. One less root file, and tool
   config lives with project config.

### Tier 2 — the structural change, ~1–2 hours

5. **Create `ga_pipeline/llm/`** and move the four modules in. Biggest single readability win:
   it separates required from optional.
6. **Create `extract/`, `load/`, `quality/`** with re-exporting `__init__.py` files. Mechanical,
   and callers don't change.
7. **Move `sql/` into the package**, load it with `importlib.resources`, and add
   `[tool.setuptools.package-data]` so it ships. Add a test asserting `ddl.sql` and the
   `TableSpec` definitions agree, so the duplication stops being a drift risk.
8. **Split `tests/` into `unit/ integration/ dag/`** and register a `pytest.ini_options` marker
   so `pytest -m "not dag"` works without Airflow.

### Tier 3 — polish, ~1 hour

9. **Split `transform.py`** (288 lines, 18 functions) into `sessions.py` / `daily_visits.py` /
   `coercion.py`. Optional — 288 lines is not yet painful, but the three concerns are cleanly
   separable.
10. **Break the README apart** into `docs/01-`…`05-`, leaving the README as a one-screen
    orientation page with a table linking each assessment step to its doc, its module, and its
    test. For a reviewer, that table is the single most useful thing in the repo.

---

## What I'd deliberately not change

- **`data_pipeline.py` at the root.** It's the documented, graded entry point.
- **A `src/` layout.** Correct for a distributed library; it adds a directory level and an
  editable-install subtlety here for no gain, and it puts distance between the reviewer and the
  code.
- **`dags/` at the root.** Airflow expects a DAGs folder it can point at; burying it in the
  package would be wrong.
- **`config.py` / `exceptions.py` / `cli.py` staying flat.** Already in the right place.
