"""Guard against drift between the Python table specs and the reference DDL.

``ga_pipeline/load/schemas.py`` is what the pipeline actually creates and loads;
``ga_pipeline/sql/ddl.sql`` is what a reviewer reads. They describe the same two
tables, so they are two sources of truth for one thing — this test makes the
duplication safe by failing when they disagree.
"""

import re
from importlib.resources import files

import pytest

from ga_pipeline.load.schemas import ALL_SPECS

# BigQuery standard-SQL type name -> the name google-cloud-bigquery reports.
SQL_TO_API_TYPE = {
    "INT64": "INTEGER",
    "BOOL": "BOOLEAN",
    "FLOAT64": "FLOAT",
    "STRING": "STRING",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "NUMERIC": "NUMERIC",
}

TABLE_RE = (
    r"CREATE TABLE IF NOT EXISTS `[^`]*\.{table}`\s*\((?P<cols>.*?)\n\)"
    r"\s*\nPARTITION BY\s+(?P<partition>\w+)"
    r"(?:\s*\nCLUSTER BY\s+(?P<cluster>[^\n]+))?"
)


@pytest.fixture(scope="module")
def ddl() -> str:
    return files("ga_pipeline.sql").joinpath("ddl.sql").read_text(encoding="utf-8")


def parse_table(ddl: str, table: str) -> dict:
    """Extract columns, partitioning and clustering for one table from the DDL."""
    match = re.search(TABLE_RE.format(table=table), ddl, re.S)
    assert match, f"No CREATE TABLE block for {table!r} in ddl.sql"

    columns = []
    for raw in match.group("cols").splitlines():
        line = raw.strip()
        if not line or line.startswith("--"):
            continue
        parts = line.rstrip(",").split()
        columns.append(
            {
                "name": parts[0],
                "type": SQL_TO_API_TYPE.get(parts[1], parts[1]),
                "required": "NOT NULL" in line,
            }
        )

    cluster = match.group("cluster")
    return {
        "columns": columns,
        "partition": match.group("partition"),
        "clustering": [c.strip() for c in cluster.split(",")] if cluster else [],
    }


@pytest.mark.parametrize("spec", ALL_SPECS, ids=lambda s: s.name)
def test_ddl_columns_match_spec(ddl, spec):
    parsed = parse_table(ddl, spec.name)

    assert [c["name"] for c in parsed["columns"]] == [f.name for f in spec.schema], (
        f"{spec.name}: column names/order differ between ddl.sql and schemas.py"
    )
    assert [c["type"] for c in parsed["columns"]] == [f.field_type for f in spec.schema], (
        f"{spec.name}: column types differ between ddl.sql and schemas.py"
    )
    assert [c["required"] for c in parsed["columns"]] == [f.mode == "REQUIRED" for f in spec.schema], (
        f"{spec.name}: NOT NULL / REQUIRED differ between ddl.sql and schemas.py"
    )


@pytest.mark.parametrize("spec", ALL_SPECS, ids=lambda s: s.name)
def test_ddl_partitioning_and_clustering_match_spec(ddl, spec):
    parsed = parse_table(ddl, spec.name)
    assert parsed["partition"] == spec.partition_field
    assert parsed["clustering"] == list(spec.clustering_fields)


def test_reconciliation_sql_is_packaged():
    """The reconciliation query must ship with the package, not just the repo."""
    sql = files("ga_pipeline.sql").joinpath("reconciliation.sql").read_text(encoding="utf-8")
    assert "FULL OUTER JOIN" in sql
