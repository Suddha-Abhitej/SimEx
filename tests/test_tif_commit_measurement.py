"""The measurement cannot create/drop a schema in a trading database."""

import asyncio
import runpy
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

TOOL = runpy.run_path(str(Path(__file__).parents[1] / "tools" / "measure_tif_commit.py"))
NAME = "tif_commit_" + "a" * 32


@pytest.mark.parametrize("database", ["simex", "postgres", "simex_test_other", ""])
def test_measurement_refuses_every_database_except_simex_test(database):
    with pytest.raises(ValueError, match="simex_test"):
        TOOL["schema_dsn"]("postgresql://127.0.0.1/" + database, NAME)


@pytest.mark.parametrize("schema", ["public", "simex_test", "tif_commit_fixed", NAME + "_extra"])
def test_drop_guard_rejects_non_generated_names_before_connecting(schema, monkeypatch):
    async def forbidden(*args, **kwargs):
        raise RuntimeError("DDL guard bypassed")

    monkeypatch.setattr(TOOL["schema_ddl"].__globals__["asyncpg"], "connect", forbidden)
    with pytest.raises(AssertionError):
        asyncio.run(TOOL["schema_ddl"]("postgresql://127.0.0.1/simex_test", schema, drop=True))


def test_schema_options_replace_any_previous_search_path():
    result = TOOL["schema_dsn"](
        "postgresql://127.0.0.1/simex_test?options=-csearch_path=public", NAME
    )
    assert parse_qs(urlsplit(result).query)["options"] == [f"-csearch_path={NAME}"]


def test_distribution_uses_exact_median_and_nearest_rank_p99():
    assert TOOL["distribution"]([1_000_000, 2_000_000, 3_000_000, 4_000_000]) == {
        "count": 4,
        "median_ms": "2.500",
        "sample_p99_ms": "4.000",
    }
