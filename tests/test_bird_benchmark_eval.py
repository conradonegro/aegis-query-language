"""Tests for the BIRD benchmark evaluator's result comparison.

Official BIRD EX semantics: set(predicted_rows) == set(gold_rows) — row
order ignored, duplicate rows collapsed, column count/order significant.
The harness additionally normalizes across the JSON serialization boundary:
gold rows carry native driver types (Decimal, date, datetime) while
generated rows arrive JSON-deserialized (str, float, int).
"""

import importlib.util
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import ModuleType

import pytest

_SCRIPT = Path(__file__).parent.parent / "scripts" / "run_bird_benchmark.py"


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("run_bird_benchmark", _SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mod = _load_module()


def test_rows_match_ignores_row_order() -> None:
    assert mod.rows_match([(1,), (2,)], [(2,), (1,)])


def test_rows_match_collapses_duplicate_rows() -> None:
    assert mod.rows_match([(1,), (1,), (2,)], [(2,), (1,)])


def test_rows_match_rejects_different_values() -> None:
    assert not mod.rows_match([(1,)], [(2,)])


def test_rows_match_rejects_extra_column() -> None:
    assert not mod.rows_match([(1,)], [(1, "extra")])


def test_rows_match_decimal_gold_vs_json_string() -> None:
    # pydantic v2 serializes Decimal("12.50") as the string "12.50"
    assert mod.rows_match([(Decimal("12.50"),)], [("12.50",)])


def test_rows_match_date_gold_vs_iso_string() -> None:
    assert mod.rows_match([(date(2013, 1, 1),)], [("2013-01-01",)])


def test_rows_match_datetime_gold_vs_iso_string() -> None:
    assert mod.rows_match(
        [(datetime(2013, 1, 1, 10, 30),)], [("2013-01-01T10:30:00",)]
    )


def test_rows_match_none_values() -> None:
    assert mod.rows_match([(None, 1)], [(None, 1)])


def test_rows_match_empty_result_sets() -> None:
    assert mod.rows_match([], [])
    assert not mod.rows_match([(1,)], [])


@pytest.mark.asyncio
async def test_run_gold_sql_tolerates_colons_in_literals() -> None:
    """Gold SQL contains literals like '1:27' — SQLAlchemy text() would
    parse ':27' as a bind parameter; raw driver execution must be used."""
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    # '%:57' puts a non-word char before the colon — the shape that makes
    # SQLAlchemy text() see a bind parameter named '57' (BIRD formula_1
    # gold queries filter lap times with exactly such LIKE patterns).
    rows = await mod._run_gold_sql(engine, "SELECT '%:57'", "any")
    await engine.dispose()
    assert rows == [("%:57",)]


def test_build_intent_appends_evidence() -> None:
    intent = mod._build_intent("How many customers?", "Year 2012 = 201201-201212")
    assert intent.startswith("How many customers?")
    assert "Year 2012 = 201201-201212" in intent


def test_build_intent_without_evidence_is_bare_question() -> None:
    assert mod._build_intent("How many customers?", "") == "How many customers?"
    assert mod._build_intent("How many customers?", None) == "How many customers?"


@pytest.mark.asyncio
async def test_gold_cache_serves_second_call_without_engine(
    tmp_path: Path,
) -> None:
    """Gold SQL results are deterministic across runs — the cache must
    serve repeat executions without touching the database."""
    from sqlalchemy.ext.asyncio import create_async_engine

    cache = tmp_path / "gold_cache.db"
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    rows1 = await mod._run_gold_sql(engine, "SELECT 41 + 1", "db1", cache)
    await engine.dispose()
    # engine=None: any real execution attempt would crash
    rows2 = await mod._run_gold_sql(None, "SELECT 41 + 1", "db1", cache)
    assert rows1 == rows2 == [(42,)]


# ---------------------------------------------------------------------------
# Numeric type parity
#
# Gold rows carry native driver types; generated rows arrive JSON-serialized.
# PostgreSQL returns float8 for gold's CAST(... AS REAL) but numeric for the
# model's plain `/`, so the same arithmetic answer reaches the comparison as a
# float on one side and a Decimal-turned-string on the other. Those can never
# be equal, and 34 questions in run 20260729-120729 were scored wrong despite
# agreeing to ~16 significant digits.
# ---------------------------------------------------------------------------


def test_rows_match_float_gold_vs_numeric_generated() -> None:
    """gold CAST(x AS REAL) -> float; model's `/` -> numeric -> JSON string."""
    assert mod.rows_match([(100.0,)], [("100.0000000000000000",)])


def test_rows_match_decimal_scales_differ() -> None:
    """Same value, different numeric scale, both via the JSON boundary."""
    assert mod.rows_match([(Decimal("100.0"),)], [("100.0000000000000000",)])


def test_rows_match_float_precision_tail() -> None:
    """float8 and numeric division disagree in the last bit or two."""
    assert mod.rows_match([(459.9562642112431,)], [("459.9562642112432",)])
    assert mod.rows_match([(2.727272727272727,)], [("2.7272727272727273",)])


def test_rows_match_still_rejects_genuinely_different_numbers() -> None:
    assert not mod.rows_match([(9,)], [("136",)])
    assert not mod.rows_match([(7.242696579592377,)], [("51.16206520043674",)])
    assert not mod.rows_match([(1.0,)], [("1.001",)])


def test_rows_match_does_not_coerce_non_numeric_text() -> None:
    assert not mod.rows_match([("CZE",)], [("SVK",)])
    assert mod.rows_match([("CZE",)], [("CZE",)])
