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
