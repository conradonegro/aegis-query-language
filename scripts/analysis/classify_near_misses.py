"""Root-cause the questions that fail official EX but pass tolerant EX.

These "near misses" look like one homogeneous bucket ("a float formatting
problem") and are not. Comparing the NATIVE driver types on both sides splits
them into causes that need completely different responses:

  * EQUAL as float, types differ  - gold float8 vs our Decimal, same value.
    Pure representation; only fixable by changing what we emit.
  * last-digit float noise        - genuinely different values arising from
    operation order or float accumulation. Not recoverable.
  * text-vs-number                - gold returns a TEXT column verbatim while
    we cast it. Nothing to do with floats at all.

It also re-tests a float-division rewrite per question, so the recoverable
subset can be told from the irreducible one.

Reads the failing set from ``benchmarks/results.db`` for a given run.

Usage::

    export AEGIS_ANALYSIS_DIR=benchmarks/analysis
    echo "<admin key>" > "$AEGIS_ANALYSIS_DIR/admin_key.txt"
    uv run python scripts/analysis/classify_near_misses.py [run_id]
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import httpx
import sqlglot
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlglot import expressions as exp

sys.path.insert(0, "scripts")

from run_bird_benchmark import _build_intent, _submit_question  # noqa: E402

ANALYSIS_DIR = Path(os.environ.get("AEGIS_ANALYSIS_DIR", "benchmarks/analysis"))
DB_URL = os.environ.get(
    "AEGIS_BENCH_DB_URL",
    "postgresql+asyncpg://user_aegis_runtime:runtime_pass"
    "@127.0.0.1:5433/aegis_data_warehouse",
)
API_URL = os.environ.get("AEGIS_API_URL", "http://localhost:8000")
RESULTS_DB = Path(os.environ.get("AEGIS_RESULTS_DB", "benchmarks/results.db"))
QUESTIONS = Path(
    os.environ.get(
        "AEGIS_BENCH_QUESTIONS",
        "data/minidev/MINIDEV/mini_dev_postgresql.json",
    )
)
DEFAULT_RUN = "20260729-154032-7749c1a"

_PYFORMAT = re.compile(r"%\((\w+)\)s")


def _restore_placeholders(sql: str) -> str:
    return _PYFORMAT.sub(r":\1", sql)


def float_divisions(sql: str) -> str | None:
    """Rewrite ``a / b`` -> ``CAST(a AS DOUBLE PRECISION) / b``.

    Returns None when there is no division to rewrite, or on a parse failure.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect="postgres")
    except Exception:
        return None
    changed = False
    for div in tree.find_all(exp.Div):
        numerator = div.this
        rendered = (
            numerator.to.sql(dialect="postgres").upper()
            if isinstance(numerator, exp.Cast)
            else ""
        )
        if "DOUBLE" in rendered:
            continue
        div.set("this", exp.cast(numerator.copy(), "DOUBLE PRECISION"))
        changed = True
    if not changed:
        return None
    return _restore_placeholders(tree.sql(dialect="postgres"))


def _type_name(value: Any) -> str:
    return type(value).__name__


def classify(
    gold: list[tuple[Any, ...]], generated: list[tuple[Any, ...]]
) -> str:
    """Root-cause bucket for a scalar mismatch, based on native driver types."""
    if not gold or not generated:
        return "empty/rowcount"
    if len(gold) != len(generated) or len(gold[0]) != len(generated[0]):
        return "shape (cols/rows differ)"
    gold_value, pred_value = gold[0][0], generated[0][0]
    gold_type, pred_type = _type_name(gold_value), _type_name(pred_value)
    if gold_type == "str" or pred_type == "str":
        return f"text-vs-number (gold {gold_type}, gen {pred_type})"
    try:
        gold_float, pred_float = float(gold_value), float(pred_value)
    except (TypeError, ValueError):
        return f"non-numeric ({gold_type} vs {pred_type})"
    if gold_float == pred_float:
        return f"EQUAL as float, types differ ({gold_type} vs {pred_type})"
    spread = max(abs(gold_float), abs(pred_float), 1e-300)
    relative = abs(gold_float - pred_float) / spread
    if relative < 1e-12:
        return f"last-digit float noise ({gold_type} vs {pred_type})"
    if relative < 1e-6:
        return f"~1e-6 divergence ({gold_type} vs {pred_type})"
    return f"GENUINELY DIFFERENT value ({gold_type} vs {pred_type})"


async def _fetch(
    engine: AsyncEngine, sql: str, params: dict[str, Any]
) -> list[tuple[Any, ...]]:
    async with engine.connect() as conn:
        result = await conn.execute(text(sql), params)
        return [tuple(row) for row in result.fetchall()]


def _normalise_params(params: Any) -> dict[str, Any]:
    if isinstance(params, list):
        return {f"p{i + 1}": v for i, v in enumerate(params)}
    return dict(params or {})


def _near_misses(run_id: str) -> list[tuple[str, str, str]]:
    conn = sqlite3.connect(RESULTS_DB)
    try:
        rows = conn.execute(
            "SELECT question_id, db_id, gold_sql FROM benchmark_results"
            " WHERE run_id = ? AND match = 0 AND match_tolerant = 1",
            (run_id,),
        ).fetchall()
    finally:
        conn.close()
    return [(str(a), str(b), str(c)) for a, b, c in rows]


async def _inspect(
    client: httpx.AsyncClient,
    engine: AsyncEngine,
    entry: dict[str, Any],
    gold_sql: str,
    api_key: str,
) -> dict[str, Any] | None:
    intent = _build_intent(entry["question"], entry.get("evidence"))
    response = await _submit_question(
        client, API_URL, api_key, intent, entry["db_id"], "replay"
    )
    explain = response["body"].get("explainability") or {}
    translation = explain.get("translation") or {}
    sql = translation.get("parameterized_sql")
    if not sql:
        return None
    params = _normalise_params(translation.get("parameters"))

    async with engine.connect() as conn:
        result = await conn.exec_driver_sql(gold_sql)
        gold = [tuple(row) for row in result.fetchall()]
    generated = await _fetch(engine, sql, params)

    record: dict[str, Any] = {
        "qid": entry.get("question_id", "?"),
        "db": entry["db_id"],
        "cause": classify(gold, generated),
        "gold": [[repr(v) for v in row] for row in gold[:2]],
        "gen": [[repr(v) for v in row] for row in generated[:2]],
        "gold_sql": " ".join(gold_sql.split()),
        "gen_sql": " ".join(sql.split()),
    }
    rewritten = float_divisions(sql)
    if rewritten is None:
        record["float_div_match"] = None
    else:
        try:
            rows = await _fetch(engine, rewritten, params)
            record["float_div_match"] = set(gold) == set(rows)
        except Exception as exc:  # noqa: BLE001 - report, don't abort
            record["float_div_match"] = False
            record["float_div_err"] = f"{type(exc).__name__}: {exc}"[:100]
    return record


async def main() -> None:
    run_id = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_RUN
    api_key = (ANALYSIS_DIR / "admin_key.txt").read_text().strip()
    with QUESTIONS.open() as handle:
        by_id = {str(q["question_id"]): q for q in json.load(handle)}

    targets = _near_misses(run_id)
    print(f"run {run_id}: {len(targets)} official-fail / tolerant-pass questions\n")

    engine = create_async_engine(DB_URL)
    results: list[dict[str, Any]] = []
    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            for qid, _db_id, gold_sql in targets:
                record = await _inspect(
                    client, engine, by_id[qid], gold_sql, api_key
                )
                if record is None:
                    print(f"{qid:>5} SKIP (no explain payload)", flush=True)
                    continue
                results.append(record)
                print(
                    f"{record['qid']:>5} {record['db']:<24}"
                    f" float_div={str(record['float_div_match']):<5}"
                    f" {record['cause']}",
                    flush=True,
                )
    finally:
        await engine.dispose()

    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    out = ANALYSIS_DIR / "classify_near_misses.json"
    with out.open("w") as handle:
        json.dump(results, handle, indent=1)

    print("\ncauses:")
    for cause, count in Counter(r["cause"] for r in results).most_common():
        print(f"  {count:>3}  {cause}")
    recovered = sum(1 for r in results if r["float_div_match"])
    no_division = sum(1 for r in results if r["float_div_match"] is None)
    print(
        f"\nfloat-division rewrite recovers {recovered}/{len(results)}"
        f"  ({no_division} had no division to rewrite)"
    )
    print(f"wrote {out}")


if __name__ == "__main__":
    asyncio.run(main())
