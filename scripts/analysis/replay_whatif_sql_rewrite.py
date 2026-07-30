"""Score a hypothetical SQL rewrite over the full 500-question BIRD mini-dev set.

Answers "what would official EX be if Aegis emitted slightly different SQL?"
*before* anything is implemented, using the replay provider so it costs zero
tokens.

The built-in hypothesis is float division: rewrite every ``a / b`` to
``CAST(a AS DOUBLE PRECISION) / b``, leaving AVG(), ROUND() and non-division
arithmetic untouched. That mirrors the shape of BIRD gold, which uses
``CAST(... AS REAL)`` inside hand-written ratios but stays numeric for AVG and
ROUND.

A ROUND-TRIP CONTROL runs the same sqlglot parse+render with NO cast applied.
Any gain or loss that also shows up in the control is a round-trip artifact
rather than a real effect — without it the measurement is not trustworthy.

Usage::

    export AEGIS_ANALYSIS_DIR=benchmarks/analysis
    echo "<admin key>" > "$AEGIS_ANALYSIS_DIR/admin_key.txt"
    uv run python scripts/analysis/replay_whatif_sql_rewrite.py
"""

from __future__ import annotations

import asyncio
import json
import os
import re
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

from run_bird_benchmark import (  # noqa: E402
    _build_intent,
    _run_gold_sql,
    _submit_question,
    rows_match,
)

ANALYSIS_DIR = Path(os.environ.get("AEGIS_ANALYSIS_DIR", "benchmarks/analysis"))
DB_URL = os.environ.get(
    "AEGIS_BENCH_DB_URL",
    "postgresql+asyncpg://user_aegis_runtime:runtime_pass"
    "@127.0.0.1:5433/aegis_data_warehouse",
)
API_URL = os.environ.get("AEGIS_API_URL", "http://localhost:8000")
QUESTIONS = Path(
    os.environ.get(
        "AEGIS_BENCH_QUESTIONS",
        "data/minidev/MINIDEV/mini_dev_postgresql.json",
    )
)
CONCURRENCY = 8

_PYFORMAT = re.compile(r"%\((\w+)\)s")


def _restore_placeholders(sql: str) -> str:
    """sqlglot renders ``:p1`` as ``%(p1)s``; SQLAlchemy needs it back."""
    return _PYFORMAT.sub(r":\1", sql)


def rewrite(sql: str, apply_cast: bool) -> str | None:
    """Parse and re-render, optionally forcing float division.

    Returns None when the SQL cannot be parsed.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect="postgres")
    except Exception:
        return None
    if apply_cast:
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
    return _restore_placeholders(tree.sql(dialect="postgres"))


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


async def _run_variants(
    engine: AsyncEngine,
    record: dict[str, Any],
    sql: str,
    params: dict[str, Any],
    gold: list[tuple[Any, ...]],
) -> None:
    """Execute the control and the rewritten variant, recording each verdict."""
    for name, cast in (("ctrl", False), ("rewritten", True)):
        candidate = rewrite(sql, cast)
        if candidate is None:
            record[name] = record["official"]
            record[f"{name}_err"] = "unparseable"
            continue
        try:
            rows = await _fetch(engine, candidate, params)
            record[name] = rows_match(gold, rows, mode="official")
        except Exception as exc:  # noqa: BLE001 - report, never abort the sweep
            record[name] = False
            record[f"{name}_err"] = f"{type(exc).__name__}: {exc}"[:70]


async def _evaluate(
    client: httpx.AsyncClient,
    engine: AsyncEngine,
    entry: dict[str, Any],
    api_key: str,
    sem: asyncio.Semaphore,
) -> dict[str, Any]:
    async with sem:
        record: dict[str, Any] = {
            "qid": entry.get("question_id", "?"),
            "db": entry["db_id"],
            "status": "ok",
        }
        try:
            intent = _build_intent(entry["question"], entry.get("evidence"))
            response = await _submit_question(
                client, API_URL, api_key, intent, entry["db_id"], "replay"
            )
            if response["status_code"] != 200:
                record["status"] = "api_error"
                return record
            explain = response["body"].get("explainability") or {}
            translation = explain.get("translation") or {}
            sql = translation.get("parameterized_sql")
            if not sql:
                record["status"] = "no_explain"
                return record
            params = _normalise_params(translation.get("parameters"))
            gold = await _run_gold_sql(engine, entry["SQL"], entry["db_id"], None)
            record["official"] = rows_match(
                gold, await _fetch(engine, sql, params), mode="official"
            )
            await _run_variants(engine, record, sql, params, gold)
        except Exception as exc:  # noqa: BLE001 - one bad question must not stop the run
            record["status"] = f"exc: {type(exc).__name__}: {exc}"[:80]
        return record


def _report(results: list[dict[str, Any]]) -> None:
    total = len(results)
    ok = [r for r in results if r["status"] == "ok"]
    baseline = sum(1 for r in ok if r["official"])
    print(f"\nevaluated {len(ok)}/{total}")
    print(f"official EX : {baseline} ({baseline / total * 100:.1f}%)")
    labels = (
        ("ctrl", "sqlglot round-trip CONTROL"),
        ("rewritten", "float-division rewrite"),
    )
    for key, label in labels:
        score = sum(1 for r in ok if r.get(key))
        gained = sorted(r["qid"] for r in ok if r.get(key) and not r["official"])
        lost = sorted(r["qid"] for r in ok if r["official"] and not r.get(key))
        print(
            f"\n{label}: {score} ({score / total * 100:.1f}%)"
            f"  NET {score - baseline:+d}  = +{len(gained)} / -{len(lost)}"
        )
        print(f"  gains : {gained}")
        print(f"  losses: {lost}")
        errors = [r for r in ok if r.get(f"{key}_err")]
        if errors:
            kinds = Counter(r[f"{key}_err"].split(":")[0] for r in errors)
            print(f"  rewrite errors ({len(errors)}): {kinds.most_common(5)}")


async def main() -> None:
    api_key = (ANALYSIS_DIR / "admin_key.txt").read_text().strip()
    with QUESTIONS.open() as handle:
        entries: list[dict[str, Any]] = json.load(handle)
    engine = create_async_engine(DB_URL, pool_size=CONCURRENCY + 2, max_overflow=4)
    sem = asyncio.Semaphore(CONCURRENCY)
    results: list[dict[str, Any]] = []
    try:
        async with httpx.AsyncClient(timeout=300.0) as client:
            tasks = [
                _evaluate(client, engine, entry, api_key, sem) for entry in entries
            ]
            for done, task in enumerate(asyncio.as_completed(tasks), start=1):
                results.append(await task)
                if done % 50 == 0:
                    print(f"  ...{done}/{len(entries)}", flush=True)
    finally:
        await engine.dispose()

    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    out = ANALYSIS_DIR / "replay_whatif_sql_rewrite.json"
    with out.open("w") as handle:
        json.dump(results, handle, indent=1)
    _report(results)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    asyncio.run(main())
