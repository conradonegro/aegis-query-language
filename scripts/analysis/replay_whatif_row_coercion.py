"""Score a hypothetical RESULT-ROW coercion over the full BIRD mini-dev set.

Answers "what would official EX be if Aegis returned numeric columns as float8
instead of exact numeric?" — without touching any code or spending any tokens.

``CAST(x AS double precision)`` in PostgreSQL is exactly ``float(Decimal)`` in
Python (both round-to-nearest-double), so the change can be simulated on the
already-fetched rows with no SQL re-run at all.

The point of the script is the LOSS column. Coercing to float8 looks like a
clear win until you count what it breaks: gold deliberately stays numeric for
plain ``AVG(...)`` and for ``ROUND(CAST(... AS NUMERIC), n)``, and those
questions stop matching. Never quote the gain without the loss.

Usage::

    export AEGIS_ANALYSIS_DIR=benchmarks/analysis
    echo "<admin key>" > "$AEGIS_ANALYSIS_DIR/admin_key.txt"
    uv run python scripts/analysis/replay_whatif_row_coercion.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

sys.path.insert(0, "scripts")

from run_bird_benchmark import (  # noqa: E402
    _build_intent,
    _rerun_generated_sql,
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
CONCURRENCY = 10


def to_float8(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    """Simulate CAST(<numeric cols> AS double precision) on a result set."""
    return [
        tuple(float(v) if isinstance(v, Decimal) else v for v in row) for row in rows
    ]


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
            body = response["body"]
            gold = await _run_gold_sql(engine, entry["SQL"], entry["db_id"], None)
            generated = await _rerun_generated_sql(engine, body)
            if generated is None:
                record["status"] = "no_explain"
                return record
            record["official"] = rows_match(gold, generated, mode="official")
            record["coerced"] = rows_match(
                gold, to_float8(generated), mode="official"
            )
            record["tolerant"] = rows_match(gold, generated, mode="tolerant")
        except Exception as exc:  # noqa: BLE001 - one bad question must not stop the run
            record["status"] = f"exc: {type(exc).__name__}: {exc}"[:80]
        return record


def _report(results: list[dict[str, Any]]) -> None:
    total = len(results)
    ok = [r for r in results if r["status"] == "ok"]
    baseline = sum(1 for r in ok if r["official"])
    coerced = sum(1 for r in ok if r["coerced"])
    tolerant = sum(1 for r in ok if r["tolerant"])
    gained = sorted(r["qid"] for r in ok if r["coerced"] and not r["official"])
    lost = sorted(r["qid"] for r in ok if r["official"] and not r["coerced"])

    print(f"\nquestions        : {total}  (evaluated {len(ok)})")
    print(f"official EX      : {baseline}  ({baseline / total * 100:.1f}%)")
    print(
        f"float8 rows      : {coerced}  ({coerced / total * 100:.1f}%)"
        f"   NET {coerced - baseline:+d}  = +{len(gained)} / -{len(lost)}"
    )
    print(f"tolerant EX      : {tolerant}  ({tolerant / total * 100:.1f}%)")
    print(f"\nGAINS ({len(gained)}): {gained}")
    print(f"LOSSES ({len(lost)}): {lost}")
    bad = Counter(r["status"] for r in results if r["status"] != "ok")
    if bad:
        print(f"\nnon-ok: {bad.most_common()}")


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
    out = ANALYSIS_DIR / "replay_whatif_row_coercion.json"
    with out.open("w") as handle:
        json.dump(results, handle, indent=1)
    _report(results)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    asyncio.run(main())
