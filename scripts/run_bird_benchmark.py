"""BIRD-SQL benchmark evaluation harness for Aegis Query Language.

Evaluates Execution Match Accuracy (EX) by:
  1. Submitting each BIRD question to /api/v1/query/execute with:
     - A fresh session_id UUID per question (no cross-question context leakage)
     - source_database set to the BIRD db_id
  2. Running the gold SQL directly against the database
  3. Comparing result sets (unordered row match)

Usage:
    uv run python scripts/run_bird_benchmark.py \\
        --questions path/to/dev.json \\
        --api-key <key> \\
        [--api-url http://localhost:8000] \\
        [--db-url postgresql+asyncpg://user:pass@host:5432/db] \\
        [--limit 50] \\
        [--db-filter financial] \\
        [--output results.json]
"""

import argparse
import asyncio
import json
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# ---------------------------------------------------------------------------
# Gold SQL execution
# ---------------------------------------------------------------------------

async def _run_gold_sql(
    engine: Any,
    sql: str,
    db_id: str,
) -> list[tuple[Any, ...]]:
    """
    Execute gold SQL against the physical database.
    Returns a sorted list of row tuples for set comparison.
    """
    async with engine.connect() as conn:
        result = await conn.execute(text(sql))
        rows = [tuple(r) for r in result.fetchall()]
    return sorted(rows, key=lambda r: [str(v) for v in r])


# ---------------------------------------------------------------------------
# API question submission
# ---------------------------------------------------------------------------

async def _submit_question(
    client: httpx.AsyncClient,
    api_url: str,
    api_key: str,
    question: str,
    db_id: str,
    provider_id: str | None,
) -> dict[str, Any]:
    """Submit one question with a fresh session_id and source_database scoping."""
    payload: dict[str, Any] = {
        "intent": question,
        "source_database": db_id,
        "session_id": str(uuid.uuid4()),  # fresh per question — no context leakage
        "explain": False,
        "schema_hints": [],
    }
    if provider_id:
        payload["provider_id"] = provider_id

    resp = await client.post(
        f"{api_url}/api/v1/query/execute",
        json=payload,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=120.0,
    )
    return {"status_code": resp.status_code, "body": resp.json()}


# ---------------------------------------------------------------------------
# Per-question evaluation
# ---------------------------------------------------------------------------

async def _evaluate_question(
    client: httpx.AsyncClient,
    engine: Any,
    api_url: str,
    api_key: str,
    entry: dict[str, Any],
    provider_id: str | None,
) -> dict[str, Any]:
    question_id = entry.get("question_id", "?")
    db_id: str = entry["db_id"]
    question: str = entry["question"]
    gold_sql: str = entry["SQL"]

    result: dict[str, Any] = {
        "question_id": question_id,
        "db_id": db_id,
        "question": question,
        "gold_sql": gold_sql,
        "generated_sql": None,
        "source_database_used": None,
        "status": "error",
        "match": False,
        "error": None,
        "latency_ms": None,
    }

    t0 = time.perf_counter()

    try:
        api_resp = await _submit_question(
            client, api_url, api_key, question, db_id, provider_id
        )
        result["latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)

        if api_resp["status_code"] != 200:
            result["status"] = "api_error"
            result["error"] = api_resp["body"].get("message", str(api_resp["body"]))
            return result

        body = api_resp["body"]
        generated_sql = body.get("sql", "")
        result["generated_sql"] = generated_sql
        result["source_database_used"] = body.get("source_database_used")

        # Compare generated results with gold results
        gold_rows = await _run_gold_sql(engine, gold_sql, db_id)
        gen_rows = [tuple(row.values()) for row in body.get("results", [])]
        gen_rows_sorted = sorted(
            gen_rows, key=lambda r: [str(v) for v in r]
        )

        result["match"] = gold_rows == gen_rows_sorted
        result["status"] = "success"
        result["gold_row_count"] = len(gold_rows)
        result["gen_row_count"] = len(gen_rows_sorted)

    except Exception as exc:
        result["latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        result["status"] = "exception"
        result["error"] = str(exc)

    return result


# ---------------------------------------------------------------------------
# Progress printer
# ---------------------------------------------------------------------------

def _print_progress(i: int, total: int, res: dict[str, Any]) -> None:
    is_error = res["status"] != "success"
    symbol = "✓" if res["match"] else ("E" if is_error else "✗")
    suffix = f"  err={res['error'][:60]}" if res.get("error") else ""
    print(
        f"[{i:>4}/{total}] {symbol}  "
        f"db={res['db_id']:<25} "
        f"q={str(res['question_id']):<6} "
        f"({res['latency_ms']}ms){suffix}"
    )


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------

def _print_summary(results: list[dict[str, Any]], output: str | None) -> None:
    total = len(results)
    matched = sum(1 for r in results if r["match"])
    errored = sum(1 for r in results if r["status"] != "success")
    accuracy = matched / total * 100 if total else 0.0

    print("\n" + "=" * 60)
    print(f"  Total questions : {total}")
    print(f"  Correct (EX)    : {matched}")
    print(f"  Incorrect       : {total - matched - errored}")
    print(f"  Errors          : {errored}")
    print(f"  EX Accuracy     : {accuracy:.1f}%")
    print("=" * 60)

    db_stats: dict[str, dict[str, int]] = {}
    for r in results:
        db = r["db_id"]
        if db not in db_stats:
            db_stats[db] = {"total": 0, "matched": 0}
        db_stats[db]["total"] += 1
        if r["match"]:
            db_stats[db]["matched"] += 1

    if len(db_stats) > 1:
        print("\nPer-database breakdown:")
        for db, s in sorted(db_stats.items()):
            pct = s["matched"] / s["total"] * 100 if s["total"] else 0.0
            print(f"  {db:<30} {s['matched']}/{s['total']}  ({pct:.1f}%)")

    if output:
        output_path = Path(output)
        with output_path.open("w") as f:
            json.dump(
                {
                    "summary": {
                        "total": total,
                        "matched": matched,
                        "errored": errored,
                        "accuracy_pct": round(accuracy, 2),
                    },
                    "results": results,
                },
                f,
                indent=2,
                default=str,
            )
        print(f"\nDetailed results written to: {output_path}")


# ---------------------------------------------------------------------------
# Main evaluation loop
# ---------------------------------------------------------------------------

async def main(args: argparse.Namespace) -> None:
    questions_path = Path(args.questions)
    if not questions_path.exists():
        print(f"ERROR: questions file not found: {questions_path}", file=sys.stderr)
        sys.exit(1)

    with questions_path.open() as f:
        dataset: list[dict[str, Any]] = json.load(f)

    if args.db_filter:
        dataset = [e for e in dataset if e.get("db_id") == args.db_filter]

    if args.limit:
        dataset = dataset[: args.limit]

    print(
        f"Evaluating {len(dataset)} questions"
        + (f" (db_filter={args.db_filter})" if args.db_filter else "")
        + (f" (limit={args.limit})" if args.limit else "")
    )

    engine = create_async_engine(args.db_url, echo=False)

    async with httpx.AsyncClient() as client:
        results: list[dict[str, Any]] = []
        for i, entry in enumerate(dataset, 1):
            res = await _evaluate_question(
                client,
                engine,
                args.api_url,
                args.api_key,
                entry,
                args.provider_id,
            )
            results.append(res)
            _print_progress(i, len(dataset), res)

    await engine.dispose()

    _print_summary(results, args.output)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="BIRD-SQL execution-match benchmark for Aegis Query Language"
    )
    parser.add_argument(
        "--questions",
        required=True,
        help="Path to BIRD dev.json questions file",
    )
    parser.add_argument(
        "--api-key",
        required=True,
        help="Aegis API key (query scope)",
    )
    parser.add_argument(
        "--api-url",
        default="http://localhost:8000",
        help="Aegis API base URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--db-url",
        default=(
            "postgresql+asyncpg://postgres:postgrespassword"
            "@127.0.0.1:5432/aegis_data_warehouse"
        ),
        help="PostgreSQL connection URL for gold SQL execution",
    )
    parser.add_argument(
        "--provider-id",
        default=None,
        help="LLM provider override (e.g. 'anthropic:claude-sonnet-4-6')",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Evaluate only the first N questions (for quick smoke tests)",
    )
    parser.add_argument(
        "--db-filter",
        default=None,
        help="Restrict evaluation to a single BIRD database (e.g. 'financial')",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional path to write detailed JSON results",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main(_parse_args()))
