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
        [--concurrency 5] \\
        [--limit 50] \\
        [--db-filter financial] \\
        [--output results.json] \\
        [--store benchmarks/results.db]
"""

import argparse
import asyncio
import json
import sqlite3
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
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
        "status": "exception",
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
# Run metadata + persistence
# ---------------------------------------------------------------------------

def _get_git_metadata(repo_root: Path) -> tuple[str | None, int | None]:
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return None, None

    try:
        status = subprocess.check_output(
            ["git", "status", "--porcelain"],
            cwd=repo_root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return commit, None

    dirty = 1 if status else 0
    return commit, dirty


def _utc_timestamp() -> tuple[str, str]:
    now = datetime.now(UTC)
    run_id_ts = now.strftime("%Y%m%d-%H%M%S")
    iso_ts = now.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return run_id_ts, iso_ts


def _redacted_args(args: argparse.Namespace) -> str:
    payload = dict(vars(args))
    if "api_key" in payload:
        payload["api_key"] = "<redacted>"
    return json.dumps(payload, sort_keys=True)


def _init_store(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS benchmark_runs(
            run_id TEXT PRIMARY KEY,
            commit_hash TEXT,
            dirty INTEGER,
            timestamp TEXT,
            provider_id TEXT,
            total INTEGER,
            matched INTEGER,
            errored INTEGER,
            accuracy_pct REAL,
            args_json TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS benchmark_results(
            run_id TEXT,
            question_id TEXT,
            db_id TEXT,
            question TEXT,
            gold_sql TEXT,
            generated_sql TEXT,
            source_database_used TEXT,
            status TEXT,
            match INTEGER,
            error TEXT,
            latency_ms REAL
        )
        """
    )


def _persist_results(
    store_path: Path,
    run_record: dict[str, Any],
    results: list[dict[str, Any]],
) -> None:
    store_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(store_path)
    try:
        _init_store(conn)
        conn.execute(
            """
            INSERT INTO benchmark_runs(
                run_id,
                commit_hash,
                dirty,
                timestamp,
                provider_id,
                total,
                matched,
                errored,
                accuracy_pct,
                args_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_record["run_id"],
                run_record["commit_hash"],
                run_record["dirty"],
                run_record["timestamp"],
                run_record["provider_id"],
                run_record["total"],
                run_record["matched"],
                run_record["errored"],
                run_record["accuracy_pct"],
                run_record["args_json"],
            ),
        )

        result_rows = [
            (
                run_record["run_id"],
                str(r.get("question_id", "")),
                r.get("db_id", ""),
                r.get("question", ""),
                r.get("gold_sql", ""),
                r.get("generated_sql"),
                r.get("source_database_used"),
                r.get("status", "exception"),
                1 if r.get("match") else 0,
                r.get("error"),
                r.get("latency_ms"),
            )
            for r in results
        ]
        conn.executemany(
            """
            INSERT INTO benchmark_results(
                run_id,
                question_id,
                db_id,
                question,
                gold_sql,
                generated_sql,
                source_database_used,
                status,
                match,
                error,
                latency_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            result_rows,
        )
        conn.commit()
    finally:
        conn.close()


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

    if args.concurrency < 1:
        print("ERROR: --concurrency must be >= 1", file=sys.stderr)
        sys.exit(1)

    print(
        f"Evaluating {len(dataset)} questions"
        + (f" (db_filter={args.db_filter})" if args.db_filter else "")
        + (f" (limit={args.limit})" if args.limit else "")
    )

    repo_root = Path(__file__).resolve().parents[1]
    commit_hash, dirty = _get_git_metadata(repo_root)
    run_id_ts, timestamp = _utc_timestamp()
    if commit_hash:
        run_id = f"{run_id_ts}-{commit_hash[:7]}"
    else:
        run_id = f"{run_id_ts}-nogit"

    engine = create_async_engine(args.db_url, echo=False)

    async with httpx.AsyncClient() as client:
        results: list[dict[str, Any]] = []
        semaphore = asyncio.Semaphore(args.concurrency)

        async def _bound_eval(entry: dict[str, Any]) -> dict[str, Any]:
            async with semaphore:
                return await _evaluate_question(
                    client,
                    engine,
                    args.api_url,
                    args.api_key,
                    entry,
                    args.provider_id,
                )

        tasks = [asyncio.create_task(_bound_eval(entry)) for entry in dataset]
        for i, task in enumerate(asyncio.as_completed(tasks), 1):
            res = await task
            results.append(res)
            _print_progress(i, len(dataset), res)

    await engine.dispose()

    total = len(results)
    matched = sum(1 for r in results if r["match"])
    errored = sum(1 for r in results if r["status"] != "success")
    accuracy = matched / total * 100 if total else 0.0

    store_path = Path(args.store)
    if store_path.as_posix() != "/dev/null":
        run_record = {
            "run_id": run_id,
            "commit_hash": commit_hash,
            "dirty": dirty,
            "timestamp": timestamp,
            "provider_id": args.provider_id,
            "total": total,
            "matched": matched,
            "errored": errored,
            "accuracy_pct": round(accuracy, 2),
            "args_json": _redacted_args(args),
        }
        try:
            _persist_results(store_path, run_record, results)
            print(f"\nStored run {run_id} in {store_path}")
        except Exception as exc:
            print(f"\nWARNING: Failed to persist results: {exc}", file=sys.stderr)

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
        "--concurrency",
        type=int,
        default=2,
        help=(
            "Max in-flight questions (default: 2; keep low for"
            " multi-database runs to avoid 429s)"
        ),
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
    parser.add_argument(
        "--store",
        default="benchmarks/results.db",
        help="SQLite DB path to store results (use /dev/null to disable)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main(_parse_args()))
